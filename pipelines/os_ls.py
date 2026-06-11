#!/usr/bin/env python
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
oci_pipe_ls.py — enumerate an OCI bucket in parallel, on the qpipe.work
harness.

All distribution mechanics — termination algebra, retries, the drain
cascade, flow control, the bus — live in qpipe.work and are documented
there. This file owns only the listing strategy: it never logs and never
touches a pipe directly. One task = one prefix:

  spec (work payload)
      {"namespace","bucket","region","prefix","depth","mode"}
      mode "split":  list one delimiter level, beget the child prefixes
      mode "stream": list the whole subtree flat — no delimiter, no
                     recursion (dispatched at/below --max-depth)
  discovery (beget)
      {"children": [prefix, ...]} — chunked during long scans so the
      coordinator fans out before the parent finishes; expand() turns
      each child into a spec at parent depth + 1
  result (data plane)
      one object record per object (see object_record) — compact JSON,
      nushell-friendly

Dedup: key_of is the prefix, so page-boundary repeats and the children of
re-delivered tasks collapse in the harness ledger.

At-least-once: a retried prefix re-emits its object records — dedupe
downstream on "name" if you need exactly-once.

Auth: resolving namespace/region is the coordinator's only SDK touch;
workers hold one client per region per thread. The collect and bus roles
never import the SDK at all.
"""

from __future__ import annotations

import sys
import argparse

from collections.abc import Iterable, Iterator
from dataclasses     import dataclass
from typing          import Any, Literal

from qpipe.work import (Coordinator, Discover, Discovery, Emit, Job,
                        Permanent, Pipeline, Pipes, Spec, Worker, run)

DELIMITER = "/"            # the only delimiter OCI Object Storage supports
PAGE_LIMIT = 1000          # ListObjects hard cap per call
CHILD_CHUNK = 5_000        # children per beget — far below the frame cap
DEFAULT_FIELDS = "name,size,etag,md5,timeCreated"

# 400/401/403/404 will not improve on retry; the SDK's DEFAULT_RETRY_STRATEGY
# has already chewed on 429 and 5xx by the time an error reaches us.
PERMANENT_HTTP_STATUSES = frozenset({400, 401, 403, 404})

Mode = Literal["split", "stream"]


# ---------------------------------------------------------------------------
# OCI plumbing

@dataclass(frozen=True, slots=True)
class OciAuth:
    """Where to find OCI credentials: config file path + profile name."""

    config_file: str
    profile: str

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "OciAuth":
        """Lift --oci-config / --profile out of parsed args."""
        return cls(config_file=args.oci_config, profile=args.profile)


def add_auth_args(p: argparse.ArgumentParser) -> None:
    """Register --oci-config / --profile (the CLI edge for OciAuth)."""
    p.add_argument("--oci-config", default="~/.oci/config", metavar="PATH",
                   help="OCI config file (default ~/.oci/config)")
    p.add_argument("--profile", default="DEFAULT",
                   help="profile within the config file (default DEFAULT)")


def make_client(auth: OciAuth,
                region: str | None = None) -> tuple[Any, dict[str, Any]]:
    """
    Build an ObjectStorageClient (SDK default retries) from a profile.

    Returns (client, loaded config dict).

    Side effects: reads the config file from disk, and mutates sys.modules —
    the deferred `import oci` below. The import buys SDK-freedom for every
    role that never calls this: collect and bus run fine on hosts with no
    oci installed.
    """
    import oci  # deferred on purpose — see docstring

    config = oci.config.from_file(file_location=auth.config_file,
                                  profile_name=auth.profile)
    if region:
        config["region"] = region

    client = oci.object_storage.ObjectStorageClient(
        config, retry_strategy=oci.retry.DEFAULT_RETRY_STRATEGY
    )
    return client, config


def resolve_namespace_region(auth: OciAuth, namespace: str | None,
                             region: str | None) -> tuple[str, str | None]:
    """
    Fill in namespace and region from the OCI profile, hitting OCI only
    when something is missing — with both supplied it builds no client at
    all.
    """
    if namespace and region:
        return namespace, region

    client, config = make_client(auth)
    return (namespace or client.get_namespace().data,
            region or config.get("region"))


def object_record(o: Any) -> dict[str, Any]:
    """
    ObjectSummary → plain dict containing only the fields the listing
    populated. Datetimes become ISO 8601 strings.
    """
    rec: dict[str, Any] = {"name": o.name}

    for attr in ("size", "etag", "md5", "storage_tier", "archival_state"):
        v = getattr(o, attr, None)
        if v is not None:
            rec[attr] = v

    for attr in ("time_created", "time_modified"):
        v = getattr(o, attr, None)
        if v is not None:
            rec[attr] = v.isoformat()

    return rec


class ClientPool:
    """
    One ObjectStorageClient per region, built on first use.

    Deliberately NOT thread-safe: the harness calls Worker.setup once per
    worker thread, so each thread owns its own pool — which sidesteps any
    OCI SDK thread-safety questions entirely.
    """

    def __init__(self, auth: OciAuth) -> None:
        """Remember the credentials; build nothing yet."""
        self._auth = auth
        self._clients: dict[str | None, Any] = {}

    def get(self, region: str | None) -> Any:
        """
        Return the client for `region`.

        Side effects: on first miss, builds the client (config-file read —
        see make_client) and caches it, buying one construction per region
        instead of one per task. Referentially transparent thereafter:
        same region, same client.
        """
        if region not in self._clients:
            self._clients[region] = make_client(self._auth, region=region)[0]
        return self._clients[region]


# ---------------------------------------------------------------------------
# listing logic

def mode_for_depth(depth: int, max_depth: int | None) -> Mode:
    """
    'split' (one delimiter level) until max_depth, then 'stream' (flat
    full-subtree scan, no further recursion).

    >>> mode_for_depth(0, None), mode_for_depth(2, 3), mode_for_depth(3, 3)
    ('split', 'split', 'stream')
    """
    return "stream" if (max_depth is not None and depth >= max_depth) \
        else "split"


def scan(client: Any, spec: Spec, result: Emit, discover: Discover,
         fields: str) -> None:
    """
    List one prefix's slice of the bucket.

    split:  one delimiter level — stream object records to `result`, chunk
            child prefixes to `discover` as we go.
    stream: the whole subtree, flat (no delimiter, no recursion).

    Side effects (the point of the function): emits while listing rather
    than returning collections. This buys bounded memory on million-object
    prefixes and early coordinator fan-out; the harness guarantees both
    emissions are ACKed before this task's `done` frame exists (see
    qpipe.work: result-before-done, beget-before-done).
    """
    split = spec.get("mode", "split") == "split"
    parent = spec.get("prefix", "")
    children: list[str] = []
    seen: set[str] = set()          # page-boundary duplicate guard
    start: str | None = None

    while True:
        page = client.list_objects(
            namespace_name=spec["namespace"],
            bucket_name=spec["bucket"],
            prefix=parent or None,
            delimiter=DELIMITER if split else None,
            fields=fields,
            limit=PAGE_LIMIT,
            start=start,
        ).data

        for o in page.objects:
            result(object_record(o))

        for c in page.prefixes or []:
            if c not in seen:
                seen.add(c)
                children.append(c)

        if len(children) >= CHILD_CHUNK:
            discover({"children": children})
            children = []

        start = page.next_start_with
        if not start:
            break

    if children:
        discover({"children": children})


# ---------------------------------------------------------------------------
# the strategies — argparse.Namespace dies inside make_*

def make_coordinator(args: argparse.Namespace) -> Coordinator:
    """
    Build the listing strategy: one seed (the --prefix subtree), children
    begotten by workers expand at depth + 1, prefixes dedup by identity.

    Namespace/region are resolved once, here, and ride in every spec, so
    workers need no out-of-band context.
    """
    auth = OciAuth.from_args(args)
    ns, region = resolve_namespace_region(auth, args.namespace, args.region)
    bucket = args.bucket
    max_depth = args.max_depth
    seed_prefix = args.prefix

    def spec(prefix: str, depth: int) -> Spec:
        """One prefix's work payload, mode chosen by depth."""
        return {"namespace": ns, "bucket": bucket, "region": region,
                "prefix": prefix, "depth": depth,
                "mode": mode_for_depth(depth, max_depth)}

    def seeds() -> Iterator[Spec]:
        """The single root task: the seed subtree at depth 0."""
        yield spec(seed_prefix, 0)

    def expand(parent: Spec, disc: Discovery) -> Iterable[Spec]:
        """Begotten children become tasks one level deeper than their
        parent."""
        return (spec(child, parent["depth"] + 1)
                for child in disc.get("children", []))

    return Coordinator(seeds=seeds, expand=expand,
                       key_of=lambda s: s["prefix"])


def make_worker(args: argparse.Namespace) -> Worker:
    """
    Build the listing worker: one prefix per task, object records to the
    results pipe, child prefixes begotten back to the coordinator.
    """
    auth = OciAuth.from_args(args)
    fields = args.fields

    def process(clients: ClientPool, job: Job, result: Emit,
                discover: Discover) -> None:
        """Scan one prefix; HTTP 4xx from OCI is permanent (no retry)."""
        client = clients.get(job.spec.get("region"))
        try:
            scan(client, job.spec, result, discover, fields)
        except Exception as e:
            status = getattr(e, "status", None)  # duck-typed ServiceError
            if status in PERMANENT_HTTP_STATUSES:
                raise Permanent(f"{type(e).__name__}: {e}") from e
            raise

    return Worker(setup=lambda: ClientPool(auth), process=process)


# ---------------------------------------------------------------------------
# CLI extras — the harness adds retry/timer/pipe flags itself

def add_coordinator_args(p: argparse.ArgumentParser) -> None:
    """ls-specific coordinator flags."""
    p.add_argument("bucket")
    p.add_argument("--prefix", default="",
                   help="seed subtree (''=whole bucket; end with '/' for a "
                        "directory)")
    p.add_argument("--namespace", help="skip the namespace lookup")
    p.add_argument("--region", help="override the profile's region")
    p.add_argument("--max-depth", type=int, default=None,
                   help="at/below this depth dispatch 'stream' tasks "
                        "(full-subtree scans) instead of splitting further")
    add_auth_args(p)


def add_worker_args(p: argparse.ArgumentParser) -> None:
    """ls-specific worker flags."""
    p.add_argument("--fields", default=DEFAULT_FIELDS,
                   help="ListObjects fields: name,size,etag,md5,timeCreated,"
                        "timeModified,storageTier,archivalState "
                        f"(default {DEFAULT_FIELDS})")
    add_auth_args(p)


PIPELINE = Pipeline(
    name="ls",
    describe="Enumerate an OCI bucket in parallel over qpipe.",
    default_pipes=Pipes(work="127.0.0.1:9101",
                        completions="127.0.0.1:9102",
                        results="127.0.0.1:9103",
                        wait=30.0),
    make_coordinator=make_coordinator,
    make_worker=make_worker,
    add_coordinator_args=add_coordinator_args,
    add_worker_args=add_worker_args,
)


if __name__ == "__main__":
    sys.exit(run(PIPELINE))
