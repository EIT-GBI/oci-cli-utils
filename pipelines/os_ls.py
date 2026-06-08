#!/usr/bin/env python
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
os_ls.py — enumerate an OCI bucket in parallel over qpipe.

Topology (acyclic by construction — workers never produce to the pipe they
consume; the coordinator is the sole producer of work):

            work (prefix tasks)                completions/children
 coordinator ──────────────────▶ worker pool ──────────────────────▶ coordinator
                                      │
                                      └──────▶ objects ──▶ collect / downstream

Frames (json codec)
  work        {"namespace","bucket","region","prefix","depth","mode"}
              mode "split": list one delimiter level, report child prefixes
              mode "stream": list the whole subtree flat, no recursion
  children    {"type":"children","prefix":parent,"children":[...]}
              chunked during long scans — keeps frames small and lets the
              coordinator fan out before the parent finishes
  completion  {"type":"completion","prefix","children":[last chunk],
               "objects":N,"worker","duration"}
  task_error  {"type":"task_error","prefix","worker","error"}
  objects     one compact-JSON record per object — any --jsonl consumer can
              tap this pipe directly

Termination (counter algebra, owned by TaskLedger)
  outstanding := tasks dispatched, not yet completed/failed. Seed = 1; every
  newly adopted child +1; every completion/failure -1. The coordinator is the
  only producer of work and the only consumer of completions, and a task's
  children frames precede its completion (FIFO pipe), so outstanding == 0 is
  a true global "done" no matter how different workers' frames interleave.

Shutdown (drain cascade — no poison pills)
  outstanding == 0 → request_drain on work, completions, objects. Workers EOF
  off the drained work pipe and close their producers; collectors EOF once
  objects runs dry. Drain is ack-on-receipt, so a watchdog escalates to
  request_shutdown if the coordinator's own EOF never arrives (--hammer).

Failure model: at-least-once
  Tasks that time out (--task-timeout) or report task_error are re-dispatched
  up to --max-attempts, then marked FAILED (exit 1, partial results). Retries
  can duplicate already-emitted object frames — dedupe downstream on "name"
  if you need exactly-once.

Code layout
  CLI edge      argparse lives only in build_parser / add_*_args / main and
                the *.from_args constructors; every role entry point takes
                explicit, frozen configuration values.
  TaskLedger    sans-I/O protocol core: completion frames in, frozen
                Decision values (Send | Retry | Failed) out — no pipe
                writes, no logging. Its lock-guarded internal mutation is
                the one documented exception (it is the class's entire job
                and never leaks). Time is injected, so retry/timeout/
                termination logic is unit testable without sleeping.
  apply()       the only interpreter of the Decision algebra; all
                control-plane I/O happens there.
  run_*         stateless wiring: pipes in, frames through, exit code out.
  bus           process supervision for the orchestrators — spawn,
                health-gate, babysit, tear down on crash or signal.

Effect convention (the house rule)
  A side effect is legitimate only if (a) it is the function's stated job —
  named I/O at the edge, e.g. run_*, push, log, *_pipes, finish,
  stop_orchestrators — or (b) the docstring carries a "Side effects:" line
  saying what the effect buys. Anything else is a bug.

Requires Python ≥ 3.10 (dataclass slots, structural pattern matching).
"""

from __future__ import annotations

import os
import sys
import enum
import time
import signal
import socket
import argparse
import threading
import subprocess

from collections.abc import Callable, Sequence
from dataclasses     import dataclass
from pathlib         import Path
from typing          import Any, Literal

import qpipe

DELIMITER = "/"            # the only delimiter OCI Object Storage supports
PAGE_LIMIT = 1000          # ListObjects hard cap per call
CHILD_CHUNK = 5_000        # children per frame — stays far below the frame cap
DEFAULT_FIELDS = "name,size,etag,timeCreated"

Mode = Literal["split", "stream"]


def log(msg: str) -> None:
    """Write one diagnostic line to stderr, unbuffered (frames go to pipes)."""
    print(msg, file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# configuration — frozen values built once at the CLI edge, passed explicitly

@dataclass(frozen=True, slots=True)
class Pipes:
    """Addresses of the qpipe orchestrators, plus the startup-wait budget."""

    work: str
    completions: str
    objects: str
    wait: float            # seconds to wait for orchestrator healthchecks

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Pipes":
        """Lift the pipe addresses out of parsed coordinator/worker args."""
        return cls(work=args.work, completions=args.completions,
                   objects=args.objects, wait=args.wait)


# Defaults for the CLI edge — a frozen value, not a mutable module dict.
DEFAULT_PIPES = Pipes(work="127.0.0.1:9101",
                      completions="127.0.0.1:9102",
                      objects="127.0.0.1:9103",
                      wait=30.0)


@dataclass(frozen=True, slots=True)
class OciAuth:
    """Where to find OCI credentials: config file path + profile name."""

    config_file: str
    profile: str

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "OciAuth":
        """Lift --oci-config / --profile out of parsed args."""
        return cls(config_file=args.oci_config, profile=args.profile)


@dataclass(frozen=True, slots=True)
class Target:
    """A fully resolved scan target — what every work frame carries."""

    namespace: str
    bucket: str
    region: str | None


@dataclass(frozen=True, slots=True)
class CoordinatorCfg:
    """Coordinator policy: seeding, splitting, retries, and timers."""

    seed_prefix: str       # '' = whole bucket
    max_depth: int | None  # at/below this depth, dispatch 'stream' tasks
    task_timeout: float    # seconds before a PENDING task is re-dispatched
    max_attempts: int      # sends per prefix before it is marked FAILED
    watchdog_tick: float   # seconds between timeout sweeps
    report_every: float    # seconds between progress lines
    hammer: float          # seconds after drain before escalating to shutdown

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "CoordinatorCfg":
        """Lift coordinator policy flags out of parsed args."""
        return cls(seed_prefix=args.prefix, max_depth=args.max_depth,
                   task_timeout=args.task_timeout,
                   max_attempts=args.max_attempts,
                   watchdog_tick=args.watchdog_tick,
                   report_every=args.report_every, hammer=args.hammer)


# ---------------------------------------------------------------------------
# OCI plumbing

def make_client(auth: OciAuth,
                region: str | None = None) -> tuple[Any, dict[str, Any]]:
    """
    Build an ObjectStorageClient (SDK default retries) from a profile.

    Returns (client, loaded config dict).

    Side effects: reads the config file from disk, and mutates sys.modules —
    the deferred `import oci` below. The import buys collect hosts freedom
    from the SDK: the collect role never touches this function, so it never
    needs oci installed.
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


def resolve_target(auth: OciAuth, bucket: str, namespace: str | None,
                   region: str | None) -> Target:
    """
    Fill in namespace/region, hitting OCI only when something is missing.

    This is the coordinator's only SDK touch — with both --namespace and
    --region supplied it builds no client at all.
    """
    if namespace and region:
        return Target(namespace=namespace, bucket=bucket, region=region)

    client, config = make_client(auth)
    ns = namespace or client.get_namespace().data
    return Target(namespace=ns, bucket=bucket,
                  region=region or config.get("region"))


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


# ---------------------------------------------------------------------------
# pipe plumbing

def wait_for_pipes(addrs: Sequence[str], timeout: float) -> None:
    """
    Block until every listed orchestrator passes healthcheck.

    Raises qpipe's timeout error if any address stays unhealthy past
    `timeout` seconds.
    """
    for addr in addrs:
        qpipe.wait_until_healthy(addr, timeout=timeout)


def shutdown_pipes(addrs: Sequence[str]) -> None:
    """
    Best-effort request_shutdown on every listed orchestrator.

    Used on Ctrl-C and in the hammer path — errors are swallowed because
    everything is going down anyway.
    """
    for addr in addrs:
        try:
            qpipe.request_shutdown(addr)
        except Exception:  # noqa: BLE001 — going down anyway
            pass


# ---------------------------------------------------------------------------
# coordinator state — the ledger owns every mutable thing, and nothing else

class TaskState(enum.Enum):
    """Lifecycle of a dispatched prefix. DONE and FAILED are terminal."""

    PENDING = enum.auto()
    DONE = enum.auto()
    FAILED = enum.auto()


@dataclass(slots=True)
class Task:
    """
    One prefix's dispatch record. Lives entirely inside TaskLedger; mutated
    only under its lock and never handed out.
    """

    prefix: str
    depth: int
    mode: Mode
    state: TaskState = TaskState.PENDING
    attempts: int = 0      # times this task has been sent to the work pipe
    deadline: float = 0.0  # monotonic clock; re-dispatch after this


@dataclass(frozen=True, slots=True)
class LedgerStats:
    """A consistent point-in-time snapshot of the ledger's counters."""

    outstanding: int
    prefixes: int
    objects: int
    failed: tuple[str, ...]


# Decisions — the ledger's entire output vocabulary. Frozen value snapshots
# taken under the lock: nothing the edge receives aliases ledger-owned
# mutable state. apply() in run_coordinator is their only interpreter.

@dataclass(frozen=True, slots=True)
class Send:
    """Decision: put this prefix's frame on the work pipe (first attempt)."""

    prefix: str
    depth: int
    mode: Mode


@dataclass(frozen=True, slots=True)
class Retry:
    """Decision: re-dispatch a task that timed out or errored."""

    prefix: str
    depth: int
    mode: Mode
    attempt: int           # ordinal of this send, for the log line
    why: str


@dataclass(frozen=True, slots=True)
class Failed:
    """Decision: a task exhausted max_attempts; report it, send nothing."""

    prefix: str
    attempts: int
    why: str


Decision = Send | Retry | Failed


def mode_for_depth(depth: int, max_depth: int | None) -> Mode:
    """
    'split' (one delimiter level) until max_depth, then 'stream' (flat
    full-subtree scan, no further recursion).
    """
    return "stream" if (max_depth is not None and depth >= max_depth) else "split"


class TaskLedger:
    """
    Bookkeeping for the at-least-once prefix protocol — and nothing else.

    Owns the counter algebra from the module docstring: outstanding := tasks
    dispatched but not yet DONE/FAILED; seed = +1, every adopted child +1,
    every completion or terminal failure -1; outstanding == 0 is global done.

    Sans-I/O: completion frames go in, Decision values come out, and that is
    the only externally visible behavior — no pipe writes, no logging. The
    lock-guarded mutation of tasks/counters is the documented exception: it
    is the entire job of this class, and it never leaks, because Decisions
    are frozen snapshots built under the lock.

    Thread-safety: every public method takes the internal lock; Task objects
    are never touched outside it and never escape.

    Time is injected: callers pass `now` (time.monotonic() at the call site).
    Tests are therefore equality assertions on returned Decision lists — no
    sleeping, no clock patching, no log capture.
    """

    def __init__(self, max_depth: int | None, task_timeout: float,
                 max_attempts: int) -> None:
        """Set the dispatch policy; counters start empty."""
        self._lock = threading.Lock()
        self._tasks: dict[str, Task] = {}    # every prefix ever dispatched
        self._outstanding = 0
        self._objects = 0
        self._failed: list[str] = []
        self._max_depth = max_depth
        self._task_timeout = task_timeout
        self._max_attempts = max_attempts

    # -- public protocol -----------------------------------------------------

    def seed(self, prefix: str, now: float) -> Send:
        """
        Register the root prefix at depth 0; returns the Send to apply.

        Call exactly once, before consuming any completion frames.
        """
        with self._lock:
            return self._register(prefix, depth=0, now=now)

    def handle(self, msg: dict[str, Any], now: float) -> list[Decision]:
        """
        Fold one completions-pipe frame into the ledger; return the
        Decisions the caller must apply.

        children    → Sends for newly adopted child prefixes
        completion  → adopt the last chunk, mark the parent DONE, count its
                      objects; returns the adopted Sends
        task_error  → one Retry, or one Failed (after max_attempts)

        Unknown frame types are ignored. Frames for tasks that are no longer
        PENDING are no-ops — at-least-once delivery means a retried task's
        first attempt may still report in.
        """
        kind = msg.get("type")
        prefix = msg.get("prefix", "")

        with self._lock:
            if kind == "children":
                return self._adopt(prefix, msg.get("children") or [], now)

            if kind == "completion":
                adopted = self._adopt(prefix, msg.get("children") or [], now)
                task = self._tasks.get(prefix)
                if task is not None and task.state is TaskState.PENDING:
                    task.state = TaskState.DONE
                    self._outstanding -= 1
                    self._objects += int(msg.get("objects", 0))
                return adopted

            if kind == "task_error":
                task = self._tasks.get(prefix)
                if task is not None and task.state is TaskState.PENDING:
                    return self._retry_or_fail(
                        task, str(msg.get("error", "")), now)
                return []

            return []

    def expired(self, now: float) -> list[Decision]:
        """
        Sweep for overdue PENDING tasks; return Retry/Failed decisions.

        Failed decisions decrement outstanding, so a sweep alone can
        complete the run.
        """
        decisions: list[Decision] = []
        with self._lock:
            for task in self._tasks.values():
                if task.state is TaskState.PENDING and now > task.deadline:
                    decisions.extend(self._retry_or_fail(task, "timeout", now))
        return decisions

    def done(self) -> bool:
        """True once outstanding == 0 — the global termination condition."""
        with self._lock:
            return self._outstanding == 0

    def stats(self) -> LedgerStats:
        """Consistent snapshot of the counters, for reporting and summary."""
        with self._lock:
            return LedgerStats(outstanding=self._outstanding,
                               prefixes=len(self._tasks),
                               objects=self._objects,
                               failed=tuple(self._failed))

    # -- internals (call only with self._lock held) ---------------------------

    def _register(self, prefix: str, depth: int, now: float) -> Send:
        """
        Create, count, and stamp a brand-new task; snapshot it as a Send.
        """
        task = Task(prefix=prefix, depth=depth,
                    mode=mode_for_depth(depth, self._max_depth))
        self._tasks[prefix] = task
        self._outstanding += 1
        self._stamp(task, now)
        return Send(prefix=task.prefix, depth=task.depth, mode=task.mode)

    def _adopt(self, parent: str, children: list[str],
               now: float) -> list[Decision]:
        """
        Register unseen children at parent.depth + 1.

        The membership check is the dedupe point: page-boundary repeats and
        children re-announced by a re-delivered task collapse here.
        """
        pt = self._tasks.get(parent)
        depth = pt.depth + 1 if pt is not None else 1

        adopted: list[Decision] = []
        for child in children:
            if child not in self._tasks:
                adopted.append(self._register(child, depth, now))
        return adopted

    def _retry_or_fail(self, task: Task, why: str,
                       now: float) -> list[Decision]:
        """
        Decide: another attempt (Retry) or terminal failure (Failed).
        """
        if task.attempts < self._max_attempts:
            self._stamp(task, now)
            return [Retry(prefix=task.prefix, depth=task.depth,
                          mode=task.mode, attempt=task.attempts, why=why)]

        task.state = TaskState.FAILED
        self._outstanding -= 1
        self._failed.append(task.prefix)
        return [Failed(prefix=task.prefix, attempts=task.attempts, why=why)]

    def _stamp(self, task: Task, now: float) -> None:
        """Account for one send: bump attempts, arm the re-dispatch deadline."""
        task.attempts += 1
        task.deadline = now + self._task_timeout


# ---------------------------------------------------------------------------
# coordinator — stateless wiring around the ledger

def work_frame(prefix: str, depth: int, mode: Mode,
               target: Target) -> dict[str, Any]:
    """
    Encode one dispatch as a work-pipe frame (wire format in the module
    docstring).
    """
    return {"namespace": target.namespace, "bucket": target.bucket,
            "region": target.region, "prefix": prefix,
            "depth": depth, "mode": mode}


def coordinator_watchdog(*, ledger: TaskLedger,
                         apply: Callable[[Decision], None],
                         finish: Callable[[], None], pipes: Pipes,
                         tick: float, hammer: float,
                         loop_done: threading.Event,
                         finishing: threading.Event) -> None:
    """
    Timer half of the coordinator. Two phases:

    1. Every `tick` seconds: apply the ledger's timeout decisions and, if a
       sweep drains outstanding to zero, fire the cascade — the completions
       loop is blocked in recv(), and draining its pipe (EOF) is what wakes
       it.
    2. Hammer: drain is ack-on-receipt, so if the loop's EOF still hasn't
       arrived `hammer` seconds after the cascade fired, the cascade has
       wedged — escalate to request_shutdown on work + completions. The
       objects pipe is left alone so a slow collector can finish draining
       legitimately.

    Yes, eight parameters — that is the watchdog's true dependency list,
    spelled out so it can never quietly reach for state it doesn't own.
    """
    while not loop_done.wait(tick):
        for decision in ledger.expired(time.monotonic()):
            try:
                apply(decision)
            except Exception as e:  # noqa: BLE001 — pipe dying mid-retry
                log(f"[coord] applying {decision!r} failed: {e}")
                return
        if ledger.done():
            finish()
            break

    if finishing.is_set() and not loop_done.wait(hammer):
        log(f"[coord] no EOF {hammer:.0f}s after drain — "
            f"escalating to shutdown")
        shutdown_pipes((pipes.work, pipes.completions))


def summarize(stats: LedgerStats, elapsed: float) -> int:
    """Final log lines + exit code: 1 if anything failed or never finished,
    else 0."""
    log(f"[coord] done: {stats.prefixes} prefixes, {stats.objects} objects, "
        f"{len(stats.failed)} failed, {elapsed:.1f}s")

    if stats.outstanding > 0:
        log(f"[coord] completions pipe closed with {stats.outstanding} "
            f"tasks outstanding — results are partial")
        return 1

    if stats.failed:
        for p in stats.failed[:10]:
            log(f"[coord]   FAILED {p!r}")
        if len(stats.failed) > 10:
            log(f"[coord]   ... +{len(stats.failed) - 10} more")
        return 1

    return 0


def run_coordinator(pipes: Pipes, auth: OciAuth, bucket: str,
                    namespace: str | None, region: str | None,
                    cfg: CoordinatorCfg) -> int:
    """
    Coordinator entry point: resolve the target, seed the ledger, apply
    its decisions until outstanding hits zero, fire the drain cascade,
    and summarize.

    Exit codes: 0 clean, 1 partial or failed prefixes, 130 interrupted.
    """
    target = resolve_target(auth, bucket, namespace=namespace, region=region)
    wait_for_pipes((pipes.work, pipes.completions, pipes.objects),
                   timeout=pipes.wait)

    log(f"[coord] bucket={target.bucket!r} namespace={target.namespace!r} "
        f"region={target.region!r} seed={cfg.seed_prefix!r}")

    ledger = TaskLedger(max_depth=cfg.max_depth,
                        task_timeout=cfg.task_timeout,
                        max_attempts=cfg.max_attempts)

    t0 = time.monotonic()
    last_report = t0
    loop_done = threading.Event()   # the completions loop has exited
    finishing = threading.Event()   # the drain cascade has been fired
    finish_lock = threading.Lock()  # makes the cascade fire exactly once
    send_lock = threading.Lock()    # loop + watchdog share one work producer

    with qpipe.Producer.connect(pipes.work, codec="json") as work, \
         qpipe.Consumer.connect(pipes.completions, codec="json") as done:

        def push(prefix: str, depth: int, mode: Mode) -> None:
            """The one effect on the work pipe; serialized across threads."""
            frame = work_frame(prefix, depth, mode, target)
            with send_lock:
                work.send(frame)

        def apply(decision: Decision) -> None:
            """Interpret one ledger Decision — the algebra's only consumer;
            all control-plane sending and logging happens here."""
            match decision:
                case Send(prefix=prefix, depth=depth, mode=mode):
                    push(prefix, depth, mode)
                case Retry(prefix=prefix, depth=depth, mode=mode,
                           attempt=attempt, why=why):
                    log(f"[coord] retry {attempt}/{cfg.max_attempts} "
                        f"{prefix!r}: {why}")
                    push(prefix, depth, mode)
                case Failed(prefix=prefix, attempts=attempts, why=why):
                    log(f"[coord] FAILED {prefix!r} after {attempts} "
                        f"attempts: {why}")

        def finish() -> None:
            """Fire the drain cascade exactly once, whichever thread is
            first."""
            with finish_lock:
                if finishing.is_set():
                    return
                finishing.set()
            for addr in (pipes.work, pipes.completions, pipes.objects):
                try:
                    qpipe.request_drain(addr)
                except Exception as e:  # noqa: BLE001 — drain is best-effort
                    log(f"[coord] drain({addr}) failed: {e}")

        def maybe_report() -> None:
            """Progress line, throttled.

            Side effects: stderr, plus updating its own throttle timestamp
            (`last_report`) — which buys rate limiting without threading a
            clock value through the recv loop.
            """
            nonlocal last_report
            now = time.monotonic()
            if now - last_report < cfg.report_every:
                return
            last_report = now
            s = ledger.stats()
            el = max(now - t0, 1e-9)
            log(f"[coord] outstanding={s.outstanding} prefixes={s.prefixes} "
                f"objects={s.objects} ({s.objects / el:.0f}/s, {el:.0f}s)")

        watchdog = threading.Thread(
            target=coordinator_watchdog,
            kwargs=dict(ledger=ledger, apply=apply, finish=finish,
                        pipes=pipes, tick=cfg.watchdog_tick,
                        hammer=cfg.hammer, loop_done=loop_done,
                        finishing=finishing),
            daemon=True,
        )

        try:
            apply(ledger.seed(cfg.seed_prefix, now=time.monotonic()))
            watchdog.start()

            for msg in done:        # EOFs only once drained or shut down
                for decision in ledger.handle(msg, now=time.monotonic()):
                    apply(decision)
                maybe_report()
                if ledger.done():
                    break

        except KeyboardInterrupt:
            log("[coord] interrupted — shutting all pipes down")
            loop_done.set()
            shutdown_pipes((pipes.work, pipes.completions, pipes.objects))
            return 130

        finally:
            loop_done.set()

    finish()                        # no-op if the watchdog beat us to it
    return summarize(ledger.stats(), elapsed=time.monotonic() - t0)


# ---------------------------------------------------------------------------
# worker — consume prefix tasks, emit objects + completions

class ClientPool:
    """
    One ObjectStorageClient per region, built on first use.

    Deliberately NOT thread-safe: each worker thread owns its own pool,
    which sidesteps any OCI SDK thread-safety questions entirely.
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


def scan(client: Any, task: dict[str, Any], out: qpipe.Producer,
         done: qpipe.Producer, fields: str) -> tuple[list[str], int]:
    """
    List one task's slice of the bucket; returns (unsent children, object
    count).

    split:  one delimiter level — stream objects out, chunk child prefixes
            back through the completions pipe as we go.
    stream: the whole subtree, flat (no delimiter, no recursion).

    Side effects (the point of the function): sends object records to `out`
    and children chunks to `done` *while* listing. This buys bounded memory
    on million-object prefixes, early coordinator fan-out, and the ordering
    guarantee below; a pure variant would allocate one event per object on
    the hottest path in the program for no behavioral gain.

    Object sends are ACKed by the orchestrator before this returns, so by the
    time our completion frame exists, every object frame is already enqueued
    — that's what makes the post-zero drain race-free.
    """
    split = task.get("mode", "split") == "split"
    parent = task.get("prefix", "")
    children: list[str] = []
    seen: set[str] = set()          # page-boundary duplicate guard
    count = 0
    start: str | None = None

    while True:
        page = client.list_objects(
            namespace_name=task["namespace"],
            bucket_name=task["bucket"],
            prefix=parent or None,
            delimiter=DELIMITER if split else None,
            fields=fields,
            limit=PAGE_LIMIT,
            start=start,
        ).data

        for o in page.objects:
            out.send(object_record(o))
            count += 1

        for c in page.prefixes or []:
            if c not in seen:
                seen.add(c)
                children.append(c)

        if len(children) >= CHILD_CHUNK:
            done.send({"type": "children", "prefix": parent,
                       "children": children})
            children = []

        start = page.next_start_with
        if not start:
            return children, count


def worker_loop(pipes: Pipes, auth: OciAuth, fields: str, wid: str) -> None:
    """
    One consume → scan → report loop.

    EOFs off the drained work pipe. Per-task OCI errors become task_error
    frames and the loop keeps serving; QpipeError propagates because the
    pipes themselves are going away.
    """
    clients = ClientPool(auth)
    n_tasks = n_objects = 0

    with qpipe.Consumer.connect(pipes.work, codec="json") as tasks, \
         qpipe.Producer.connect(pipes.objects, codec="json") as out, \
         qpipe.Producer.connect(pipes.completions, codec="json") as done:

        for task in tasks:          # EOFs when the work pipe drains
            t_start = time.monotonic()
            prefix = task.get("prefix", "")
            try:
                children, n = scan(clients.get(task.get("region")),
                                   task, out, done, fields)
                done.send({"type": "completion", "prefix": prefix,
                           "children": children, "objects": n,
                           "worker": wid,
                           "duration": round(time.monotonic() - t_start, 3)})
                n_tasks += 1
                n_objects += n
            except qpipe.QpipeError:
                raise               # pipes are going away — stop
            except Exception as e:  # noqa: BLE001 — report, keep serving
                done.send({"type": "task_error", "prefix": prefix,
                           "worker": wid,
                           "error": f"{type(e).__name__}: {e}"})

    log(f"[worker {wid}] {n_tasks} tasks, {n_objects} objects")


def run_worker(pipes: Pipes, auth: OciAuth, fields: str, threads: int) -> int:
    """
    Worker entry point: spin `threads` independent loops and wait them out.

    Threads rather than processes: the qpipe bindings drop the GIL around
    all blocking pipe I/O and the OCI SDK is HTTP-bound, so threads
    parallelize this fine. Exit codes: 0 clean, 130 interrupted.
    """
    wait_for_pipes((pipes.work, pipes.completions, pipes.objects),
                   timeout=pipes.wait)
    base = f"{socket.gethostname()}:{os.getpid()}"

    def boot(i: int) -> None:
        """Run one loop; downgrade expected shutdown races to a log line."""
        wid = f"{base}.{i}"
        try:
            worker_loop(pipes, auth, fields, wid)
        except qpipe.QpipeError as e:   # shutdown race — expected
            log(f"[worker {wid}] pipe closed: {e}")
        except Exception as e:          # noqa: BLE001
            log(f"[worker {wid}] fatal: {type(e).__name__}: {e}")

    pool = [threading.Thread(target=boot, args=(i,), daemon=True)
            for i in range(threads)]
    for t in pool:
        t.start()

    try:
        for t in pool:
            t.join()
    except KeyboardInterrupt:
        return 130
    return 0


# ---------------------------------------------------------------------------
# collect — drain the objects pipe to JSONL

def run_collect(objects_addr: str, wait: float, output: str | None) -> int:
    """
    Collect entry point: stream the objects pipe to JSONL on stdout or
    `output`.

    Side effects: opens `output` with "wb" — an existing file is truncated.

    Frames are already compact, newline-free JSON (the json codec guarantees
    it), so the raw codec passes the bytes straight through — no decode /
    re-encode round trip.
    """
    wait_for_pipes((objects_addr,), timeout=wait)
    out = sys.stdout.buffer if output in (None, "-") else open(output, "wb")
    n = 0
    t0 = time.monotonic()

    try:
        with qpipe.Consumer.connect(objects_addr, codec="raw") as objs:
            for frame in objs:      # EOFs via the drain cascade
                out.write(frame)
                out.write(b"\n")
                n += 1
                if n % 100_000 == 0:
                    el = time.monotonic() - t0
                    log(f"[collect] {n} objects ({n / el:.0f}/s)")
    finally:
        if out is not sys.stdout.buffer:
            out.close()

    log(f"[collect] {n} objects")
    return 0


# ---------------------------------------------------------------------------
# bus — spawn and supervise the pipe orchestrators

def stop_orchestrators(procs: dict[str, subprocess.Popen[bytes]],
                       grace: float = 10.0) -> None:
    """
    SIGTERM every still-running orchestrator, then SIGKILL the stragglers.

    Idempotent and best-effort — already-exited children are skipped — so it
    is safe to call from both the supervisor and the teardown path.
    """
    for p in procs.values():
        if p.poll() is None:
            p.terminate()

    deadline = time.monotonic() + grace
    for name, p in procs.items():
        try:
            p.wait(timeout=max(deadline - time.monotonic(), 0.1))
        except subprocess.TimeoutExpired:
            log(f"[bus] {name} ignored SIGTERM after {grace:g}s — killing")
            p.kill()
            p.wait()


def bus_healthy(procs: dict[str, subprocess.Popen[bytes]],
                addrs: dict[str, str], timeout: float) -> bool:
    """
    Gate on every orchestrator's healthcheck within a shared `timeout`
    budget.

    Interleaves the health probe with a liveness check, so a child that
    dies immediately (port already bound, bad flags) fails the gate in
    about a second instead of burning the whole budget. Returns False on
    death or timeout; the caller owns the teardown.
    """
    deadline = time.monotonic() + timeout
    for name, addr in addrs.items():
        while True:
            rc = procs[name].poll()
            if rc is not None:
                log(f"[bus] {name} died before its healthcheck (rc={rc}) — "
                    f"see its log")
                return False

            budget = deadline - time.monotonic()
            if budget <= 0:
                log(f"[bus] {name} not healthy after {timeout:g}s")
                return False

            try:
                qpipe.wait_until_healthy(addr, timeout=min(1.0, budget))
                break
            except qpipe.QpipeError:    # not up yet — re-check liveness
                continue
    return True


def supervise_bus(procs: dict[str, subprocess.Popen[bytes]]) -> int:
    """
    Wait for every orchestrator to exit; the first NONZERO exit tears the
    survivors down.

    Clean (rc == 0) exits are allowed to stagger — the drain cascade shuts
    the pipes down at different times, and objects must be free to outlive
    work/completions for slow collectors (see the module docstring).
    Polling rather than os.wait(): three children, half-second resolution,
    portable, readable.
    """
    rcs: dict[str, int] = {}
    while len(rcs) < len(procs):
        time.sleep(0.5)
        for name, p in procs.items():
            if name in rcs:
                continue
            rc = p.poll()
            if rc is None:
                continue
            rcs[name] = rc
            log(f"[bus] {name} exited rc={rc}")
            if rc != 0:
                log("[bus] nonzero exit — half a bus is worse than none, "
                    "stopping the rest")
                stop_orchestrators(procs)
                return 1
    return 0


def run_bus(pipes: Pipes, logs: Path, rust_log: str, orchestrator: str) -> int:
    """
    Bus entry point: spawn one qpipe orchestrator per pipe, gate on their
    healthchecks (--wait budget), then supervise until they exit.

    Children log to <logs>/<name>.log, append mode, stderr merged into
    stdout. Assumes orchestrator processes exit once their pipe is drained
    or shut down; if they are run-forever servers, the bus ends only via
    signal.

    Side effects beyond the stated job: installs a SIGTERM handler that
    converts the signal into SystemExit — buying a teardown path under
    systemd/Slurm/k8s stop signals, which otherwise reach only this
    supervisor and orphan the children on their ports.

    Exit codes: 0 all clean, 1 spawn/health/crash failure, 130 interrupted;
    SIGTERM propagates as 143 after teardown.
    """
    logs.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["RUST_LOG"] = rust_log

    def on_sigterm(signum: int, frame: Any) -> None:
        """Turn SIGTERM into an exception so `finally` runs the teardown."""
        log("[bus] SIGTERM — shutting the bus down")
        sys.exit(143)

    signal.signal(signal.SIGTERM, on_sigterm)

    addrs = {"work": pipes.work,
             "completions": pipes.completions,
             "objects": pipes.objects}
    procs: dict[str, subprocess.Popen[bytes]] = {}
    logfiles: list[Any] = []

    try:
        for name, addr in addrs.items():
            f = open(logs / f"{name}.log", "ab")
            logfiles.append(f)
            # No text=/bufsize=: stdout is a real file, so the child writes
            # straight to the fd and buffers however its runtime pleases.
            procs[name] = subprocess.Popen([orchestrator, addr], env=env,
                                           stdout=f, stderr=subprocess.STDOUT)
            log(f"[bus] {name} pid={procs[name].pid} on {addr} → {f.name}")

        if not bus_healthy(procs, addrs, timeout=pipes.wait):
            return 1

        log("[bus] all orchestrators healthy — supervising")
        return supervise_bus(procs)

    except FileNotFoundError as e:
        log(f"[bus] cannot spawn {orchestrator!r}: {e}")
        return 1
    except KeyboardInterrupt:
        log("[bus] interrupted — stopping orchestrators")
        return 130
    finally:
        stop_orchestrators(procs)
        for f in logfiles:
            f.close()


# ---------------------------------------------------------------------------
# CLI edge — the only place argparse.Namespace is allowed to exist

def add_pipe_args(p: argparse.ArgumentParser, *names: str) -> None:
    """
    Register --<name> HOST:PORT overrides (defaults from DEFAULT_PIPES) plus
    the shared --wait startup budget.
    """
    for n in names:
        default = getattr(DEFAULT_PIPES, n)
        p.add_argument(f"--{n}", default=default, metavar="HOST:PORT",
                       help=f"{n} pipe orchestrator (default {default})")
    p.add_argument("--wait", type=float, default=DEFAULT_PIPES.wait,
                   help=f"seconds to wait for pipes to come up "
                            f"(default {DEFAULT_PIPES.wait:g})")


def add_auth_args(p: argparse.ArgumentParser) -> None:
    """Register --oci-config / --profile (the CLI edge for OciAuth)."""
    p.add_argument("--oci-config", default="~/.oci/config", metavar="PATH",
                   help="OCI config file (default ~/.oci/config)")
    p.add_argument("--profile", default="DEFAULT",
                   help="profile within the config file (default DEFAULT)")


def build_parser() -> argparse.ArgumentParser:
    """Define the CLI — one subparser per role.

    Flag names are stable interface; the from_args constructors are their
    only readers.
    """
    ap = argparse.ArgumentParser(
        description="Enumerate an OCI bucket in parallel over qpipe."
    )
    sub = ap.add_subparsers(dest="role", required=True)

    c = sub.add_parser("coordinator", help="seed, track, terminate")
    c.add_argument("bucket")
    c.add_argument("--prefix", default="",
                   help="seed subtree (''=whole bucket; end with '/' for a "
                        "directory)")
    c.add_argument("--namespace", help="skip the namespace lookup")
    c.add_argument("--region", help="override the profile's region for workers")
    c.add_argument("--max-depth", type=int, default=None,
                   help="below this depth dispatch 'stream' tasks "
                        "(full-subtree scans) instead of splitting further")
    c.add_argument("--task-timeout", type=float, default=300.0,
                   help="seconds before a task is re-dispatched (default 300)")
    c.add_argument("--max-attempts", type=int, default=3)
    c.add_argument("--watchdog-tick", type=float, default=5.0)
    c.add_argument("--report-every", type=float, default=5.0)
    c.add_argument("--hammer", type=float, default=60.0,
                   help="seconds after drain before escalating to shutdown")
    add_pipe_args(c, "work", "completions", "objects")
    add_auth_args(c)

    w = sub.add_parser("worker",
                       help="consume prefixes, emit objects+completions")
    w.add_argument("--fields", default=DEFAULT_FIELDS,
                   help="ListObjects fields: name,size,etag,md5,timeCreated,"
                        "timeModified,storageTier,archivalState "
                        f"(default {DEFAULT_FIELDS})")
    w.add_argument("--threads", type=int, default=4,
                   help="independent worker loops in this process (default 4)")
    add_pipe_args(w, "work", "completions", "objects")
    add_auth_args(w)

    g = sub.add_parser("collect", help="drain the objects pipe to JSONL")
    g.add_argument("--output", "-o",
                   help="file (default stdout, '-' works too)")
    add_pipe_args(g, "objects")

    b = sub.add_parser("bus", help="spawn + supervise the pipe orchestrators "
                                   "(the message-exchange fabric)")
    b.add_argument("--logdir", type=Path, default=Path.cwd() / "os-ls-logs",
                   metavar="DIR",
                   help="directory for per-orchestrator logs "
                        "(default ./bus_logs)")
    b.add_argument("--rust-log", default="debug",
                   help="RUST_LOG for the spawned orchestrators "
                        "(default debug)")
    b.add_argument("--orchestrator", default="orchestrator", metavar="BIN",
                   help="orchestrator binary to spawn "
                        "(default: 'orchestrator' from PATH)")
    add_pipe_args(b, "work", "completions", "objects")

    return ap


def main(argv: list[str] | None = None) -> int:
    """
    Parse the CLI and hand explicit configuration to one role's entry point.
    """
    args = build_parser().parse_args(argv)

    if args.role == "coordinator":
        return run_coordinator(pipes=Pipes.from_args(args),
                               auth=OciAuth.from_args(args),
                               bucket=args.bucket,
                               namespace=args.namespace,
                               region=args.region,
                               cfg=CoordinatorCfg.from_args(args))

    if args.role == "worker":
        return run_worker(pipes=Pipes.from_args(args),
                          auth=OciAuth.from_args(args),
                          fields=args.fields,
                          threads=args.threads)

    if args.role == "collect":
        return run_collect(objects_addr=args.objects, wait=args.wait,
                           output=args.output)

    if args.role == "bus":
        return run_bus(pipes=Pipes.from_args(args), logs=args.logdir,
                       rust_log=args.rust_log, orchestrator=args.orchestrator)

    raise AssertionError(f"unhandled role {args.role!r}")  # unreachable


if __name__ == "__main__":
    sys.exit(main())
