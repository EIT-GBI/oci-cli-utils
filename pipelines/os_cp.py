#!/usr/bin/env python
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
os_cp.py — server-side copy of OCI objects, in parallel over qpipe.

Topology (acyclic; the coordinator is the sole producer of work):

  stdin (JSONL) ─▶ coordinator ──work──▶ worker pool ──completions──▶ coordinator
                                              │
                                              └──results──▶ collect / downstream

Input (JSONL on the coordinator's stdin, or --input FILE)
  required  "name", "src_namespace", "src_bucket",
            "dst_namespace", "dst_bucket"
  optional  "dst_name"    destination object name (default: name)
            "src_region"  region of the source / the client (default: the
                          worker profile's region)
            "dst_region"  copy destination region (default: the effective
                          source region; the API requires one)
  Keys are flat on purpose — nushell filters see them as columns; dedupe
  upstream (`uniq`) if your input may repeat. The task id IS the input line
  number; rejected (unparseable / incomplete) lines are logged with that
  number, never dispatched, and force exit 1.

Frames (json codec)
  work        the CopySpec fields + {"task":N,"attempt":n} — the wire schema
              is single-sourced from the dataclass via asdict
  completion  {"type":"completion","task":N,"attempt":n,"worker","duration"}
  task_error  {"type":"task_error","task":N,"worker","error","permanent"}
  results     one record per ACCEPTED copy request: the input coordinates,
              resolved regions, "work_request", "worker", "duration" — feed
              this to a later checker that polls the work requests

Termination (the oci_pipe_ls counter algebra, with stdin as the root task)
  outstanding starts at 1: the feed itself is the root task. Every ingested
  line +1; every completion or terminal failure -1; EOF on the feed
  "completes" the root (seal, -1). outstanding == 0 therefore implies the
  feed is sealed AND every task is terminal, however frames interleave —
  and nothing can hit zero before seal, so thread start order is a don't-care.

Flow control (--in-flight)
  The listing pipeline is demand-paced: new tasks are born from worker
  completions. This one is supply-paced: stdin can outrun the workers by
  orders of magnitude, and a task's retry deadline is armed at DISPATCH —
  unbounded feeding turns a deep queue into a retry storm (everything queued
  longer than --task-timeout gets re-sent). The feeder blocks once
  --in-flight tasks are pending; size it so in_flight / aggregate worker
  throughput stays well below --task-timeout.

Shutdown — the oci_pipe_ls drain cascade and watchdog hammer, unchanged.
  The results pipe is spared by the hammer so a slow collector can finish.

Failure model: at-least-once SUBMISSION
  copy_object is asynchronous: OCI accepts the request (202), returns an
  opc-work-request-id response header, and copies in the background. "Done"
  here means every copy request was ACCEPTED — poll the work requests in the
  results JSONL to confirm bytes landed. Retries (timeout / retryable error)
  can submit duplicate copy requests for one task: group results on "task"
  downstream. HTTP 400/401/403/404 are permanent — they fail immediately,
  no retry; the SDK's own retry strategy absorbs 429/5xx before we see them.

Code layout
  CLI edge      argparse lives only in build_parser / add_*_args / main and
                the *.from_args constructors; every role entry point takes
                explicit, frozen configuration values.
  CopyLedger    sans-I/O protocol core: completion frames in, frozen
                Decision values (Send | Retry | Failed) out — no pipe
                writes, no logging. Its lock-guarded internal mutation is
                the one documented exception. Time is injected, so retry/
                timeout/termination logic is unit testable without sleeping.
  apply()       the only interpreter of the Decision algebra; all
                control-plane I/O happens there.
  feed()        the supply side: parse, validate, backpressure, seal.
  run_*         stateless wiring: pipes in, frames through, exit code out.
  bus           process supervision for the orchestrators — spawn,
                health-gate, babysit, tear down on crash or signal.

Effect convention (the house rule)
  A side effect is legitimate only if (a) it is the function's stated job —
  named I/O at the edge, e.g. run_*, push, log, *_pipes, finish,
  stop_orchestrators, request_copy — or (b) the docstring carries a
  "Side effects:" line saying what the effect buys. Anything else is a bug.

Requires Python ≥ 3.10 (dataclass slots, structural pattern matching).
"""

from __future__ import annotations

import os
import sys
import enum
import json
import time
import signal
import socket
import argparse
import threading
import subprocess

from collections.abc import Callable, Sequence
from dataclasses     import asdict, dataclass
from pathlib         import Path
from typing          import Any

import qpipe

# 400/401/403/404 will not improve on retry; the SDK's DEFAULT_RETRY_STRATEGY
# has already chewed on 429 and 5xx by the time an error reaches us.
PERMANENT_HTTP_STATUSES = frozenset({400, 401, 403, 404})


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
    results: str
    wait: float            # seconds to wait for orchestrator healthchecks

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Pipes":
        """Lift the pipe addresses out of parsed coordinator/worker args."""
        return cls(work=args.work, completions=args.completions,
                   results=args.results, wait=args.wait)


# Defaults for the CLI edge — deliberately disjoint from oci_pipe_ls
# (9101–9103) so the listing and copying pipelines can run side by side.
DEFAULT_PIPES = Pipes(work="127.0.0.1:9111",
                      completions="127.0.0.1:9112",
                      results="127.0.0.1:9113",
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
class CopySpec:
    """
    One validated copy order: which object, from where, to where.

    The work-pipe frame is asdict(spec) + task/attempt, so this class IS
    the wire schema for the data fields — change it here, nowhere else.
    """

    name: str
    src_namespace: str
    src_bucket: str
    dst_namespace: str
    dst_bucket: str
    dst_name: str
    src_region: str | None
    dst_region: str | None

    @classmethod
    def from_record(cls, rec: Any) -> "CopySpec":
        """
        Validate one parsed input record (pure).

        Raises ValueError naming every missing/blank required key; the line
        number is the caller's context to attach.
        """
        if not isinstance(rec, dict):
            raise ValueError("not a JSON object")

        required = ("name", "src_namespace", "src_bucket",
                    "dst_namespace", "dst_bucket")
        missing = [k for k in required if not rec.get(k)]
        if missing:
            raise ValueError(f"missing/empty: {', '.join(missing)}")

        return cls(name=rec["name"],
                   src_namespace=rec["src_namespace"],
                   src_bucket=rec["src_bucket"],
                   dst_namespace=rec["dst_namespace"],
                   dst_bucket=rec["dst_bucket"],
                   dst_name=rec.get("dst_name") or rec["name"],
                   src_region=rec.get("src_region"),
                   dst_region=rec.get("dst_region"))


@dataclass(frozen=True, slots=True)
class CopyCfg:
    """Coordinator policy: retries, flow control, and timers."""

    task_timeout: float    # seconds before a PENDING task is re-dispatched
    max_attempts: int      # sends per task before it is marked FAILED
    in_flight: int         # feeder backpressure: max PENDING tasks
    watchdog_tick: float   # seconds between timeout sweeps
    report_every: float    # seconds between progress lines
    hammer: float          # seconds after drain before escalating to shutdown

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "CopyCfg":
        """Lift coordinator policy flags out of parsed args."""
        return cls(task_timeout=args.task_timeout,
                   max_attempts=args.max_attempts,
                   in_flight=args.in_flight,
                   watchdog_tick=args.watchdog_tick,
                   report_every=args.report_every,
                   hammer=args.hammer)


# ---------------------------------------------------------------------------
# OCI plumbing

def make_client(auth: OciAuth,
                region: str | None = None) -> tuple[Any, dict[str, Any]]:
    """
    Build an ObjectStorageClient (SDK default retries) from a profile.

    Returns (client, loaded config dict).

    Side effects: reads the config file from disk, and mutates sys.modules —
    the deferred `import oci` below. The import buys collect hosts and the
    coordinator freedom from the SDK: neither role touches this function,
    so neither needs oci installed.
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
    """Lifecycle of a dispatched copy task. DONE and FAILED are terminal."""

    PENDING = enum.auto()
    DONE = enum.auto()
    FAILED = enum.auto()


@dataclass(slots=True)
class Task:
    """
    One copy order's dispatch record. Lives entirely inside CopyLedger;
    mutated only under its lock and never handed out.
    """

    task_id: int           # the input line number
    spec: CopySpec
    state: TaskState = TaskState.PENDING
    attempts: int = 0      # times this task has been sent to the work pipe
    deadline: float = 0.0  # monotonic clock; re-dispatch after this


@dataclass(frozen=True, slots=True)
class CopyStats:
    """A consistent point-in-time snapshot of the ledger's counters."""

    outstanding: int
    ingested: int
    accepted: int
    failed: tuple[tuple[int, str], ...]   # (task id, object name)
    sealed: bool


# Decisions — the ledger's entire output vocabulary. Frozen value snapshots
# (CopySpec is itself frozen) taken under the lock: nothing the edge
# receives aliases ledger-owned mutable state. apply() in run_coordinator
# is their only interpreter.

@dataclass(frozen=True, slots=True)
class Send:
    """Decision: dispatch this task's first attempt to the work pipe."""

    task: int
    attempt: int
    spec: CopySpec


@dataclass(frozen=True, slots=True)
class Retry:
    """Decision: re-dispatch a task that timed out or errored retryably."""

    task: int
    attempt: int           # ordinal of this send, for the log line
    spec: CopySpec
    why: str


@dataclass(frozen=True, slots=True)
class Failed:
    """Decision: a task is terminally failed; report it, send nothing."""

    task: int
    name: str
    attempts: int
    why: str


Decision = Send | Retry | Failed


class CopyLedger:
    """
    Bookkeeping for the at-least-once submission protocol — and nothing else.

    Owns the counter algebra from the module docstring, with the feed as the
    root task: outstanding starts at 1, ingest() +1, every completion or
    terminal failure -1, seal() -1 exactly once at feed EOF. outstanding ==
    0 is therefore global done — provably after seal.

    Sans-I/O: completion frames go in, Decision values come out, and that is
    the only externally visible behavior — no pipe writes, no logging. The
    lock-guarded mutation of tasks/counters is the documented exception: it
    is the entire job of this class, and it never leaks, because Decisions
    are frozen snapshots built under the lock.

    A task_error frame carrying "permanent": true skips the retry budget
    and fails immediately (HTTP 4xx will not improve on resend).

    Thread-safety: every public method takes the internal lock; Task objects
    are never touched outside it and never escape. Time is injected
    (`now` = time.monotonic() at the call site), so tests are equality
    assertions on returned Decision lists — no sleeping, no clock patching.
    """

    def __init__(self, task_timeout: float, max_attempts: int) -> None:
        """Set the dispatch policy; outstanding starts at 1 — the feed."""
        self._lock = threading.Lock()
        self._tasks: dict[int, Task] = {}    # every task ever ingested
        self._outstanding = 1                # the feed is the root task
        self._sealed = False
        self._accepted = 0
        self._failed: list[tuple[int, str]] = []
        self._task_timeout = task_timeout
        self._max_attempts = max_attempts

    # -- public protocol -----------------------------------------------------

    def ingest(self, task_id: int, spec: CopySpec, now: float) -> Send:
        """
        Register one copy order; returns the Send to apply.

        Task ids are the caller's (input line numbers) and must be unique —
        a duplicate raises ValueError rather than silently overwriting.
        """
        with self._lock:
            if task_id in self._tasks:
                raise ValueError(f"duplicate task id {task_id}")
            task = Task(task_id=task_id, spec=spec)
            self._tasks[task_id] = task
            self._outstanding += 1
            self._stamp(task, now)
            return Send(task=task_id, attempt=task.attempts, spec=spec)

    def seal(self) -> None:
        """
        Mark the feed exhausted: the root task completes (-1). Idempotent.

        After seal, outstanding == 0 becomes reachable; before it, done()
        can never fire, which is what makes thread start order irrelevant.
        """
        with self._lock:
            if not self._sealed:
                self._sealed = True
                self._outstanding -= 1

    def handle(self, msg: dict[str, Any], now: float) -> list[Decision]:
        """
        Fold one completions-pipe frame into the ledger; return the
        Decisions the caller must apply.

        completion  → mark the task DONE, count the accepted copy request
        task_error  → one Retry, or one Failed (permanent error, or the
                      retry budget is spent)

        Unknown frame types are ignored. Frames for tasks that are no longer
        PENDING are no-ops — at-least-once delivery means a retried task's
        first attempt may still report in.
        """
        kind = msg.get("type")
        task = self._tasks.get(msg.get("task"))  # None for unknown ids

        with self._lock:
            if task is None or task.state is not TaskState.PENDING:
                return []

            if kind == "completion":
                task.state = TaskState.DONE
                self._outstanding -= 1
                self._accepted += 1
                return []

            if kind == "task_error":
                return self._retry_or_fail(
                    task, str(msg.get("error", "")), now,
                    permanent=bool(msg.get("permanent")))

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

    def pending(self) -> int:
        """
        PENDING task count, the feed's own +1 excluded — this is the number
        the feeder's --in-flight backpressure gates on.
        """
        with self._lock:
            return self._outstanding - (0 if self._sealed else 1)

    def done(self) -> bool:
        """True once outstanding == 0 — sealed feed AND all tasks terminal."""
        with self._lock:
            return self._outstanding == 0

    def stats(self) -> CopyStats:
        """Consistent snapshot of the counters, for reporting and summary."""
        with self._lock:
            return CopyStats(outstanding=self._outstanding,
                             ingested=len(self._tasks),
                             accepted=self._accepted,
                             failed=tuple(self._failed),
                             sealed=self._sealed)

    # -- internals (call only with self._lock held) ---------------------------

    def _retry_or_fail(self, task: Task, why: str, now: float,
                       *, permanent: bool = False) -> list[Decision]:
        """
        Decide: another attempt (Retry) or terminal failure (Failed).
        Permanent errors skip the remaining retry budget.
        """
        if not permanent and task.attempts < self._max_attempts:
            self._stamp(task, now)
            return [Retry(task=task.task_id, attempt=task.attempts,
                          spec=task.spec, why=why)]

        task.state = TaskState.FAILED
        self._outstanding -= 1
        self._failed.append((task.task_id, task.spec.name))
        return [Failed(task=task.task_id, name=task.spec.name,
                       attempts=task.attempts,
                       why=f"{why} (permanent)" if permanent else why)]

    def _stamp(self, task: Task, now: float) -> None:
        """Account for one send: bump attempts, arm the re-dispatch deadline."""
        task.attempts += 1
        task.deadline = now + self._task_timeout


# ---------------------------------------------------------------------------
# coordinator — stateless wiring around the ledger

def work_frame(task: int, attempt: int, spec: CopySpec) -> dict[str, Any]:
    """
    Encode one dispatch as a work-pipe frame: the CopySpec fields (the
    schema's single source) plus task id and attempt ordinal.
    """
    frame = asdict(spec)
    frame["task"] = task
    frame["attempt"] = attempt
    return frame


def coordinator_watchdog(*, ledger: CopyLedger,
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
       results pipe is left alone so a slow collector can finish draining
       legitimately.

    The parameter list is the watchdog's true dependency budget, spelled
    out so it can never quietly reach for state it doesn't own.
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


def summarize(stats: CopyStats, rejected: int, elapsed: float) -> int:
    """
    Final log lines + exit code: 1 if anything failed, was rejected, or
    never finished; else 0.
    """
    log(f"[coord] done: {stats.ingested} tasks, {stats.accepted} copy "
        f"requests accepted, {len(stats.failed)} failed, "
        f"{rejected} rejected, {elapsed:.1f}s")

    if stats.outstanding > 0:
        pending = stats.outstanding - (0 if stats.sealed else 1)
        note = "" if stats.sealed else " and the feed still open"
        log(f"[coord] completions pipe closed with {pending} tasks "
            f"outstanding{note} — results are partial")
        return 1

    if stats.failed:
        for tid, name in stats.failed[:10]:
            log(f"[coord]   FAILED task {tid}: {name!r}")
        if len(stats.failed) > 10:
            log(f"[coord]   ... +{len(stats.failed) - 10} more")
        return 1

    if rejected:
        return 1            # the lines were already logged at rejection time

    return 0


def run_coordinator(pipes: Pipes, cfg: CopyCfg, input: str | None) -> int:
    """
    Coordinator entry point: feed copy orders from JSONL, apply the
    ledger's decisions until outstanding hits zero, fire the drain cascade,
    and summarize.

    Needs no OCI credentials — namespaces and buckets arrive in the input;
    only workers talk to OCI.

    Exit codes: 0 clean, 1 partial / failed / rejected, 130 interrupted.
    """
    src = sys.stdin if input in (None, "-") else open(input, "r")

    try:
        wait_for_pipes((pipes.work, pipes.completions, pipes.results),
                       timeout=pipes.wait)

        label = "stdin" if src is sys.stdin else input
        log(f"[coord] feeding copy orders from {label}")

        ledger = CopyLedger(task_timeout=cfg.task_timeout,
                            max_attempts=cfg.max_attempts)

        t0 = time.monotonic()
        last_report = t0
        rejected: list[int] = []        # line numbers, logged as they happen
        loop_done = threading.Event()   # the completions loop has exited
        finishing = threading.Event()   # the drain cascade has been fired
        finish_lock = threading.Lock()  # makes the cascade fire exactly once
        send_lock = threading.Lock()    # three threads share one producer

        with qpipe.Producer.connect(pipes.work, codec="json") as work, \
             qpipe.Consumer.connect(pipes.completions, codec="json") as done:

            def push(task: int, attempt: int, spec: CopySpec) -> None:
                """The one effect on the work pipe; serialized across the
                feeder, the watchdog, and the completions loop."""
                frame = work_frame(task, attempt, spec)
                with send_lock:
                    work.send(frame)

            def apply(decision: Decision) -> None:
                """Interpret one ledger Decision — the algebra's only
                consumer; all control-plane sending and logging happens
                here."""
                match decision:
                    case Send(task=task, attempt=attempt, spec=spec):
                        push(task, attempt, spec)
                    case Retry(task=task, attempt=attempt, spec=spec,
                               why=why):
                        log(f"[coord] retry {attempt}/{cfg.max_attempts} "
                            f"task {task} ({spec.name!r}): {why}")
                        push(task, attempt, spec)
                    case Failed(task=task, name=name, attempts=attempts,
                                why=why):
                        log(f"[coord] FAILED task {task} ({name!r}) after "
                            f"{attempts} attempts: {why}")

            def finish() -> None:
                """Fire the drain cascade exactly once, whichever thread is
                first."""
                with finish_lock:
                    if finishing.is_set():
                        return
                    finishing.set()
                for addr in (pipes.work, pipes.completions, pipes.results):
                    try:
                        qpipe.request_drain(addr)
                    except Exception as e:  # noqa: BLE001 — best-effort
                        log(f"[coord] drain({addr}) failed: {e}")

            def maybe_report() -> None:
                """Progress line, throttled.

                Side effects: stderr, plus updating its own throttle
                timestamp (`last_report`) — which buys rate limiting
                without threading a clock value through the recv loop.
                """
                nonlocal last_report
                now = time.monotonic()
                if now - last_report < cfg.report_every:
                    return
                last_report = now
                s = ledger.stats()
                el = max(now - t0, 1e-9)
                log(f"[coord] outstanding={s.outstanding} "
                    f"ingested={s.ingested} accepted={s.accepted} "
                    f"feed={'sealed' if s.sealed else 'open'} "
                    f"({s.accepted / el:.0f}/s, {el:.0f}s)")

            def feed() -> None:
                """
                The supply side: parse and validate each input line, gate
                on --in-flight, ingest, and seal the ledger at EOF.

                Rejected lines are logged with their line number and never
                dispatched. If everything already finished by seal time
                (tiny or empty feed), fires the cascade itself — otherwise
                the watchdog would catch it within one tick.
                """
                n = 0
                try:
                    for line_no, raw in enumerate(src, start=1):
                        raw = raw.strip()
                        if not raw:
                            continue
                        try:
                            spec = CopySpec.from_record(json.loads(raw))
                        except (json.JSONDecodeError, ValueError) as e:
                            rejected.append(line_no)
                            log(f"[coord] rejected line {line_no}: {e}")
                            continue
                        while ledger.pending() >= cfg.in_flight:
                            time.sleep(0.05)    # backpressure — see docstring
                        apply(ledger.ingest(line_no, spec,
                                            now=time.monotonic()))
                        n += 1
                except Exception as e:  # noqa: BLE001 — pipes dying mid-feed
                    log(f"[coord] feed aborted: {type(e).__name__}: {e}")
                finally:
                    ledger.seal()
                    log(f"[coord] feed sealed: {n} tasks, "
                        f"{len(rejected)} rejected")
                    if ledger.done():
                        finish()

            feeder = threading.Thread(target=feed, daemon=True)
            watchdog = threading.Thread(
                target=coordinator_watchdog,
                kwargs=dict(ledger=ledger, apply=apply, finish=finish,
                            pipes=pipes, tick=cfg.watchdog_tick,
                            hammer=cfg.hammer, loop_done=loop_done,
                            finishing=finishing),
                daemon=True,
            )

            try:
                feeder.start()
                watchdog.start()

                for msg in done:    # EOFs only once drained or shut down
                    for decision in ledger.handle(msg, now=time.monotonic()):
                        apply(decision)
                    maybe_report()
                    if ledger.done():
                        break

            except KeyboardInterrupt:
                log("[coord] interrupted — shutting all pipes down")
                loop_done.set()
                shutdown_pipes((pipes.work, pipes.completions,
                                pipes.results))
                return 130

            finally:
                loop_done.set()

        finish()                    # no-op if the watchdog beat us to it
        return summarize(ledger.stats(), rejected=len(rejected),
                         elapsed=time.monotonic() - t0)

    finally:
        if src is not sys.stdin:
            src.close()


# ---------------------------------------------------------------------------
# worker — consume copy orders, submit server-side copies, emit results

class ClientPool:
    """
    One ObjectStorageClient per region, built on first use.

    Deliberately NOT thread-safe: each worker thread owns its own pool,
    which sidesteps any OCI SDK thread-safety questions entirely.
    """

    def __init__(self, auth: OciAuth) -> None:
        """Remember the credentials; build nothing yet."""
        self._auth = auth
        self._clients: dict[str | None, tuple[Any, str | None]] = {}

    def get(self, region: str | None) -> tuple[Any, str | None]:
        """
        Return (client, effective region) for `region` — the effective
        region is the requested one, or the profile's when None. The copy
        API needs it to default the destination region.

        Side effects: on first miss, builds the client (config-file read —
        see make_client) and caches it, buying one construction per region
        instead of one per task. Referentially transparent thereafter.
        """
        if region not in self._clients:
            client, config = make_client(self._auth, region=region)
            self._clients[region] = (client, region or config.get("region"))
        return self._clients[region]


def request_copy(client: Any, task: dict[str, Any], dst_region: str) -> str | None:
    """
    Submit one server-side copy; returns the OCI work-request id.

    copy_object is asynchronous: OCI accepts the request (202) and performs
    the copy in the background. The opc-work-request-id response header is
    the handle a later checker polls via get_work_request.

    Side effects beyond the stated job: the deferred `import oci` — same
    rationale as make_client.
    """
    import oci  # deferred on purpose — see make_client

    details = oci.object_storage.models.CopyObjectDetails(
        source_object_name=task["name"],
        destination_region=dst_region,
        destination_namespace=task["dst_namespace"],
        destination_bucket=task["dst_bucket"],
        destination_object_name=task.get("dst_name") or task["name"],
    )
    resp = client.copy_object(namespace_name=task["src_namespace"],
                              bucket_name=task["src_bucket"],
                              copy_object_details=details)

    return resp.headers.get("opc-work-request-id")


def copy_record(task: dict[str, Any], src_region: str | None,
                dst_region: str, work_request: str | None, worker: str,
                duration: float) -> dict[str, Any]:
    """
    One results-pipe record (pure): the input coordinates, the resolved
    regions, and the work-request handle a later checker needs.
    """
    return {"task": task.get("task"), "attempt": task.get("attempt"),
            "name": task.get("name"),
            "src_namespace": task.get("src_namespace"),
            "src_bucket": task.get("src_bucket"),
            "src_region": src_region,
            "dst_namespace": task.get("dst_namespace"),
            "dst_bucket": task.get("dst_bucket"),
            "dst_name": task.get("dst_name") or task.get("name"),
            "dst_region": dst_region,
            "work_request": work_request,
            "worker": worker, "duration": duration}


def worker_loop(pipes: Pipes, auth: OciAuth, wid: str) -> None:
    """
    One consume → submit → report loop.

    EOFs off the drained work pipe. Per-task OCI errors become task_error
    frames (flagged permanent for HTTP 4xx and config errors) and the loop
    keeps serving; QpipeError propagates because the pipes themselves are
    going away.

    The result record is sent BEFORE the completion frame: result sends are
    ACKed by the orchestrator first, so by the time a completion exists its
    result is already enqueued — that's what makes the post-zero drain of
    the results pipe race-free.
    """
    clients = ClientPool(auth)
    n_tasks = 0

    with qpipe.Consumer.connect(pipes.work, codec="json") as tasks, \
         qpipe.Producer.connect(pipes.results, codec="json") as results, \
         qpipe.Producer.connect(pipes.completions, codec="json") as done:

        for task in tasks:          # EOFs when the work pipe drains
            t_start = time.monotonic()
            tid = task.get("task")
            attempt = task.get("attempt")
            try:
                client, src_region = clients.get(task.get("src_region"))
                dst_region = task.get("dst_region") or src_region
                if not dst_region:
                    raise ValueError(
                        "no destination region: set dst_region in the "
                        "input or a region in the OCI profile")

                wr = request_copy(client, task, dst_region)
                dur = round(time.monotonic() - t_start, 3)

                results.send(copy_record(task, src_region, dst_region,
                                         wr, wid, dur))
                done.send({"type": "completion", "task": tid,
                           "attempt": attempt, "worker": wid,
                           "duration": dur})
                n_tasks += 1
            except qpipe.QpipeError:
                raise               # pipes are going away — stop
            except Exception as e:  # noqa: BLE001 — report, keep serving
                status = getattr(e, "status", None)  # duck-typed ServiceError
                permanent = (isinstance(e, ValueError)
                             or status in PERMANENT_HTTP_STATUSES)
                done.send({"type": "task_error", "task": tid,
                           "worker": wid, "permanent": permanent,
                           "error": f"{type(e).__name__}: {e}"})

    log(f"[worker {wid}] {n_tasks} copy requests submitted")


def run_worker(pipes: Pipes, auth: OciAuth, threads: int) -> int:
    """
    Worker entry point: spin `threads` independent loops and wait them out.

    Threads rather than processes: the qpipe bindings drop the GIL around
    all blocking pipe I/O and the OCI SDK is HTTP-bound, so threads
    parallelize this fine. Exit codes: 0 clean, 130 interrupted.
    """
    wait_for_pipes((pipes.work, pipes.completions, pipes.results),
                   timeout=pipes.wait)
    base = f"{socket.gethostname()}:{os.getpid()}"

    def boot(i: int) -> None:
        """Run one loop; downgrade expected shutdown races to a log line."""
        wid = f"{base}.{i}"
        try:
            worker_loop(pipes, auth, wid)
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
# collect — drain the results pipe to JSONL

def run_collect(results_addr: str, wait: float, output: str | None) -> int:
    """
    Collect entry point: stream the results pipe to JSONL on stdout or
    `output` — this file is the input to a later work-request checker.

    Side effects: opens `output` with "wb" — an existing file is truncated.

    Frames are already compact, newline-free JSON (the json codec guarantees
    it), so the raw codec passes the bytes straight through — no decode /
    re-encode round trip.
    """
    wait_for_pipes((results_addr,), timeout=wait)
    out = sys.stdout.buffer if output in (None, "-") else open(output, "wb")
    n = 0
    t0 = time.monotonic()

    try:
        with qpipe.Consumer.connect(results_addr, codec="raw") as recs:
            for frame in recs:      # EOFs via the drain cascade
                out.write(frame)
                out.write(b"\n")
                n += 1
                if n % 100_000 == 0:
                    el = time.monotonic() - t0
                    log(f"[collect] {n} results ({n / el:.0f}/s)")
    finally:
        if out is not sys.stdout.buffer:
            out.close()

    log(f"[collect] {n} results")
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
    the pipes down at different times, and results must be free to outlive
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
             "results": pipes.results}
    procs: dict[str, subprocess.Popen[bytes]] = {}
    logfiles: list[Any] = []

    try:
        for name, addr in addrs.items():
            f = open(logs / f"{name}.log", "ab")
            logfiles.append(f)
            # No text=/bufsize=: stdout is a real file, so the child writes
            # straight to the fd and buffers however its runtime pleases.
            procs[name] = subprocess.Popen([orchestrator, addr], env=env,
                                           stdout=f, stderr=subprocess.STDERR)
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
        p.add_argument(
            f"--{n}",
            default=default,
            metavar="HOST:PORT",
            help=f"{n} pipe orchestrator (default {default})"
        )
    p.add_argument(
        "--wait",
        type=float,
        default=DEFAULT_PIPES.wait,
        help=f"seconds to wait for pipes to come up "
             f"(default {DEFAULT_PIPES.wait:g})"
    )


def add_auth_args(p: argparse.ArgumentParser) -> None:
    """Register --oci-config / --profile (the CLI edge for OciAuth)."""
    p.add_argument(
        "--oci-config",
        default="~/.oci/config",
        metavar="PATH",
        help="OCI config file (default ~/.oci/config)"
    )
    p.add_argument(
        "--profile",
        default="DEFAULT",
        help="profile within the config file (default DEFAULT)"
    )


def build_parser() -> argparse.ArgumentParser:
    """
    Define the CLI — one subparser per role.

    Flag names are stable interface; the from_args constructors are their
    only readers.
    """
    ap = argparse.ArgumentParser(
        description="Server-side copy of OCI objects, in parallel over qpipe."
    )
    sub = ap.add_subparsers(dest="role", required=True)

    c = sub.add_parser("coordinator", help="feed, track, terminate")
    c.add_argument(
        "--input", "-i",
        default="-",
        metavar="FILE",
        help="JSONL copy orders (default '-' = stdin)"
    )
    c.add_argument(
        "--task-timeout",
        type=float,
        default=300.0,
        help="seconds before a task is re-dispatched (default 300)"
    )
    c.add_argument("--max-attempts", type=int, default=3)
    c.add_argument(
        "--in-flight",
        type=int,
        default=1000,
        help="feeder backpressure: max PENDING tasks; keep "
             "in_flight/throughput well below --task-timeout (default 1000)"
    )
    c.add_argument("--watchdog-tick", type=float, default=5.0)
    c.add_argument("--report-every", type=float, default=5.0)
    c.add_argument(
        "--hammer",
        type=float,
        default=60.0,
        help="seconds after drain before escalating to shutdown"
    )
    add_pipe_args(c, "work", "completions", "results")

    w = sub.add_parser(
        "worker",
        help="consume copy orders, submit server-side copies"
    )
    w.add_argument(
        "--threads",
        type=int,
        default=4,
        help="independent worker loops in this process (default 4)"
    )
    add_pipe_args(w, "work", "completions", "results")
    add_auth_args(w)

    g = sub.add_parser("collect", help="drain the results pipe to JSONL")
    g.add_argument(
        "--output", "-o",
        help="file (default stdout, '-' works too)"
    )
    add_pipe_args(g, "results")

    b = sub.add_parser(
        "bus", 
        help="spawn + supervise the pipe orchestrators "
             "(the message-exchange fabric)"
    )
    b.add_argument(
        "--logdir",
        type=Path,
        default=Path.cwd() / "cp-logs",
        metavar="DIR",
        help="directory for per-orchestrator logs (default ./cp-logs)"
    )
    b.add_argument(
        "--rust-log",
        default="debug",
        help="RUST_LOG for the spawned orchestrators (default debug)"
    )
    b.add_argument(
        "--orchestrator",
        default="orchestrator",
        metavar="BIN",
        help="orchestrator binary to spawn (default: 'orchestrator' from PATH)"
    )
    add_pipe_args(b, "work", "completions", "results")

    return ap


def main(argv: list[str] | None = None) -> int:
    """
    Parse the CLI and hand explicit configuration to one role's entry point.
    """
    args = build_parser().parse_args(argv)

    if args.role == "coordinator":
        return run_coordinator(pipes=Pipes.from_args(args),
                               cfg=CopyCfg.from_args(args),
                               input=args.input)

    if args.role == "worker":
        return run_worker(pipes=Pipes.from_args(args),
                          auth=OciAuth.from_args(args),
                          threads=args.threads)

    if args.role == "collect":
        return run_collect(results_addr=args.results, wait=args.wait,
                           output=args.output)

    if args.role == "bus":
        return run_bus(pipes=Pipes.from_args(args), logs=args.logdir,
                       rust_log=args.rust_log, orchestrator=args.orchestrator)

    raise AssertionError(f"unhandled role {args.role!r}")  # unreachable


if __name__ == "__main__":
    sys.exit(main())
