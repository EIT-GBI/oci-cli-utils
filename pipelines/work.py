#!/usr/bin/env python
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
qpipe.work — a work-distribution harness over qpipe.

qpipe gives you pipes; this gives you work distributed over pipes. A
pipeline author supplies two strategy objects and gets the whole harness —
coordinator, worker pool, collector, and orchestrator supervisor — for free:

    inputs ─▶ coordinator ──work──▶ worker pool ──completions──▶ coordinator
                                        │
                                        └──results──▶ collect / downstream

The two patterns this replaces are one pattern
  - work-generating (e.g. a recursive bucket listing): a seed begets more
    work as workers discover it.
  - pipelining (e.g. a copy or status-check stream): a list of inputs feeds
    straight through, nothing begets.
  They differ in exactly one thing — whether worker output feeds back as new
  work — so they differ in exactly one hook: Coordinator.expand. Its default
  returns nothing (pipelining); override it and you have work-generation. The
  coordinator loop, the ledger, and the termination algebra are identical
  either way and never branch on "which pattern".

The contract (two structs of functions, no inheritance, state explicit)
  Coordinator
    seeds()                 -> Iterator[Spec]      the inputs
    expand(parent, disc)    -> Iterable[Spec]      discovery -> new specs
                                                   (default: none)
    key_of(spec)            -> Hashable | None     dedup identity
                                                   (default: monotonic ids)
  Worker
    setup()                 -> S                   per-thread state (clients…)
    process(S, job, result, discover) -> None      do one task
      result(rec)   sends a record to the results pipe (data plane — yours)
      discover(d)   reports discovered work to the coordinator (a beget)
      return        success;  raise Permanent(...)  fails without retry;
                    raise anything else            retries up to --max-attempts
  Spec and the discovery dict are opaque to the harness — meaningful only to
  your strategies. The harness owns the control plane; the results pipe is
  entirely yours (put a nushell-flat schema there).

Wire protocol (harness-owned; do not emit these shapes yourself)
  work    {"task":int, "attempt":int, "spec":<your spec dict>}
  beget   {"k":"beget", "task":int, "spec":<your discovery dict>}
  done    {"k":"done",  "task":int, "attempt":int, "worker":str,
           "duration":float}
  error   {"k":"error", "task":int, "worker":str, "why":str,
           "permanent":bool}

What the harness guarantees so your worker can't get it wrong
  - result-before-done: the wrapper sends `done` only after process() returns,
    and every result()/discover() call ACKs before it does — so when the
    coordinator sees a task's `done`, that task's results are already enqueued.
    This is what makes the post-zero drain of the results pipe race-free.
  - beget-before-done: begets and the done ride the same producer on the
    completions pipe (FIFO), so a task's child registrations are always
    applied before its retirement — which is what keeps termination sound.
  - at-least-once with timeout/permanent retry, the drain cascade, the
    watchdog hammer, SIGTERM teardown, Ctrl-C → 130.

Termination (one counter algebra, with the input source as the root task)
  outstanding starts at 1 — the source token. Each registered spec +1, each
  completion or terminal failure -1, source exhaustion seals the token (-1),
  each beget +1. outstanding == 0 therefore means the source is sealed AND
  every task is terminal — for both patterns, and nothing can hit zero before
  seal, so coordinator thread start order is a don't-care.

Flow control (--in-flight)
  Seeds are gated: the feeder blocks once --in-flight tasks are pending, so a
  supply-paced stream (stdin) can't out-dispatch the workers into a retry
  storm (a task's deadline is armed at dispatch). Begets are NOT gated — they
  are self-paced by completions; a very wide discovery tree can still grow the
  queue (acceptable near branching factor 1; revisit if you fan out hugely).

Effect convention (the house rule)
  A side effect is legitimate only if (a) it is the function's stated job —
  named I/O at the edge: run, push, log, *_pipes, finish, stop_orchestrators —
  or (b) the docstring carries a "Side effects:" line saying what it buys.
  The harness never imports oci (or any domain SDK): Worker.setup returns
  opaque state, so clients live entirely in your code, and collect/bus stay
  dependency-free.

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

from collections.abc import Callable, Hashable, Iterable, Iterator, Sequence
from dataclasses     import dataclass
from typing          import Any

import qpipe

__all__ = ["Permanent", "Spec", "Discovery", "Emit", "Discover", "Job",
           "Coordinator", "Worker", "Pipeline", "Pipes", "run"]


def log(msg: str) -> None:
    """Write one diagnostic line to stderr, unbuffered (frames go to pipes)."""
    print(msg, file=sys.stderr, flush=True)


def _short(spec: "Spec", limit: int = 100) -> str:
    """Truncated repr of an opaque spec, for failure logs."""
    text = repr(spec)
    return text if len(text) <= limit else text[: limit - 1] + "…"


# ---------------------------------------------------------------------------
# the contract — what a pipeline author supplies

class Permanent(Exception):
    """
    Raise from Worker.process for a failure that must NOT be retried (e.g. an
    HTTP 4xx). Anything else process raises is retried up to --max-attempts.
    """


Spec = dict[str, Any]                       # opaque to the harness
Discovery = dict[str, Any]                  # opaque to the harness
Emit = Callable[[dict[str, Any]], None]     # result(rec)
Discover = Callable[[dict[str, Any]], None] # discover(disc)


@dataclass(frozen=True, slots=True)
class Job:
    """The harness's view of one dispatch, handed to Worker.process."""

    task: int       # stable task id
    attempt: int    # 1-based send ordinal (echo into result records to dedup)
    spec: Spec      # your opaque payload


@dataclass(frozen=True, slots=True)
class Coordinator:
    """
    The supply + branching strategy. `expand`'s default makes this a
    pipelining coordinator; override it for work-generation. `key_of`'s
    default gives every spec a fresh id; provide it to dedup by identity.
    """

    seeds: Callable[[], Iterator[Spec]]
    expand: Callable[[Spec, Discovery], Iterable[Spec]] = lambda parent, d: ()
    key_of: Callable[[Spec], Hashable] | None = None


@dataclass(frozen=True, slots=True)
class Worker:
    """The per-task work strategy. `setup` builds per-thread state once."""

    setup: Callable[[], Any]
    process: Callable[[Any, Job, Emit, Discover], None]


@dataclass(frozen=True, slots=True)
class Pipeline:
    """
    Everything `run` needs to stand up a pipeline. make_coordinator and
    make_worker are the ONLY place argparse.Namespace is seen — they close
    the parsed config into the strategy callables, exactly as a *.from_args
    constructor does. add_*_args contribute role-specific CLI flags.
    """

    name: str
    describe: str
    default_pipes: "Pipes"
    make_coordinator: Callable[[argparse.Namespace], Coordinator]
    make_worker: Callable[[argparse.Namespace], Worker]
    add_coordinator_args: Callable[[argparse.ArgumentParser], None] = \
        lambda p: None
    add_worker_args: Callable[[argparse.ArgumentParser], None] = \
        lambda p: None


# ---------------------------------------------------------------------------
# configuration — frozen values built once at the CLI edge

@dataclass(frozen=True, slots=True)
class Pipes:
    """Addresses of the three qpipe orchestrators, plus the startup-wait."""

    work: str
    completions: str
    results: str
    wait: float

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Pipes":
        """Lift the pipe addresses out of parsed args."""
        return cls(work=args.work, completions=args.completions,
                   results=args.results, wait=args.wait)


@dataclass(frozen=True, slots=True)
class CoordinatorCfg:
    """Harness-owned coordinator policy: retries, flow control, timers."""

    task_timeout: float
    max_attempts: int
    in_flight: int
    watchdog_tick: float
    report_every: float
    hammer: float

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "CoordinatorCfg":
        """Lift the common coordinator flags out of parsed args."""
        return cls(task_timeout=args.task_timeout,
                   max_attempts=args.max_attempts, in_flight=args.in_flight,
                   watchdog_tick=args.watchdog_tick,
                   report_every=args.report_every, hammer=args.hammer)


# ---------------------------------------------------------------------------
# pipe plumbing

def wait_for_pipes(addrs: Sequence[str], timeout: float) -> None:
    """Block until every listed orchestrator passes healthcheck (raises on
    timeout)."""
    for addr in addrs:
        qpipe.wait_until_healthy(addr, timeout=timeout)


def shutdown_pipes(addrs: Sequence[str]) -> None:
    """Best-effort request_shutdown on every listed orchestrator (Ctrl-C /
    hammer)."""
    for addr in addrs:
        try:
            qpipe.request_shutdown(addr)
        except Exception:  # noqa: BLE001 — going down anyway
            pass


# ---------------------------------------------------------------------------
# ledger state — generic, sans-I/O, sans-strategy

class TaskState(enum.Enum):
    """Lifecycle of a dispatched task. DONE and FAILED are terminal."""

    PENDING = enum.auto()
    DONE = enum.auto()
    FAILED = enum.auto()


@dataclass(slots=True)
class _Task:
    """One task's record. Lives inside Ledger; mutated only under its lock."""

    task_id: int
    spec: Spec
    state: TaskState = TaskState.PENDING
    attempts: int = 0
    deadline: float = 0.0


@dataclass(frozen=True, slots=True)
class Stats:
    """A consistent point-in-time snapshot of the ledger's counters."""

    outstanding: int
    tasks: int                              # registered (excludes deduped)
    done: int                               # completed
    failed: tuple[tuple[int, Spec], ...]    # (task id, spec)
    sealed: bool


# Decisions — the ledger's entire output vocabulary. Send/Retry carry the
# spec so the coordinator can build the work frame; the spec is treated as
# immutable (never mutated by harness or pipeline), so the snapshot is safe
# even though the ledger holds the same reference.

@dataclass(frozen=True, slots=True)
class Send:
    """Decision: dispatch this task's first attempt to the work pipe."""

    task: int
    attempt: int
    spec: Spec


@dataclass(frozen=True, slots=True)
class Retry:
    """Decision: re-dispatch a task that timed out or errored retryably."""

    task: int
    attempt: int
    spec: Spec
    why: str


@dataclass(frozen=True, slots=True)
class Failed:
    """Decision: a task is terminally failed; report it, send nothing."""

    task: int
    attempts: int
    spec: Spec
    why: str


Decision = Send | Retry | Failed


class Ledger:
    """
    Bookkeeping for the at-least-once protocol — generic, and nothing else.

    Owns the counter algebra from the module docstring, with the input source
    as the root task: outstanding starts at 1, register() +1, completion or
    terminal failure -1, seal() -1 once at source exhaustion, begets are just
    more register() calls. outstanding == 0 is global done — after seal.

    Sans-I/O AND sans-strategy: frames-worth-of-data go in, frozen Decision
    values come out; it never writes a pipe, logs, or calls user code
    (`key_of` is the single closure it holds, for dedup identity only). Its
    lock-guarded mutation is the documented exception. Time is injected, so
    retry/timeout/termination tests are equality assertions on Decision lists.
    """

    def __init__(self, key_of: Callable[[Spec], Hashable] | None,
                 task_timeout: float, max_attempts: int) -> None:
        """Set dedup identity and policy; outstanding starts at 1 (source)."""
        self._lock = threading.Lock()
        self._key_of = key_of
        self._tasks: dict[int, _Task] = {}
        self._seen: dict[Hashable, int] = {}   # key -> task id, if key_of
        self._next = 0
        self._outstanding = 1                  # the source token
        self._sealed = False
        self._done = 0
        self._failed: list[tuple[int, Spec]] = []
        self._task_timeout = task_timeout
        self._max_attempts = max_attempts

    # -- public protocol -----------------------------------------------------

    def register(self, spec: Spec, now: float) -> list[Decision]:
        """
        Register one spec (a seed or a begotten child); returns the Send to
        apply, or [] if key_of dedups it against a known task.
        """
        with self._lock:
            if self._key_of is not None:
                key = self._key_of(spec)
                if key in self._seen:
                    return []
            self._next += 1
            tid = self._next
            task = _Task(task_id=tid, spec=spec)
            self._tasks[tid] = task
            if self._key_of is not None:
                self._seen[self._key_of(spec)] = tid
            self._outstanding += 1
            self._stamp(task, now)
            return [Send(task=tid, attempt=task.attempts, spec=spec)]

    def seal(self) -> None:
        """Mark the source exhausted: the root token completes (-1).
        Idempotent."""
        with self._lock:
            if not self._sealed:
                self._sealed = True
                self._outstanding -= 1

    def spec_of(self, task_id: int) -> Spec | None:
        """The spec of a known task, or None — used to expand a beget's
        parent."""
        with self._lock:
            task = self._tasks.get(task_id)
            return task.spec if task is not None else None

    def complete(self, task_id: int) -> list[Decision]:
        """Retire a task on its `done` frame (no new work). Late/dup
        no-op."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is not None and task.state is TaskState.PENDING:
                task.state = TaskState.DONE
                self._outstanding -= 1
                self._done += 1
            return []

    def fail_or_retry(self, task_id: int, why: str, now: float,
                      *, permanent: bool) -> list[Decision]:
        """Apply an `error` frame: one Retry, or one Failed. Late/dup
        no-op."""
        with self._lock:
            task = self._tasks.get(task_id)
            if task is None or task.state is not TaskState.PENDING:
                return []
            return self._retry_or_fail(task, why, now, permanent=permanent)

    def expired(self, now: float) -> list[Decision]:
        """Sweep overdue PENDING tasks; return Retry/Failed decisions."""
        decisions: list[Decision] = []
        with self._lock:
            for task in self._tasks.values():
                if task.state is TaskState.PENDING and now > task.deadline:
                    decisions.extend(
                        self._retry_or_fail(task, "timeout", now,
                                            permanent=False))
        return decisions

    def pending(self) -> int:
        """PENDING task count (the source token excluded) — feeder
        backpressure."""
        with self._lock:
            return self._outstanding - (0 if self._sealed else 1)

    def done(self) -> bool:
        """True once outstanding == 0 — sealed source AND all tasks
        terminal."""
        with self._lock:
            return self._outstanding == 0

    def stats(self) -> Stats:
        """Consistent snapshot for reporting and the final summary."""
        with self._lock:
            return Stats(outstanding=self._outstanding,
                         tasks=len(self._tasks), done=self._done,
                         failed=tuple(self._failed), sealed=self._sealed)

    # -- internals (call only with self._lock held) ---------------------------

    def _retry_or_fail(self, task: _Task, why: str, now: float,
                       *, permanent: bool) -> list[Decision]:
        """Decide: another attempt (Retry) or terminal failure (Failed)."""
        if not permanent and task.attempts < self._max_attempts:
            self._stamp(task, now)
            return [Retry(task=task.task_id, attempt=task.attempts,
                          spec=task.spec, why=why)]

        task.state = TaskState.FAILED
        self._outstanding -= 1
        self._failed.append((task.task_id, task.spec))
        return [Failed(task=task.task_id, attempts=task.attempts,
                       spec=task.spec,
                       why=f"{why} (permanent)" if permanent else why)]

    def _stamp(self, task: _Task, now: float) -> None:
        """Account for one send: bump attempts, arm the re-dispatch
        deadline."""
        task.attempts += 1
        task.deadline = now + self._task_timeout


# ---------------------------------------------------------------------------
# coordinator — stateless wiring around the ledger and the strategy

def _coordinator_watchdog(*, name: str, ledger: Ledger,
                          apply: Callable[[Decision], None],
                          finish: Callable[[], None], pipes: Pipes,
                          tick: float, hammer: float,
                          loop_done: threading.Event,
                          finishing: threading.Event) -> None:
    """
    Timer half of the coordinator. Every `tick`: apply timeout decisions and,
    if a sweep drains outstanding to zero, fire the cascade (the completions
    loop is blocked in recv(); draining its pipe is what wakes it). Then the
    hammer: if the loop's EOF hasn't arrived `hammer` seconds after the
    cascade fired, escalate to shutdown on work+completions (results is left
    for a slow collector).
    """
    while not loop_done.wait(tick):
        for decision in ledger.expired(time.monotonic()):
            try:
                apply(decision)
            except Exception as e:  # noqa: BLE001 — pipe dying mid-retry
                log(f"[{name}] applying {decision!r} failed: {e}")
                return
        if ledger.done():
            finish()
            break

    if finishing.is_set() and not loop_done.wait(hammer):
        log(f"[{name}] no EOF {hammer:.0f}s after drain — "
            f"escalating to shutdown")
        shutdown_pipes((pipes.work, pipes.completions))


def _summarize(name: str, stats: Stats, elapsed: float) -> int:
    """Final log lines + exit code: 1 if anything failed or never finished."""
    log(f"[{name}] done: {stats.tasks} tasks, {stats.done} completed, "
        f"{len(stats.failed)} failed, {elapsed:.1f}s")

    if stats.outstanding > 0:
        pending = stats.outstanding - (0 if stats.sealed else 1)
        note = "" if stats.sealed else " and the source still open"
        log(f"[{name}] completions closed with {pending} tasks "
            f"outstanding{note} — results are partial")
        return 1

    if stats.failed:
        for tid, spec in stats.failed[:10]:
            log(f"[{name}]   FAILED task {tid}: {_short(spec)}")
        if len(stats.failed) > 10:
            log(f"[{name}]   ... +{len(stats.failed) - 10} more")
        return 1

    return 0


def _run_coordinator(name: str, pipes: Pipes, cfg: CoordinatorCfg,
                     coordinator: Coordinator) -> int:
    """
    Coordinator entry point: feed seeds, apply the ledger's decisions until
    outstanding hits zero, fire the drain cascade, summarize.

    Exit codes: 0 clean, 1 partial / failed, 130 interrupted.
    """
    wait_for_pipes((pipes.work, pipes.completions, pipes.results),
                   timeout=pipes.wait)
    log(f"[{name}] coordinator up")

    ledger = Ledger(key_of=coordinator.key_of,
                    task_timeout=cfg.task_timeout,
                    max_attempts=cfg.max_attempts)

    t0 = time.monotonic()
    last_report = t0
    loop_done = threading.Event()
    finishing = threading.Event()
    finish_lock = threading.Lock()
    send_lock = threading.Lock()

    with qpipe.Producer.connect(pipes.work, codec="json") as work, \
         qpipe.Consumer.connect(pipes.completions, codec="json") as control:

        def push(task: int, attempt: int, spec: Spec) -> None:
            """The one effect on the work pipe; serialized across threads."""
            with send_lock:
                work.send({"task": task, "attempt": attempt, "spec": spec})

        def apply(decision: Decision) -> None:
            """Interpret one ledger Decision — the algebra's only consumer."""
            match decision:
                case Send(task=task, attempt=attempt, spec=spec):
                    push(task, attempt, spec)
                case Retry(task=task, attempt=attempt, spec=spec, why=why):
                    log(f"[{name}] retry {attempt}/{cfg.max_attempts} "
                        f"task {task} ({_short(spec)}): {why}")
                    push(task, attempt, spec)
                case Failed(task=task, attempts=attempts, spec=spec, why=why):
                    log(f"[{name}] FAILED task {task} after {attempts} "
                        f"attempts: {why} — {_short(spec)}")

        def handle(msg: dict[str, Any], now: float) -> list[Decision]:
            """Turn one completions-pipe frame into Decisions (calls the
            strategy's expand for begets)."""
            kind = msg.get("k")
            if kind == "done":
                return ledger.complete(msg.get("task"))
            if kind == "error":
                return ledger.fail_or_retry(
                    msg.get("task"), str(msg.get("why", "")), now,
                    permanent=bool(msg.get("permanent")))
            if kind == "beget":
                parent = ledger.spec_of(msg.get("task"))
                if parent is None:
                    return []           # stale beget for an unknown task
                decisions: list[Decision] = []
                for child in coordinator.expand(parent, msg.get("spec") or {}):
                    decisions.extend(ledger.register(child, now))
                return decisions
            return []

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
                    log(f"[{name}] drain({addr}) failed: {e}")

        def maybe_report() -> None:
            """Throttled progress line.

            Side effects: stderr + its own throttle timestamp — buying rate
            limiting without threading a clock through the recv loop.
            """
            nonlocal last_report
            now = time.monotonic()
            if now - last_report < cfg.report_every:
                return
            last_report = now
            s = ledger.stats()
            el = max(now - t0, 1e-9)
            log(f"[{name}] outstanding={s.outstanding} tasks={s.tasks} "
                f"done={s.done} source={'sealed' if s.sealed else 'open'} "
                f"({s.done / el:.0f}/s, {el:.0f}s)")

        def feed() -> None:
            """
            The supply side: pull from coordinator.seeds(), gate on
            --in-flight, register, and seal at exhaustion. If everything
            already finished by seal time (empty/tiny source), fires the
            cascade itself — otherwise the watchdog catches it within a tick.

            A seeds() that raises is treated as a fatal source: log, seal
            (the run ends partial). Per-item soft rejects are the strategy's
            to log-and-skip.
            """
            n = 0
            try:
                for spec in coordinator.seeds():
                    while ledger.pending() >= cfg.in_flight:
                        time.sleep(0.05)        # backpressure — see docstring
                    for decision in ledger.register(spec, time.monotonic()):
                        apply(decision)
                    n += 1
            except Exception as e:  # noqa: BLE001 — pipes/source dying
                log(f"[{name}] seed feed aborted: {type(e).__name__}: {e}")
            finally:
                ledger.seal()
                log(f"[{name}] source sealed: {n} seeds")
                if ledger.done():
                    finish()

        feeder = threading.Thread(target=feed, daemon=True)
        watchdog = threading.Thread(
            target=_coordinator_watchdog,
            kwargs=dict(name=name, ledger=ledger, apply=apply, finish=finish,
                        pipes=pipes, tick=cfg.watchdog_tick,
                        hammer=cfg.hammer, loop_done=loop_done,
                        finishing=finishing),
            daemon=True,
        )

        try:
            feeder.start()
            watchdog.start()

            for msg in control:     # EOFs only once drained or shut down
                for decision in handle(msg, now=time.monotonic()):
                    apply(decision)
                maybe_report()
                if ledger.done():
                    break

        except KeyboardInterrupt:
            log(f"[{name}] interrupted — shutting all pipes down")
            loop_done.set()
            shutdown_pipes((pipes.work, pipes.completions, pipes.results))
            return 130

        finally:
            loop_done.set()

    finish()                        # no-op if the watchdog beat us to it
    return _summarize(name, ledger.stats(), elapsed=time.monotonic() - t0)


# ---------------------------------------------------------------------------
# worker — drive the strategy, own the protocol envelopes

def _worker_loop(name: str, pipes: Pipes, worker: Worker, wid: str) -> None:
    """
    One consume → process → report loop. The result-before-done and
    beget-before-done orderings live here, not in process(): the `done`
    frame is sent only after process() returns, and result()/discover()
    have already ACKed. QpipeError propagates (pipes going away); Permanent
    becomes a non-retryable error frame, anything else a retryable one.
    """
    state = worker.setup()
    n = 0

    with qpipe.Consumer.connect(pipes.work, codec="json") as tasks, \
         qpipe.Producer.connect(pipes.results, codec="json") as results, \
         qpipe.Producer.connect(pipes.completions, codec="json") as control:

        for frame in tasks:         # EOFs when the work pipe drains
            job = Job(task=frame["task"], attempt=frame["attempt"],
                      spec=frame["spec"])
            t0 = time.monotonic()

            def discover(disc: dict[str, Any]) -> None:
                """Report discovered work (a beget) for this job."""
                control.send({"k": "beget", "task": job.task, "spec": disc})

            try:
                worker.process(state, job, results.send, discover)
            except qpipe.QpipeError:
                raise               # pipes are going away — stop
            except Permanent as e:
                control.send({"k": "error", "task": job.task, "worker": wid,
                              "why": str(e), "permanent": True})
                continue
            except Exception as e:  # noqa: BLE001 — report, keep serving
                control.send({"k": "error", "task": job.task, "worker": wid,
                              "why": f"{type(e).__name__}: {e}",
                              "permanent": False})
                continue

            control.send({"k": "done", "task": job.task,
                          "attempt": job.attempt, "worker": wid,
                          "duration": round(time.monotonic() - t0, 3)})
            n += 1

    log(f"[{name} worker {wid}] {n} tasks")


def _run_worker(name: str, pipes: Pipes, worker: Worker, threads: int) -> int:
    """
    Worker entry point: spin `threads` independent loops and wait them out.
    Exit codes: 0 clean, 130 interrupted.
    """
    wait_for_pipes((pipes.work, pipes.completions, pipes.results),
                   timeout=pipes.wait)
    base = f"{socket.gethostname()}:{os.getpid()}"

    def boot(i: int) -> None:
        """Run one loop; downgrade expected shutdown races to a log line."""
        wid = f"{base}.{i}"
        try:
            _worker_loop(name, pipes, worker, wid)
        except qpipe.QpipeError as e:
            log(f"[{name} worker {wid}] pipe closed: {e}")
        except Exception as e:      # noqa: BLE001
            log(f"[{name} worker {wid}] fatal: {type(e).__name__}: {e}")

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

def _run_collect(results_addr: str, wait: float, output: str | None) -> int:
    """
    Collect entry point: stream the results pipe to JSONL on stdout or
    `output`.

    Side effects: opens `output` with "wb" — an existing file is truncated.

    Frames are already compact, newline-free JSON (the json codec guarantees
    it), so the raw codec passes the bytes straight through.
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

# Per-pipe stderr tags, padded to a common width so the prefixes line up in
# the terminal. The tag names each pipe's CONSUMER — work is drained by the
# workers, completions by the coordinator, results by collect — so flip these
# if you'd rather think of the pipes by producer.
_BUS_TAGS = {"work": "work", "completions": "coord", "results": "col"}


def stop_orchestrators(procs: dict[str, subprocess.Popen[str]],
                       grace: float = 10.0) -> None:
    """
    SIGTERM every still-running orchestrator, then SIGKILL the stragglers.
    Idempotent and best-effort — safe from both the supervisor and teardown.
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


def _pump(tag: str, proc: subprocess.Popen[str],
          write_lock: threading.Lock) -> None:
    """
    Forward one orchestrator's stderr to our stderr, one tagged line at a
    time. The child's stdout is not ours to touch — see the spawn site.

    Side effects (its entire job): reads proc.stderr to EOF and writes each
    line to sys.stderr under `write_lock`, so lines from the three pumps never
    interleave mid-line. It needs no stop signal — the child closing the pipe
    (i.e. exiting) is its EOF, which ends the loop.
    """
    if proc.stderr is None:             # unreachable with stderr=PIPE; for mypy
        return
    prefix = f"[{tag:<5}] "
    for line in proc.stderr:            # text mode: str, already newline-ended
        with write_lock:
            sys.stderr.write(prefix + line)
            sys.stderr.flush()


def _bus_healthy(procs: dict[str, subprocess.Popen[str]],
                 addrs: dict[str, str], timeout: float) -> bool:
    """
    Gate on every orchestrator's healthcheck within a shared budget,
    interleaving the probe with a liveness check so a child that dies
    immediately (bound port, bad flags) fails in ~1 s. Returns False on
    death or timeout; the caller owns the teardown.
    """
    deadline = time.monotonic() + timeout
    for name, addr in addrs.items():
        while True:
            rc = procs[name].poll()
            if rc is not None:
                log(f"[bus] {name} died before its healthcheck (rc={rc}) — "
                    f"its last stderr is tagged above")
                return False
            budget = deadline - time.monotonic()
            if budget <= 0:
                log(f"[bus] {name} not healthy after {timeout:g}s")
                return False
            try:
                qpipe.wait_until_healthy(addr, timeout=min(1.0, budget))
                break
            except qpipe.QpipeError:
                continue
    return True


def _supervise_bus(procs: dict[str, subprocess.Popen[str]]) -> int:
    """
    Wait for every orchestrator; the first NONZERO exit tears the survivors
    down. Clean exits may stagger — the drain cascade shuts pipes down at
    different times, and results must outlive work/completions for slow
    collectors.
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


def _run_bus(pipes: Pipes, rust_log: str, orchestrator: str) -> int:
    """
    Bus entry point: spawn one orchestrator per pipe, gate on their
    healthchecks (--wait budget), supervise until they exit.

    Each child's stderr is forwarded to THIS process's stderr, every line
    prefixed by a per-pipe tag ([work ] / [coord] / [col  ]). Each child's
    stdout is left inherited: the data plane passes straight through to the
    bus's own stdout, untouched and untagged, so results stay pipelineable.
    Assumes orchestrators exit once their pipe is drained/shut down;
    if they are run-forever servers, the bus ends only via signal.

    Side effects beyond the stated job: installs a SIGTERM handler converting
    the signal to SystemExit — a teardown path under systemd/Slurm/k8s stops,
    which otherwise orphan the children on their ports.

    Exit codes: 0 all clean, 1 spawn/health/crash, 130 interrupted; SIGTERM
    propagates as 143 after teardown.
    """
    env = os.environ.copy()
    env["RUST_LOG"] = rust_log

    def on_sigterm(signum: int, frame: Any) -> None:
        """Turn SIGTERM into an exception so `finally` runs the teardown."""
        log("[bus] SIGTERM — shutting the bus down")
        sys.exit(143)

    signal.signal(signal.SIGTERM, on_sigterm)

    addrs = {"work": pipes.work, "completions": pipes.completions,
             "results": pipes.results}
    procs: dict[str, subprocess.Popen[str]] = {}
    pumps: list[threading.Thread] = []
    write_lock = threading.Lock()

    try:
        for name, addr in addrs.items():
            # Stream discipline across the process boundary: stderr is the
            # diagnostic plane — ours to adopt, so PIPE it and pump it to
            # our own stderr, tagged. stdout is the data plane — not ours
            # to touch: left inherited (stdout=None, spelled out), the
            # child writes straight through to whatever the bus's stdout
            # is connected to, with no tag and no Python thread in the
            # data path. text=True shapes only the stderr pipe we read.
            proc = subprocess.Popen([orchestrator, addr], env=env,
                                    stdout=None, stderr=subprocess.PIPE,
                                    text=True)
            procs[name] = proc
            pump = threading.Thread(target=_pump,
                                    args=(_BUS_TAGS[name], proc, write_lock),
                                    daemon=True)
            pump.start()
            pumps.append(pump)
            log(f"[bus] {name} pid={proc.pid} on {addr}")

        if not _bus_healthy(procs, addrs, timeout=pipes.wait):
            return 1

        log("[bus] all orchestrators healthy — supervising")
        return _supervise_bus(procs)

    except FileNotFoundError as e:
        log(f"[bus] cannot spawn {orchestrator!r}: {e}")
        return 1
    except KeyboardInterrupt:
        log("[bus] interrupted — stopping orchestrators")
        return 130
    finally:
        stop_orchestrators(procs)       # children die -> pipes EOF
        for pump in pumps:              # drain the tails, then the pumps end
            pump.join(timeout=2.0)


# ---------------------------------------------------------------------------
# CLI edge — argparse lives here and in the pipeline's make_*/add_* hooks

def add_pipe_args(p: argparse.ArgumentParser, defaults: Pipes,
                  *names: str) -> None:
    """Register --work/--completions/--results overrides (defaults from the
    pipeline) plus the shared --wait."""
    for n in names:
        d = getattr(defaults, n)
        p.add_argument(f"--{n}", default=d, metavar="HOST:PORT",
                       help=f"{n} pipe orchestrator (default {d})")
    p.add_argument("--wait", type=float, default=defaults.wait,
                   help=f"seconds to wait for pipes (default {defaults.wait:g})")


def _add_coordinator_common(p: argparse.ArgumentParser) -> None:
    """The harness-owned coordinator policy flags."""
    p.add_argument("--task-timeout", type=float, default=300.0,
                   help="seconds before a task is re-dispatched (default 300)")
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--in-flight", type=int, default=1000,
                   help="feeder backpressure: max PENDING tasks; keep "
                        "in_flight/throughput below --task-timeout "
                        "(default 1000)")
    p.add_argument("--watchdog-tick", type=float, default=5.0)
    p.add_argument("--report-every", type=float, default=5.0)
    p.add_argument("--hammer", type=float, default=60.0,
                   help="seconds after drain before escalating to shutdown")


def _build_parser(pipeline: Pipeline) -> argparse.ArgumentParser:
    """Assemble the CLI: four roles, harness flags + the pipeline's extras."""
    d = pipeline.default_pipes
    ap = argparse.ArgumentParser(description=pipeline.describe)
    sub = ap.add_subparsers(dest="role", required=True)

    c = sub.add_parser("coordinator", help="feed, track, terminate")
    _add_coordinator_common(c)
    pipeline.add_coordinator_args(c)
    add_pipe_args(c, d, "work", "completions", "results")

    w = sub.add_parser("worker", help="consume tasks, do the work")
    w.add_argument("--threads", type=int, default=4,
                   help="independent worker loops in this process (default 4)")
    pipeline.add_worker_args(w)
    add_pipe_args(w, d, "work", "completions", "results")

    g = sub.add_parser("collect", help="drain the results pipe to JSONL")
    g.add_argument("--output", "-o",
                   help="file (default stdout, '-' works too)")
    add_pipe_args(g, d, "results")

    b = sub.add_parser("bus", help="spawn + supervise the pipe orchestrators")
    b.add_argument("--rust-log", default="debug",
                   help="RUST_LOG for the orchestrators (default debug)")
    b.add_argument("--orchestrator", default="orchestrator", metavar="BIN",
                   help="orchestrator binary to spawn (default from PATH)")
    add_pipe_args(b, d, "work", "completions", "results")

    return ap


def run(pipeline: Pipeline, argv: list[str] | None = None) -> int:
    """
    The pipeline's single entry point. Parses the CLI and dispatches to the
    requested role. Each branch reads only the flags its own subparser
    registered — collect's parser deliberately knows just --results/--wait,
    so Pipes is constructed per-branch, never hoisted above the dispatch
    (a shared Pipes.from_args up here is the bug that silently asserts
    "every subparser defines every pipe flag"). argparse.Namespace dies
    here and in the pipeline's make_*/add_* hooks.
    """
    args = _build_parser(pipeline).parse_args(argv)

    if args.role == "coordinator":
        return _run_coordinator(pipeline.name, Pipes.from_args(args),
                                CoordinatorCfg.from_args(args),
                                pipeline.make_coordinator(args))

    if args.role == "worker":
        return _run_worker(pipeline.name, Pipes.from_args(args),
                           pipeline.make_worker(args), args.threads)

    if args.role == "collect":
        return _run_collect(args.results, args.wait, args.output)

    if args.role == "bus":
        return _run_bus(Pipes.from_args(args), args.rust_log,
                        args.orchestrator)

    raise AssertionError(f"unhandled role {args.role!r}")  # unreachable
