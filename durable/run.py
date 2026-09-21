"""Run every durable scenario of SPEC.md against the live mvm-demo-durable stack and record it.

    AWS_PROFILE=heisenberg python3 durable/run.py                      # all scenarios, one at a time
    AWS_PROFILE=heisenberg python3 durable/run.py --only single,hang    # a subset
    AWS_PROFILE=heisenberg python3 durable/run.py --report   # rebuild results/durable/README.md

Each scenario invokes `mvm-demo-durable-orchestrator:live` asynchronously (InvocationType
Event with a deterministic DurableExecutionName), for the two approval scenarios answers
the request approver.py finds on the mvm-demo-durable-approvals queue, polls
`get_durable_execution` until the execution ends, and writes results/durable/<scenario>/:

    input.json              the exact event
    output.json             get_durable_execution (status, service timestamps, the handler's result)
    history.json            every event of get_durable_execution_history, all pages
    history.txt             event type, name, timestamp, seconds since the first event
    vm-logs.txt             /aws/lambda-microvms/demo-agent, the streams of the VMs this run launched
    orchestrator-logs.txt   /aws/lambda/mvm-demo-durable-orchestrator between start and end
    summary.json            the SPEC's summary shape; matches_expected is computed from the output and history

Times come from the service (StartTimestamp / EndTimestamp), never from the poll loop.
After each scenario every VM it launched is checked and terminated if still alive (noted).
Nothing here reads or prints credentials.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import time
from pathlib import Path

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import approver  # noqa: E402

REGION = os.environ.get("MVM_REGION", "us-east-1")
FUNCTION = "mvm-demo-durable-orchestrator"
ALIAS = f"{FUNCTION}:live"
IMAGE = "demo-agent"
QUEUE_NAME = "mvm-demo-durable-approvals"
VM_LOG_GROUP = f"/aws/lambda-microvms/{IMAGE}"
ORCH_LOG_GROUP = f"/aws/lambda/{FUNCTION}"
REPO = Path(__file__).resolve().parents[1]
RESULTS = REPO / "results" / "durable"
TERMINAL = {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}
LIVE_STATES = {"PENDING", "RUNNING", "SUSPENDING", "SUSPENDED", "RESUMING"}
MAX_VMS_ON_ACCOUNT = 8          # 8 GB quota / 512 MiB
VM_RE = re.compile(r"microvm-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

SLEEP3 = ["sleep 3"] * 4
SHARD = [{"steps": [f"echo shard {i}", "sleep 1"]} for i in range(40)]

# id -> (description, event, needed VMs at once, approver decision, expected outcome text)
SCENARIOS: dict[str, dict] = {
    "single": {
        "description": "one lease: launch, /run, heartbeats, completion, terminate",
        "event": {"mode": "single", "task": {"steps": ["uname -m", "python3 -c 'print(2+2)'", "sleep 2"]}},
        "vms": 1, "approver": None,
        "expected": "SUCCEEDED, passed: true, 3 steps, VM terminated",
    },
    "parallel": {
        "description": "in-VM parallelism: 4 x sleep 3 in parallel vs the same 4 sequential (two shards)",
        "event": {"mode": "fanout", "shards": [{"steps": SLEEP3, "parallel": True, "max_parallel": 4},
                                               {"steps": SLEEP3}]},
        "vms": 2, "approver": None,
        "expected": "parallel wall time ~3 s vs ~12 s inside the VM (from the result's step durations)",
    },
    "retryable-fail": {
        "description": "typed retryable failure and relaunch",
        "event": {"mode": "single", "task": {"steps": ["echo retryable-fail"], "fail_after_s": 2}},
        "vms": 1, "approver": None,
        "expected": "lease_with_relaunch attempt 0 and 1 both fail, outcome status: failed, retryable: true, "
                    "attempt: 1",
    },
    "hang": {
        "description": "budget timeout while the VM keeps heartbeating",
        "event": {"mode": "single", "task": {"steps": ["echo hang"], "hang_s": 400}, "budget_s": 60},
        "vms": 1, "approver": None,
        "expected": "CallbackTimeoutError -> terminate -> status: timeout",
    },
    "fanout-4": {
        "description": "governed fan-out inside the plan",
        "event": {"mode": "fanout", "shards": SHARD[:4]},
        "vms": 4, "approver": None,
        "expected": "plan: concurrency 4, 1 wave; all 4 done; lease_map output has 4 results",
    },
    "fanout-8": {
        "description": "fan-out in waves",
        "event": {"mode": "fanout", "shards": SHARD[:8]},
        "vms": 4, "approver": None,
        "expected": "plan: concurrency 4, 2 waves; 8 done; RunMicrovm throttling retries visible if they "
                    "happen",
    },
    "fanout-refused": {
        "description": "pre-flight refusal, nothing launched",
        "event": {"mode": "fanout", "shards": SHARD[:40]},
        "vms": 0, "approver": None,
        "expected": "lease_map plan step returns status: rejected, zero RunMicrovm calls",
    },
    "fanout-approved": {
        "description": "approval gate, approved",
        "event": {"mode": "fanout", "shards": SHARD[:12]},
        "vms": 4, "approver": "approve",
        "expected": "approve publishes callback id to SQS; approver calls "
                    "SendDurableExecutionCallbackSuccess; status: done",
    },
    "fanout-denied": {
        "description": "approval gate, denied",
        "event": {"mode": "fanout", "shards": SHARD[:12]},
        "vms": 0, "approver": "deny",
        "expected": "SendDurableExecutionCallbackFailure -> status: denied, nothing launched",
    },
}


# ----------------------------------------------------------------- small helpers
def iso(ts) -> str | None:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        ts = dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def epoch(ts) -> float | None:
    return None if ts is None else (ts if isinstance(ts, (int, float)) else ts.timestamp())


def jsonable(obj):
    return json.loads(json.dumps(obj, default=str))


def parse_json(raw):
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "replace")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return raw
    return raw


def write(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(data, str):
        path.write_text(data)
    else:
        path.write_text(json.dumps(data, indent=2, sort_keys=False, default=str) + "\n")


def log(msg: str) -> None:
    print(f"[{dt.datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class Plane:
    """The few AWS calls run.py makes, on one session (profile from AWS_PROFILE)."""

    def __init__(self):
        self.session = boto3.Session(region_name=REGION)
        self.lam = self.session.client("lambda")
        self.logs = self.session.client("logs")
        self.sqs = self.session.client("sqs")
        from microvm import FleetManager, PlaneConfig

        self.fm = FleetManager(PlaneConfig(region=REGION), quota_aware=False)
        self.api = self.fm.api
        self.queue_url = self.sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]

    # -- the durable function -----------------------------------------------------------
    def start(self, name: str, event: dict) -> str:
        resp = self.lam.invoke(FunctionName=ALIAS, InvocationType="Event", DurableExecutionName=name,
                               Payload=json.dumps(event).encode())
        arn = resp.get("DurableExecutionArn")
        if not arn:  # look the name up on the bare function (the API refuses a name next to a qualifier)
            deadline = time.time() + 30
            while not arn and time.time() < deadline:
                items = self.lam.list_durable_executions_by_function(
                    FunctionName=FUNCTION, DurableExecutionName=name).get("DurableExecutions", [])
                arn = items[0]["DurableExecutionArn"] if items else None
                if not arn:
                    time.sleep(1)
        if not arn:
            raise RuntimeError(f"no durable execution ARN for {name}")
        return arn

    def wait(self, arn: str, timeout_s: float = 1500) -> dict:
        deadline = time.time() + timeout_s
        while True:
            d = self.lam.get_durable_execution(DurableExecutionArn=arn, IncludeExecutionData=True)
            if d["Status"] in TERMINAL:
                return d
            if time.time() > deadline:
                raise TimeoutError(f"{arn} still {d['Status']} after {timeout_s}s")
            time.sleep(3)

    def history(self, arn: str) -> list[dict]:
        events, marker = [], None
        while True:
            kw = {"DurableExecutionArn": arn, "IncludeExecutionData": True, "MaxItems": 100}
            if marker:
                kw["Marker"] = marker
            page = self.lam.get_durable_execution_history(**kw)
            events.extend(page.get("Events", []))
            marker = page.get("NextMarker")
            if not marker:
                return events

    # -- logs ---------------------------------------------------------------------------
    def orchestrator_logs(self, start: float, end: float) -> list[dict]:
        out, token = [], None
        while True:
            kw = {"logGroupName": ORCH_LOG_GROUP, "startTime": int((start - 5) * 1000),
                  "endTime": int((end + 90) * 1000), "limit": 10000}
            if token:
                kw["nextToken"] = token
            try:
                page = self.logs.filter_log_events(**kw)
            except self.logs.exceptions.ResourceNotFoundException:
                return out
            out.extend(page.get("events", []))
            token = page.get("nextToken")
            if not token:
                return sorted(out, key=lambda e: e["timestamp"])

    def vm_streams(self, vm_ids: list[str]) -> dict[str, str]:
        """stream name per VM id: '<YYYY/MM/DD>[<version>]<microvm id>' (build logs live elsewhere)."""
        found, token, pages = {}, None, 0
        while len(found) < len(vm_ids) and pages < 6:
            kw = {"logGroupName": VM_LOG_GROUP, "orderBy": "LastEventTime", "descending": True, "limit": 50}
            if token:
                kw["nextToken"] = token
            try:
                page = self.logs.describe_log_streams(**kw)
            except self.logs.exceptions.ResourceNotFoundException:
                return found
            for s in page.get("logStreams", []):
                for vm in vm_ids:
                    if s["logStreamName"].endswith(vm):
                        found[vm] = s["logStreamName"]
            token = page.get("nextToken")
            pages += 1
            if not token:
                break
        return found

    def vm_logs(self, vm_ids: list[str], attempts: int = 4, pause_s: float = 30) -> tuple[str, list[str]]:
        """Every line of every VM's stream, from the head; waits for streams that are still on their way."""
        if not vm_ids:
            return "(no VM launched)\n", []
        streams: dict[str, str] = {}
        for attempt in range(attempts):
            streams = self.vm_streams(vm_ids)
            if len(streams) == len(vm_ids):
                break
            if attempt < attempts - 1:
                log(f"  vm logs: {len(streams)}/{len(vm_ids)} streams so far, waiting {pause_s:.0f}s")
                time.sleep(pause_s)
        chunks, missing = [], [vm for vm in vm_ids if vm not in streams]
        for vm in vm_ids:
            if vm not in streams:
                chunks.append(f"===== {vm}: no log stream in {VM_LOG_GROUP} yet =====\n")
                continue
            lines, token = [], None
            while True:
                kw = {"logGroupName": VM_LOG_GROUP, "logStreamName": streams[vm], "startFromHead": True,
                      "limit": 10000}
                if token:
                    kw["nextToken"] = token
                page = self.logs.get_log_events(**kw)
                lines.extend(f"{iso(e['timestamp'] / 1000)} {e['message'].rstrip()}"
                             for e in page.get("events", []))
                nxt = page.get("nextForwardToken")
                if not nxt or nxt == token:
                    break
                token = nxt
            chunks.append(f"===== {vm}  ({VM_LOG_GROUP} / {streams[vm]}) =====\n" + "\n".join(lines) + "\n")
        return "\n".join(chunks), missing

    # -- the fleet ----------------------------------------------------------------------
    def vm_record(self, vm_id: str) -> dict | None:
        try:
            rec = self.api.get_microvm(microvmIdentifier=vm_id)
        except self.api.exceptions.ResourceNotFoundException:
            return None
        return jsonable({k: v for k, v in rec.items() if k != "ResponseMetadata"})

    def live_vms(self) -> list:
        return [v for v in self.fm.list(IMAGE) if v.state in LIVE_STATES]

    def wait_for_room(self, needed: int, max_wait_s: float = 900) -> list[str]:
        """Another orchestrator shares the image and the 8 x 512 MiB quota: wait until ours fit."""
        notes, deadline = [], time.time() + max_wait_s
        while needed:
            live = self.live_vms()
            if len(live) + needed <= MAX_VMS_ON_ACCOUNT:
                if live:
                    notes.append(f"{len(live)} demo-agent VM(s) not ours were live at start: "
                                 + ", ".join(v.microvm_id for v in live))
                return notes
            if time.time() > deadline:
                notes.append(f"started with {len(live)} live demo-agent VMs after waiting {max_wait_s:.0f}s "
                             "for room")
                return notes
            log(f"  {len(live)} demo-agent VMs live on the account; waiting for room for {needed}")
            time.sleep(10)
        return notes

    def reap(self, vm_ids: list[str]) -> tuple[list[str], dict]:
        """Terminate anything we launched that is still alive; return notes and the final states."""
        notes, states = [], {}
        for vm in vm_ids:
            rec = self.vm_record(vm)
            if rec is None:
                states[vm] = "GONE"
                continue
            state = rec.get("state")
            if state in LIVE_STATES:
                self.fm.terminate(vm)
                notes.append(f"{vm} was still {state} after the execution ended: terminated by run.py")
                state = "TERMINATING (by run.py)"
            states[vm] = state
        return notes, states


# ----------------------------------------------------------------- history rendering
def _detail(ev: dict) -> str:
    for key, val in ev.items():
        if not key.endswith("Details") or not isinstance(val, dict) or not val:
            continue
        bits = []
        for k, v in val.items():
            if k == "Result" and isinstance(v, dict):
                payload = parse_json(v.get("Payload")) if "Payload" in v else v
                text = json.dumps(payload, default=str)
                bits.append(f"result={text[:160]}{'...' if len(text) > 160 else ''}")
            elif k == "Error" and isinstance(v, dict):
                bits.append(f"error={v.get('ErrorType')}: {str(v.get('ErrorMessage'))[:120]}")
            elif k in ("CallbackId",):
                bits.append(f"callback_id={str(v)[:12]}...")
            elif k in ("StartTimestamp", "EndTimestamp"):
                continue
            else:
                bits.append(f"{k}={v}")
        return " ".join(bits)
    return ""


def render_history(events: list[dict]) -> str:
    if not events:
        return "(no events)\n"
    t0 = epoch(events[0].get("EventTimestamp")) or 0.0
    lines = [f"{'+s':>9}  {'timestamp':<24}  {'event':<28} {'sub':<10} {'name':<44} detail"]
    for ev in events:
        ts = epoch(ev.get("EventTimestamp"))
        delta = f"{ts - t0:+9.3f}" if ts is not None else f"{'?':>9}"
        sub, name = str(ev.get("SubType") or ""), str(ev.get("Name") or "")
        lines.append(f"{delta}  {iso(ts) or '':<24}  {ev.get('EventType', ''):<28} {sub:<10} {name:<44} "
                     f"{_detail(ev)}".rstrip())
    return "\n".join(lines) + "\n"


def count(events: list[dict], event_type: str, name_suffix: str | None = None,
          name_contains: str | None = None) -> int:
    n = 0
    for ev in events:
        if ev.get("EventType") != event_type:
            continue
        name = str(ev.get("Name") or "")
        if name_suffix and not name.endswith(name_suffix):
            continue
        if name_contains and name_contains not in name:
            continue
        n += 1
    return n


def history_line(events: list[dict], outcome) -> str:
    """One sentence of what the history shows, from counts of the library's named operations."""
    parts = []
    if count(events, "StepSucceeded", name_suffix="-plan"):
        parts.append("plan step")
    approvals = count(events, "CallbackSucceeded", name_contains="approval")
    denials = count(events, "CallbackFailed", name_contains="approval")
    if approvals:
        parts.append("approval callback succeeded")
    if denials:
        parts.append("approval callback failed (denied)")
    launches = count(events, "StepSucceeded", name_suffix="-launch")
    if launches:
        parts.append(f"{launches} launch step{'s' if launches != 1 else ''}")
    ok = count(events, "CallbackSucceeded") - approvals
    failed = count(events, "CallbackFailed") - denials
    timed_out = count(events, "CallbackTimedOut")
    for n, label in ((ok, "lease callback succeeded"), (failed, "lease callback failed"),
                     (timed_out, "lease callback timed out")):
        if n:
            parts.append(f"{n} {label}")
    terminates = count(events, "StepSucceeded", name_suffix="-terminate")
    if terminates:
        parts.append(f"{terminates} terminate step{'s' if terminates != 1 else ''}")
    end = next((ev["EventType"] for ev in reversed(events)
                if ev.get("EventType", "").startswith("Execution")), "?")
    status = outcome.get("status") if isinstance(outcome, dict) else None
    parts.append(f"{end}" + (f" with status {status}" if status else ""))
    return "; ".join(parts)


# ----------------------------------------------------------------- expectations
def check(scenario: str, status: str, out, events: list[dict], vm_states: dict, orch_lines: list[str],
          runmicrovm_calls: int) -> tuple[bool, list[str]]:
    """matches_expected and the reasons, computed from what was captured (never assumed)."""
    notes: list[str] = []
    ok = status == "SUCCEEDED"  # the handler returns an outcome dict in every scenario; FAILED means a crash
    if not ok:
        notes.append(f"durable execution ended {status}")
    if not isinstance(out, dict):
        return False, notes + ["no result object in the execution"]
    st = out.get("status")
    launches = count(events, "StepSucceeded", name_suffix="-launch")
    all_terminated = all(s.startswith("TERMINAT") or s == "GONE" for s in vm_states.values())

    def need(cond: bool, what: str):
        nonlocal ok
        if not cond:
            ok = False
            notes.append(f"expected {what}")

    if scenario == "single":
        result = out.get("result") or {}
        need(st == "done", f"status done (got {st})")
        need(result.get("passed") is True, "result.passed true")
        need(len(result.get("steps") or []) == 3, f"3 steps (got {len(result.get('steps') or [])})")
        need(out.get("attempt") == 0, "attempt 0")
        need(all_terminated and vm_states, f"VM terminated (states {vm_states})")
    elif scenario == "parallel":
        need(st == "done" and out.get("succeeded") == 2,
             f"both shards done (got {st}, {out.get('succeeded')})")
        for o in out.get("outcomes") or []:
            r = o.get("result") or {}
            durations = [s.get("duration_s", 0) for s in r.get("steps") or []]
            if r.get("parallel"):
                wall = max(durations or [0])
                notes.append(f"parallel shard: 4 steps, in-VM wall ~{wall:.2f} s (max step), "
                             f"sum {sum(durations):.2f} s")
                need(wall < 6, f"parallel wall ~3 s (got {wall:.2f})")
            else:
                wall = sum(durations)
                notes.append(f"sequential shard: 4 steps, in-VM wall {wall:.2f} s (sum of steps)")
                need(wall >= 11, f"sequential wall ~12 s (got {wall:.2f})")
        need(all_terminated, f"VMs terminated (states {vm_states})")
    elif scenario == "retryable-fail":
        err = out.get("error") or {}
        need(st == "failed", f"status failed (got {st})")
        need(out.get("retryable") is True, "retryable true")
        need(out.get("attempt") == 1, f"attempt 1 (got {out.get('attempt')})")
        need(err.get("error_type") == "Injected", f"error_type Injected (got {err.get('error_type')})")
        need(launches == 2, f"2 launch steps (got {launches})")
        need(count(events, "CallbackFailed") == 2, "2 failed lease callbacks")
        need(all_terminated, f"VMs terminated (states {vm_states})")
    elif scenario == "hang":
        err = out.get("error") or {}
        need(st == "timed_out", f"status timed_out (got {st})")
        notes.append("the library reports the SPEC's `status: timeout` as `status: timed_out` "
                     "(lease_microvm's outcome)")
        need(err.get("error_type") == "CallbackTimeout",
             f"error_type CallbackTimeout (got {err.get('error_type')})")
        need(out.get("attempt") == 1, f"attempt 1 after one relaunch (got {out.get('attempt')})")
        need(count(events, "CallbackTimedOut") == 2, "2 lease callbacks timed out")
        budgets = [ev["CallbackStartedDetails"].get("Timeout") for ev in events
                   if ev.get("EventType") == "CallbackStarted" and ev.get("CallbackStartedDetails")]
        need(budgets and all(b == 60 for b in budgets), f"callback timeout 60 s (got {budgets})")
        need(count(events, "StepSucceeded", name_suffix="-terminate") == launches,
             "a terminate step per launch")
        need(all_terminated, f"VMs terminated (states {vm_states})")
    elif scenario in ("fanout-4", "fanout-8"):
        n = 4 if scenario == "fanout-4" else 8
        plan = out.get("plan") or {}
        need(st == "done" and out.get("succeeded") == n,
             f"{n} shards done (got {st}, {out.get('succeeded')})")
        need(plan.get("concurrency") == 4, f"plan concurrency 4 (got {plan.get('concurrency')})")
        need(plan.get("waves") == n // 4, f"plan waves {n // 4} (got {plan.get('waves')})")
        need(len(out.get("outcomes") or []) == n, f"{n} outcomes")
        need(all_terminated, f"VMs terminated (states {vm_states})")
        throttles = sum(1 for line in orch_lines if "RunMicrovm error" in line)
        notes.append(f"RunMicrovm throttling retries seen in the orchestrator log: {throttles}")
    elif scenario == "fanout-refused":
        need(st == "rejected", f"status rejected (got {st})")
        need("max_vm_seconds" in str(out.get("reason")),
             f"rejection by max_vm_seconds (got {out.get('reason')})")
        need(launches == 0 and runmicrovm_calls == 0, f"zero RunMicrovm calls (got {runmicrovm_calls})")
    elif scenario == "fanout-approved":
        plan = out.get("plan") or {}
        need(plan.get("needs_approval") is True, "plan needs_approval true")
        need(count(events, "CallbackSucceeded", name_contains="approval") == 1, "approval callback succeeded")
        need(st == "done" and out.get("succeeded") == 12,
             f"12 shards done (got {st}, {out.get('succeeded')})")
        need(plan.get("waves") == 3 and plan.get("concurrency") == 4, "plan 4 at a time in 3 waves")
        need(all_terminated, f"VMs terminated (states {vm_states})")
    elif scenario == "fanout-denied":
        plan = out.get("plan") or {}
        need(plan.get("needs_approval") is True, "plan needs_approval true")
        need(count(events, "CallbackFailed", name_contains="approval") == 1, "approval callback failed")
        need(st == "denied", f"status denied (got {st})")
        need(launches == 0 and runmicrovm_calls == 0,
             f"nothing launched (got {runmicrovm_calls} RunMicrovm calls)")
    return ok, notes


# ----------------------------------------------------------------- one scenario
def run_scenario(plane: Plane, sid: str, spec: dict) -> dict:
    out_dir = RESULTS / sid
    out_dir.mkdir(parents=True, exist_ok=True)
    notes: list[str] = []
    event = spec["event"]
    write(out_dir / "input.json", event)
    notes += plane.wait_for_room(spec["vms"])

    name = f"mvm-demo-durable-{sid}-{dt.datetime.now(dt.timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    log(f"{sid}: invoking {ALIAS} as {name}")
    arn = plane.start(name, event)
    decision = None
    if spec["approver"]:
        log(f"{sid}: approver.py waiting for the approval request ({spec['approver']})")
        handled = approver.decide(plane.queue_url, spec["approver"], execution=name, wait_s=300,
                                  session=plane.session)
        if handled:
            decision = handled[0]
            notes.append(f"approver.py {decision['decision']}d callback {decision['callback_id'][:12]}... "
                         f"for execution {decision['execution']} ({decision['plan_summary']})")
        else:
            notes.append("approver.py saw no approval request within 300 s")
    d = plane.wait(arn)
    start, stop = epoch(d.get("StartTimestamp")), epoch(d.get("EndTimestamp"))
    result = parse_json(d.get("Result"))
    log(f"{sid}: {d['Status']} in {stop - start:.1f}s (service clock)")

    events = plane.history(arn)
    write(out_dir / "history.json", jsonable(events))
    write(out_dir / "history.txt", render_history(events))
    output = {"execution_arn": arn, "execution_name": name, "status": d["Status"],
              "start": iso(start), "stop": iso(stop), "seconds_by_service": round(stop - start, 3),
              "result": result, "error": jsonable(d.get("Error")) if d.get("Error") else None}
    write(out_dir / "output.json", output)

    time.sleep(20)  # the orchestrator's last lines and the VM's first log batch trail the execution
    # every invocation of this execution has a RequestId in the history; other executions (the benchmark
    # shares the function) are dropped by it
    request_ids = {ev["InvocationCompletedDetails"]["RequestId"] for ev in events
                   if ev.get("EventType") == "InvocationCompleted" and ev.get("InvocationCompletedDetails")}
    orch = plane.orchestrator_logs(start, stop)
    orch_lines = [f"{iso(e['timestamp'] / 1000)} {e['message'].rstrip()}" for e in orch
                  if any(rid in e["message"] for rid in request_ids) or arn in e["message"]]
    write(out_dir / "orchestrator-logs.txt",
          f"# {ORCH_LOG_GROUP} between {iso(start)} and {iso(stop)} (+90 s), lines carrying one of this "
          f"execution's {len(request_ids)} request ids or its ARN\n" + "\n".join(orch_lines) + "\n")

    blob = json.dumps(events, default=str) + json.dumps(result, default=str) + "\n".join(orch_lines)
    vm_ids = sorted(set(VM_RE.findall(blob)))
    runmicrovm_calls = sum(1 for line in orch_lines if "RunMicrovm call" in line)
    launches = count(events, "StepStarted", name_suffix="-launch")
    if runmicrovm_calls != launches:
        notes.append(f"RunMicrovm calls in the orchestrator log: {runmicrovm_calls}; "
                     f"launch steps started: {launches}")

    reap_notes, vm_states = plane.reap(vm_ids)
    notes += reap_notes
    vm_text, missing = plane.vm_logs(vm_ids)
    write(out_dir / "vm-logs.txt", vm_text)
    if missing:
        notes.append(f"no VM log stream arrived within ~2 min for: {', '.join(missing)}")
    vm_records = {vm: plane.vm_record(vm) for vm in vm_ids}
    vm_seconds = 0.0
    for rec in vm_records.values():
        if rec and rec.get("startedAt"):
            t1 = rec.get("terminatedAt") or rec.get("updatedAt") or rec.get("lastModified")
            s0 = dt.datetime.fromisoformat(str(rec["startedAt"]).replace("Z", "+00:00")).timestamp()
            if t1:
                vm_seconds += dt.datetime.fromisoformat(str(t1).replace("Z", "+00:00")).timestamp() - s0

    ok, check_notes = check(sid, d["Status"], result, events, vm_states, orch_lines, runmicrovm_calls)
    summary = {
        "scenario": sid, "orchestrator": "durable", "status": d["Status"], "expected": spec["expected"],
        "matches_expected": ok, "start": iso(start), "stop": iso(stop),
        "seconds_by_service": round(stop - start, 3), "vm_ids": vm_ids, "runmicrovm_calls": runmicrovm_calls,
        "notes": notes + check_notes,
        "execution_arn": arn, "execution_name": name,
        "outcome_status": result.get("status") if isinstance(result, dict) else None,
        "history_events": len(events), "history": history_line(events, result),
        "vm_states_after": vm_states, "vm_seconds": round(vm_seconds, 1) if vm_seconds else None,
        "vm_records": vm_records, "approval": decision, "description": spec["description"],
    }
    write(out_dir / "summary.json", summary)
    log(f"{sid}: matches_expected={ok} vms={len(vm_ids)} runmicrovm_calls={runmicrovm_calls}")
    for n in summary["notes"]:
        log(f"  note: {n}")
    return summary


# ----------------------------------------------------------------- the report
def report() -> str:
    rows, deviations = [], []
    for sid in SCENARIOS:
        p = RESULTS / sid / "summary.json"
        if not p.exists():
            rows.append(f"| [`{sid}`]({sid}/) | not run | | | | | |")
            continue
        s = json.loads(p.read_text())
        rows.append(f"| [`{sid}`]({sid}/) | {s['status']} / `{s.get('outcome_status')}` | "
                    f"{s['seconds_by_service']:.1f} | {len(s['vm_ids'])} | {s['runmicrovm_calls']} | "
                    f"{'yes' if s['matches_expected'] else 'no'} | {s.get('history', '')} |")
        for n in s.get("notes", []):
            if not n.startswith("parallel shard") and not n.startswith("sequential shard"):
                deviations.append(f"- `{sid}`: {n}")
    text = [
        "# Durable functions results", "",
        "Every row is one live execution of `mvm-demo-durable-orchestrator:live` (stack `mvm-demo-durable`, "
        "account 643603452951, us-east-1) driven by [`durable/run.py`](../../durable/run.py). Seconds are "
        "the service's own `StartTimestamp` to `EndTimestamp` from `get_durable_execution`; VMs and "
        "RunMicrovm calls are counted from the execution history and the orchestrator's CloudWatch log; "
        "`matches expected` is computed by `run.py` from the captured output and history against the SPEC "
        "row. Each directory holds `input.json`, `output.json`, `history.json`, `history.txt`, "
        "`vm-logs.txt`, `orchestrator-logs.txt`, and `summary.json`.", "",
        "| scenario | execution status / outcome | seconds (service) | VMs | RunMicrovm calls | "
        "matches expected | what the history shows |",
        "|---|---|---|---|---|---|---|",
        *rows, "",
        "## Notes and deviations", "",
        "Every line below is copied from the `notes` of that scenario's `summary.json`.", "",
        *(deviations or ["- none"]), "",
        "## Fixed during the run (first attempts kept as `first-attempt-*` files)", "",
        "- `hang`, first attempt ([history](hang/first-attempt-history.txt), "
        "[VM logs](hang/first-attempt-vm-logs.txt)): both callbacks ended with `CallbackTimedOut` at +30 s, "
        "not at the 60 s budget. The library's default VM heartbeat interval is 30 s, equal to the policy's "
        "30 s heartbeat timeout, and the first heartbeat lands after boot (~5 s) plus the interval, so the "
        "heartbeat timeout fired first (the VM logs show no heartbeat before the callback closed). The "
        "orchestrator now passes `heartbeat_s=10` to every lease; the final run reaches the 60 s budget.",
        "- `fanout-approved`, first attempt ([history](fanout-approved/first-attempt-history.txt), "
        "[summary](fanout-approved/first-attempt-summary.json)): the approval request was published, but "
        "its `execution` field carried the execution *id* (the library's `execution_name` returns the last "
        "ARN segment) while `approver.py` filters by the execution *name*; nothing answered and "
        "`lease_map` returned `status: denied` after the 600 s approval timeout with nothing launched. The "
        "orchestrator now publishes the name segment of the ARN (`execution_label`).", "",
    ]
    return "\n".join(text)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--only", help="comma-separated scenario ids (default: all, in SPEC order)")
    ap.add_argument("--report", action="store_true", help="only rebuild results/durable/README.md")
    args = ap.parse_args(argv)
    if args.report:
        write(RESULTS / "README.md", report())
        print(RESULTS / "README.md")
        return 0
    ids = [s.strip() for s in args.only.split(",")] if args.only else list(SCENARIOS)
    unknown = [s for s in ids if s not in SCENARIOS]
    if unknown:
        ap.error(f"unknown scenario(s): {unknown}; known: {list(SCENARIOS)}")
    plane = Plane()
    failures = 0
    for sid in ids:
        try:
            summary = run_scenario(plane, sid, SCENARIOS[sid])
            failures += 0 if summary["matches_expected"] else 1
        except Exception as exc:  # keep going; the summary of a broken run is the traceback
            failures += 1
            log(f"{sid}: run.py failed: {exc!r}")
            write(RESULTS / sid / "summary.json", {
                "scenario": sid, "orchestrator": "durable", "status": "RUN_ERROR",
                "expected": SCENARIOS[sid]["expected"], "matches_expected": False, "start": None,
                "stop": None, "seconds_by_service": None, "vm_ids": [], "runmicrovm_calls": 0,
                "notes": [f"run.py error: {exc!r}"]})
    live = plane.live_vms()
    if live:
        log("WARNING: demo-agent VMs still live on the account (not necessarily ours): "
            + ", ".join(f"{v.microvm_id}={v.state}" for v in live))
    write(RESULTS / "README.md", report())
    log(f"wrote {RESULTS / 'README.md'}; {failures} scenario(s) did not match")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
