"""Drive every Step Functions scenario in SPEC.md against the live stacks and write the results.

    AWS_PROFILE=heisenberg python3 run.py                 # every sfn scenario, one at a time
    AWS_PROFILE=heisenberg python3 run.py single hang     # a subset
    python3 run.py --report                               # only regenerate results/stepfunctions/README.md

For each scenario this starts one execution of the right machine (mvm-demo-sfn-lease,
mvm-demo-sfn-lease-short for `hang`, mvm-demo-sfn-map-lease for the fan-outs), waits for it to
end, and writes results/stepfunctions/<scenario>/: input.json, history.json (every page of
get_execution_history), history.txt (event, state, timestamp, +seconds from ExecutionStarted),
output.json, vm-logs.txt (the VM's lines from /aws/lambda-microvms/demo-agent), orchestrator-logs.txt
(the machine's vended CloudWatch log lines for the execution), summary.json, and for the fan-outs
plan.txt (what `mvm lease plan` printed and its exit code). `fanout-refused` never starts an
execution: the plan exits 2 and run.py stops there. The approval scenarios run an approver that
reads the SQS queue, takes the task token from the SNS message RequestApproval published, and
calls SendTaskSuccess or SendTaskFailure. Task tokens are redacted from every saved file.

After every scenario the fleet is listed; any demo-agent VM still active is terminated and noted,
so nothing this script launched outlives it. Timings come from describe_execution's startDate and
stopDate, never from the poll loop. microvm-ctl 0.3.0 from PyPI: run this from outside a source tree.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
import microvm
from microvm.client import microvm_client
from microvm.fleet import ACTIVE_STATES

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
RESULTS = REPO / "results" / "stepfunctions"
PROFILE = os.environ.get("AWS_PROFILE", "heisenberg")
REGION = os.environ.get("MVM_REGION", "us-east-1")
IMAGE = "demo-agent"
STACK = "mvm-demo-sfn"
STACK_MAP = "mvm-demo-sfn-map"
VM_LOG_GROUP = f"/aws/lambda-microvms/{IMAGE}"
POLICY_FLAGS = ["--budget", "120", "--heartbeat", "30", "--slack", "60", "--max-concurrency", "4",
                "--max-vm-seconds", "3000", "--approval-usd", "0.015"]
MEMORY_QUOTA_VMS = 8  # 8 GB quota / 512 MiB
VM_ID = re.compile(r"microvm-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
TOKEN = re.compile(r'((?:\\+)?"(?:token|task_token|taskToken|TaskToken)(?:\\+)?"\s*:\s*(?:\\+)?")([^"\\]+)')
RUN = "runMicrovm"
POLL_S = 3

SHARD_TASK = {"steps": ["echo shard $SHARD on $(uname -m)", "sleep 3"], "env": {"SHARD": "{i}"}}
FOUR_SLEEPS = ["sleep 3"] * 4


def shards(n: int) -> dict:
    out = []
    for i in range(n):
        out.append(json.loads(json.dumps(SHARD_TASK).replace("{i}", str(i))))
    return {"shards": out}


# scenario id -> machine, input, the SPEC's expected outcome for sfn, approver decision, plan shards
SCENARIOS = {
    "single": {
        "machine": "lease",
        "input": {"steps": ["uname -m", "python3 -c 'print(2+2)'", "sleep 2"]},
        "expected": "SUCCEEDED, `passed: true`, 3 steps, VM terminated",
    },
    "parallel": {
        "machine": "lease",
        "input": {"steps": FOUR_SLEEPS, "parallel": True, "max_parallel": 4},
        "comparison": {"steps": FOUR_SLEEPS},
        "expected": "parallel wall time ~3 s vs ~12 s inside the VM (from the result's step durations)",
    },
    "retryable-fail": {
        "machine": "lease",
        "input": {"steps": ["echo before the injected failure"], "fail_after_s": 2},
        "expected": "sfn (microvm-ctl 0.3.1): Lease -> OnLeaseError -> TerminateFailed (the VM the cause "
                    "names is terminated at once) -> Reap -> TerminateStale -> Failed with the typed error "
                    "in the output",
    },
    "hang": {
        "machine": "short", "leave_vm": True,
        "input": {"steps": ["echo one step, then hang"], "hang_s": 400},
        "expected": "sfn: States.Timeout at 60 s -> OnLeaseError -> Reap (nothing stale yet) -> Failed; the "
                    "timed-out task carries no VM id, so the VM runs until its MaximumDurationInSeconds "
                    "(budget + slack = 120 s) ends it, which GetMicrovm's startedAt/terminatedAt must show",
    },
    "fanout-4": {
        "machine": "map", "shards": 4, "input": shards(4),
        "expected": "plan: concurrency 4, 1 wave; all 4 done; Map/lease_map output has 4 results",
    },
    "fanout-8": {
        "machine": "map", "shards": 8, "input": shards(8),
        "expected": "plan: concurrency 4, 2 waves; 8 done; RunMicrovm throttling retries visible if they "
                    "happen",
    },
    "fanout-refused": {
        "machine": "map", "shards": 40, "input": shards(40),
        "expected": "sfn: `mvm lease plan --shards 40` exits 2 and run.py refuses to start the execution "
                    "(record the plan)",
    },
    "fanout-approved": {
        "machine": "map", "shards": 12, "input": shards(12), "approver": "approve",
        "expected": "sfn: RequestApproval publishes to SNS with the task token; approver reads SQS and calls "
                    "SendTaskSuccess; Fanout runs",
    },
    "fanout-denied": {
        "machine": "map", "shards": 12, "input": shards(12), "approver": "deny",
        "expected": "sfn: SendTaskFailure -> Denied state, nothing launched",
    },
}


# ----------------------------------------------------------------------------- helpers
def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def iso(dt) -> str | None:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds") if dt else None


def redact(text: str) -> str:
    return TOKEN.sub(r"\1<redacted>", text)


def dump(path: Path, obj) -> None:
    path.write_text(redact(json.dumps(obj, indent=2, default=str)) + "\n")


def log(msg: str) -> None:
    print(f"{datetime.now().strftime('%H:%M:%S')}  {msg}", flush=True)


class Aws:
    def __init__(self) -> None:
        self.session = boto3.Session(profile_name=PROFILE, region_name=REGION)
        self.sfn = self.session.client("stepfunctions")
        self.sqs = self.session.client("sqs")
        self.logs = self.session.client("logs")
        self.cfn = self.session.client("cloudformation")
        self.vms = microvm_client(REGION, PROFILE)
        self.outputs = {STACK: self._outputs(STACK), STACK_MAP: self._outputs(STACK_MAP)}
        self.machines = {
            "lease": self.outputs[STACK]["StateMachineArn"],
            "short": self.outputs[STACK]["ShortStateMachineArn"],
            "map": self.outputs[STACK_MAP]["StateMachineArn"],
        }
        self.queue_url = self.outputs[STACK_MAP]["ApprovalQueueUrl"]
        group, map_group = self.outputs[STACK]["LogGroupName"], self.outputs[STACK_MAP]["LogGroupName"]
        self.log_groups = {"lease": group, "short": group, "map": map_group}

    def _outputs(self, stack: str) -> dict:
        st = self.cfn.describe_stacks(StackName=stack)["Stacks"][0]
        return {o["OutputKey"]: o["OutputValue"] for o in st.get("Outputs", [])}

    # ---- fleet
    def active_vms(self) -> list[dict]:
        out = self.vms.get_paginator("list_microvms").paginate().build_full_result()
        return [v for v in out.get("items", []) if v["state"] in ACTIVE_STATES]

    def vm(self, vm_id: str) -> dict:
        try:
            return self.vms.get_microvm(microvmIdentifier=vm_id)
        except Exception as e:  # noqa: BLE001 - recorded, not hidden
            return {"microvmId": vm_id, "state": f"lookup failed: {type(e).__name__}"}

    def terminate(self, vm_id: str) -> None:
        self.vms.terminate_microvm(microvmIdentifier=vm_id)


# ----------------------------------------------------------------------------- history
def fetch_history(aws: Aws, arn: str) -> list[dict]:
    events: list[dict] = []
    paginator = aws.sfn.get_paginator("get_execution_history")
    for page in paginator.paginate(executionArn=arn, includeExecutionData=True):
        events.extend(page["events"])
    return sorted(events, key=lambda e: e["id"])


def _details(ev: dict) -> dict:
    for k, v in ev.items():
        if k.endswith("EventDetails") and isinstance(v, dict):
            return v
    return {}


def _state_index(events: list[dict]) -> dict[int, dict]:
    return {e["id"]: e for e in events}


def state_of(ev: dict, by_id: dict[int, dict]) -> tuple[str, str, int | None]:
    """(state name, entered-event id, map index) by walking previousEventId back to the state entry."""
    name, entered, index = "", None, None
    cur = ev
    for _ in range(10000):
        t = cur["type"]
        if t.endswith("StateEntered") and not name:
            name, entered = cur["stateEnteredEventDetails"]["name"], cur["id"]
        if t == "MapIterationStarted":
            index = cur["mapIterationStartedEventDetails"].get("index")
            break
        if t == "ExecutionStarted" or not cur.get("previousEventId"):
            break
        cur = by_id.get(cur["previousEventId"])
        if cur is None:
            break
    return name, entered, index


def history_text(events: list[dict], meta: dict) -> str:
    by_id = _state_index(events)
    start = events[0]["timestamp"] if events else None
    lines = [f"execution  {meta.get('executionArn')}", f"status     {meta.get('status')}",
             f"start      {iso(meta.get('startDate'))}", f"stop       {iso(meta.get('stopDate'))}",
             f"seconds    {meta.get('seconds_by_service')}", "",
             f"{'+seconds':>9}  {'timestamp':<29} {'event':<30} state"]
    for ev in events:
        name, _, index = state_of(ev, by_id)
        d = _details(ev)
        extra = ""
        if ev["type"] in ("TaskScheduled", "TaskSubmitted", "TaskStarted", "TaskSucceeded", "TaskFailed",
                          "TaskTimedOut", "TaskSubmitFailed", "TaskStartFailed"):
            extra = f"  {d.get('resourceType', '')}:{d.get('resource', '')}".rstrip(":")
            if d.get("error"):
                extra += f"  error={d['error']}"
        elif ev["type"] == "ExecutionFailed":
            extra = f"  error={d.get('error')}"
        elif ev["type"] == "MapStateStarted":
            extra = f"  length={d.get('length')}"
        elif ev["type"] == "ChoiceStateExited":
            extra = ""
        shown = f"{name}[{index}]" if index is not None and name else name
        delta = (ev["timestamp"] - start).total_seconds() if start else 0.0
        lines.append(f"{delta:+9.3f}  {iso(ev['timestamp']):<29} {ev['type']:<30} {shown}{extra}")
    return "\n".join(lines) + "\n"


def _json_or_none(text):
    try:
        return json.loads(text) if text else None
    except (TypeError, ValueError):
        return None


def analyse(events: list[dict]) -> dict:
    """VM ids, RunMicrovm calls, the states entered in order, task outcomes, in-flight peak."""
    by_id = _state_index(events)
    vm_ids: list[str] = []
    run_calls = 0
    throttles = 0
    entered: list[str] = []
    task_events: dict[int, dict] = {}
    approval = {}
    terminated_by_machine: list[str] = []

    def add(vm):
        if vm and vm not in vm_ids:
            vm_ids.append(vm)

    for ev in events:
        t, d = ev["type"], _details(ev)
        name, entered_id, index = state_of(ev, by_id)
        if t.endswith("StateEntered"):
            entered.append(f"{name}[{index}]" if index is not None else name)
        is_run = d.get("resource", "").startswith(RUN)
        if t == "TaskScheduled" and is_run:
            run_calls += 1
        if t == "TaskFailed" and is_run and d.get("error") in ("LambdaMicrovms.ThrottlingException",
                                                                "LambdaMicrovms.ServiceQuotaExceededException"):
            throttles += 1
        if t == "TaskSubmitted" and is_run:
            out = _json_or_none(d.get("output")) or {}
            add(out.get("MicrovmId") or out.get("microvmId"))
        if t in ("TaskSucceeded", "TaskFailed") and is_run:
            payload = _json_or_none(d.get("output") if t == "TaskSucceeded" else d.get("cause")) or {}
            add(payload.get("microvm_id"))
        if t == "TaskScheduled" and d.get("resource") == "terminateMicrovm":
            params = _json_or_none(d.get("parameters")) or {}
            vm = params.get("MicrovmIdentifier")
            if vm:
                terminated_by_machine.append(f"{name}:{vm}")
                add(vm)
        if t in ("TaskScheduled", "TaskSubmitted", "TaskSucceeded", "TaskFailed", "TaskTimedOut") and is_run:
            slot = task_events.setdefault(entered_id, {"state": name, "index": index})
            slot.setdefault(t, ev["timestamp"])
            slot["last"] = t
            if d.get("error"):
                slot["error"] = d["error"]
        terminal = ("TaskSubmitted", "TaskSucceeded", "TaskFailed", "TaskTimedOut")
        if name == "RequestApproval" and t in terminal:
            approval[t] = iso(ev["timestamp"])
            if d.get("error"):
                approval["error"] = d["error"]
    # peak of runMicrovm tasks in flight (TaskSubmitted .. terminal event)
    intervals = []
    for slot in task_events.values():
        s = slot.get("TaskSubmitted") or slot.get("TaskScheduled")
        e = slot.get("TaskSucceeded") or slot.get("TaskFailed") or slot.get("TaskTimedOut")
        if s and e:
            intervals.append((s, e))
    peak, live = 0, 0
    for _ts, kind in sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals],
                            key=lambda x: (x[0], x[1])):
        live += kind
        peak = max(peak, live)
    for slot in task_events.values():
        for k in list(slot):
            if isinstance(slot[k], datetime):
                slot[k] = iso(slot[k])
    return {"vm_ids": vm_ids, "runmicrovm_calls": run_calls, "throttling_retries": throttles,
            "states_entered": entered, "lease_tasks": list(task_events.values()), "peak_in_flight": peak,
            "approval": approval, "terminated_by_machine": terminated_by_machine}


# ----------------------------------------------------------------------------- logs
def vm_logs(aws: Aws, vm_ids: list[str], start_ms: int,
            attempts: int = 6) -> tuple[str, list[str], list[dict]]:
    """The VM's own lines: log streams named after the VM id, plus any message that mentions it.
    Returns the text, notes, and the parsed JSON records (with the CloudWatch timestamp as `ts_ms`)."""
    notes: list[str] = []
    if not vm_ids:
        return "no VM ids in this execution's history, so nothing to filter the log group by\n", notes, []
    for attempt in range(attempts):
        streams = []
        paginator = aws.logs.get_paginator("describe_log_streams")
        for page in paginator.paginate(logGroupName=VM_LOG_GROUP, orderBy="LastEventTime", descending=True,
                                       PaginationConfig={"MaxItems": 400}):
            for s in page["logStreams"]:
                if any(v in s["logStreamName"] for v in vm_ids):
                    streams.append(s["logStreamName"])
        events: dict[tuple, dict] = {}
        for i in range(0, len(streams), 100):
            for page in aws.logs.get_paginator("filter_log_events").paginate(
                    logGroupName=VM_LOG_GROUP, logStreamNames=streams[i:i + 100],
                    startTime=start_ms - 120_000):
                for e in page["events"]:
                    events[(e["timestamp"], e["logStreamName"], e["message"])] = e
        for vm in vm_ids:
            for page in aws.logs.get_paginator("filter_log_events").paginate(
                    logGroupName=VM_LOG_GROUP, filterPattern=f'"{vm}"', startTime=start_ms - 120_000):
                for e in page["events"]:
                    events[(e["timestamp"], e["logStreamName"], e["message"])] = e
        if events or attempt == attempts - 1:
            break
        log(f"  no VM log events yet for {len(vm_ids)} VM(s); waiting 15 s for delivery "
            f"({attempt + 1}/{attempts})")
        time.sleep(15)
    header = [f"log group {VM_LOG_GROUP}", f"VM ids    {', '.join(vm_ids)}",
              f"streams   {', '.join(streams) if streams else '(none named after these VMs)'}", ""]
    if not events:
        msg = (f"no log events for {', '.join(vm_ids)} in {VM_LOG_GROUP} as of {now_iso()} (checked stream "
               f"names and message text, {attempts} attempts over {(attempts - 1) * 15} s)")
        notes.append(msg)
        return "\n".join(header + [msg]) + "\n", notes, []
    lines = header
    records = []
    for key in sorted(events):
        e = events[key]
        ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")
        lines.append(f"{ts}  {e['logStreamName']}  {e['message'].rstrip()}")
        rec = _json_or_none(e["message"])
        if isinstance(rec, dict):
            rec["ts_ms"] = e["timestamp"]
            records.append(rec)
    return redact("\n".join(lines)) + "\n", notes, records


def orchestrator_logs(aws: Aws, group: str, execution_arn: str, start_ms: int, attempts: int = 4) -> str:
    events = []
    for attempt in range(attempts):
        events = []
        try:
            for page in aws.logs.get_paginator("filter_log_events").paginate(
                    logGroupName=group, filterPattern=f'"{execution_arn}"', startTime=start_ms - 60_000):
                events.extend(page["events"])
        except aws.logs.exceptions.ResourceNotFoundException:
            return f"log group {group} does not exist\n"
        if events or attempt == attempts - 1:
            break
        log(f"  no orchestrator log events yet; waiting 10 s ({attempt + 1}/{attempts})")
        time.sleep(10)
    lines = [f"log group {group}", f"filter    \"{execution_arn}\"", ""]
    if not events:
        lines.append(f"no log events for this execution as of {now_iso()}")
    for e in sorted(events, key=lambda e: e["timestamp"]):
        ts = datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).isoformat(timespec="milliseconds")
        lines.append(f"{ts}  {e['message'].rstrip()}")
    return redact("\n".join(lines)) + "\n"


# ----------------------------------------------------------------------------- plan
def lease_plan(n_shards: int, cwd: Path) -> tuple[str, int, dict | None]:
    """Run `mvm lease plan` twice (table for people, --json for the summary); return text, exit code, plan."""
    env = dict(os.environ, AWS_PROFILE=PROFILE, MVM_REGION=REGION, COLUMNS="120", TERM="dumb", NO_COLOR="1")
    base = [sys.executable, "-m", "microvm.cli", "lease", "plan", "--image", IMAGE, "--shards", str(n_shards),
            *POLICY_FLAGS]
    table = subprocess.run(base, cwd=cwd, env=env, capture_output=True, text=True, check=False)
    as_json = subprocess.run(base + ["--json"], cwd=cwd, env=env, capture_output=True, text=True, check=False)
    plan = _json_or_none(as_json.stdout)
    text = (f"$ mvm lease plan --image {IMAGE} --shards {n_shards} {' '.join(POLICY_FLAGS)}\n"
            f"{table.stdout}{table.stderr}exit code {table.returncode}"
            "  (0 = can launch, 2 = rejected, 3 = needs approval)\n")
    return text, table.returncode, plan


# ----------------------------------------------------------------------------- approver
def approve_or_deny(aws: Aws, execution_name: str, decision: str, deadline_s: int = 180) -> dict:
    """Read the SQS queue until the SNS message for this execution arrives, then complete its task token."""
    started = time.time()
    record = {"decision": decision, "queue": aws.queue_url, "stale_messages_deleted": 0}
    while time.time() - started < deadline_s:
        resp = aws.sqs.receive_message(QueueUrl=aws.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10)
        for m in resp.get("Messages", []):
            body = _json_or_none(m["Body"])
            if isinstance(body, dict) and "Message" in body and "TopicArn" in body:  # not raw after all
                body = _json_or_none(body["Message"])
            aws.sqs.delete_message(QueueUrl=aws.queue_url, ReceiptHandle=m["ReceiptHandle"])
            if not isinstance(body, dict) or body.get("execution") != execution_name:
                record["stale_messages_deleted"] += 1
                continue
            token = body.get("task_token")
            record["message"] = {k: v for k, v in body.items() if k != "task_token"}
            record["received_at"] = now_iso()
            if decision == "approve":
                aws.sfn.send_task_success(taskToken=token, output=json.dumps(
                    {"approved": True, "approver": "stepfunctions/run.py", "at": record["received_at"]}))
                record["call"] = "SendTaskSuccess"
            else:
                aws.sfn.send_task_failure(taskToken=token, error="ApprovalDenied",
                                          cause="denied by stepfunctions/run.py approver")
                record["call"] = "SendTaskFailure"
            record["completed_at"] = now_iso()
            return record
    record["error"] = f"no approval message for {execution_name} within {deadline_s} s"
    return record


# ----------------------------------------------------------------------------- one execution
def run_execution(aws: Aws, machine: str, name: str, payload: dict, approver: str | None = None) -> dict:
    arn = aws.machines[machine]
    started = aws.sfn.start_execution(stateMachineArn=arn, name=name, input=json.dumps(payload))
    exec_arn = started["executionArn"]
    log(f"  started {exec_arn}")
    approval = None
    if approver:
        approval = approve_or_deny(aws, name, approver)
        log(f"  approver: {approval.get('call') or approval.get('error')}")
    while True:
        time.sleep(POLL_S)
        desc = aws.sfn.describe_execution(executionArn=exec_arn)
        if desc["status"] != "RUNNING":
            break
    elapsed = (desc["stopDate"] - desc["startDate"]).total_seconds()
    log(f"  {desc['status']} after {elapsed:.2f} s (service clock)")
    events = fetch_history(aws, exec_arn)
    meta = {"executionArn": exec_arn, "status": desc["status"], "startDate": desc["startDate"],
            "stopDate": desc.get("stopDate"), "error": desc.get("error"), "cause": desc.get("cause"),
            "seconds_by_service": round((desc["stopDate"] - desc["startDate"]).total_seconds(), 3)}
    return {"meta": meta, "events": events, "output": _json_or_none(desc.get("output")), "approval": approval,
            "stateMachineArn": arn}


def write_execution(d: Path, run: dict, aws: Aws, machine: str) -> tuple[dict, list[str]]:
    """Write history.json, history.txt, output.json, vm-logs.txt, orchestrator-logs.txt; return analysis."""
    meta, events = run["meta"], run["events"]
    notes: list[str] = []
    dump(d / "history.json", events)
    (d / "history.txt").write_text(redact(history_text(events, meta)))
    output = run["output"]
    if output is None and meta.get("error"):
        output = {"error": meta["error"], "cause": _json_or_none(meta.get("cause")) or meta.get("cause")}
        run["output"] = output
    dump(d / "output.json", output)
    a = analyse(events)
    start_ms = int(meta["startDate"].timestamp() * 1000)
    text, vm_notes, records = vm_logs(aws, a["vm_ids"], start_ms)
    (d / "vm-logs.txt").write_text(text)
    notes.extend(vm_notes)
    a["vm_log_records"] = records
    a["in_vm_wall_s"] = in_vm_wall(records)
    (d / "orchestrator-logs.txt").write_text(
        orchestrator_logs(aws, aws.log_groups[machine], meta["executionArn"], start_ms))
    return a, notes


# ----------------------------------------------------------------------------- fleet hygiene
def wait_for_room(aws: Aws, needed: int) -> list[str]:
    notes = []
    for _ in range(40):
        active = aws.active_vms()
        if len(active) + needed <= MEMORY_QUOTA_VMS:
            return notes
        others = [v["microvmId"] for v in active]
        log(f"  {len(active)} VM(s) active, need room for {needed}; waiting 15 s: {others}")
        notes.append(f"waited for room: {len(active)} active VMs before start")
        time.sleep(15)
    raise SystemExit("the fleet never had room; refusing to exceed 8 x 512 MiB")


def straggler_check(aws: Aws, scenario: str, started_at: datetime, vm_ids: list[str],
                    leave: bool = False) -> tuple[dict, list[str]]:
    """List every VM; terminate demo-agent VMs this run left active; leave anything else alone.
    With `leave`, this scenario's VMs are watched until the platform ends them (the proof that
    MaximumDurationInSeconds bounds a timed-out lease) instead of being terminated here."""
    notes: list[str] = []
    check = {"at": now_iso(), "active_before": [], "terminated": [], "left_alone": [], "watched": []}
    for v in aws.active_vms():
        row = {"microvmId": v["microvmId"], "state": v["state"], "image": v["imageArn"].rsplit(":", 1)[-1],
               "startedAt": iso(v.get("startedAt"))}
        check["active_before"].append(row)
        mine = v["imageArn"].endswith(f":{IMAGE}") and (
            v["microvmId"] in vm_ids or (v.get("startedAt") and v["startedAt"] >= started_at))
        if mine and leave:
            check["watched"].append(v["microvmId"])
            notes.append(f"{v['microvmId']} was still {v['state']} after the execution ended; run.py did "
                         "not terminate it and polled GetMicrovm until the platform did")
        elif mine:
            aws.terminate(v["microvmId"])
            check["terminated"].append(v["microvmId"])
            notes.append(f"straggler: {v['microvmId']} was still {v['state']} after the execution ended; "
                         "run.py terminated it")
        else:
            check["left_alone"].append(v["microvmId"])
            notes.append(f"{v['microvmId']} ({row['image']}) is active but was not launched by this "
                         "scenario; left alone")
    if check["terminated"]:
        deadline = time.time() + 90
        while time.time() < deadline:
            still = [v["microvmId"] for v in aws.active_vms() if v["microvmId"] in check["terminated"]]
            if not still:
                break
            time.sleep(5)
        check["all_terminated_by"] = now_iso()
    if check["watched"]:
        deadline = time.time() + 300
        while time.time() < deadline:
            still = [v["microvmId"] for v in aws.active_vms() if v["microvmId"] in check["watched"]]
            if not still:
                break
            log(f"  waiting for the platform to end {still} ...")
            time.sleep(10)
        else:
            for vm in check["watched"]:
                aws.terminate(vm)
                check["terminated"].append(vm)
                notes.append(f"deviation: {vm} was still active 300 s after the execution ended, well past "
                             "MaximumDurationInSeconds; run.py terminated it")
        check["watched_until"] = now_iso()
    log(f"  fleet after {scenario}: {len(check['active_before'])} active, "
        f"{len(check['terminated'])} terminated by run.py, {len(check['watched'])} watched, "
        f"{len(check['left_alone'])} left alone")
    return check, notes


def final_vm_states(aws: Aws, vm_ids: list[str], polls: int = 12) -> dict[str, dict]:
    out = {}
    for vm in vm_ids:
        for _ in range(polls):
            v = aws.vm(vm)
            if v.get("state") not in ACTIVE_STATES:
                break
            time.sleep(5)
        out[vm] = {k: (iso(v[k]) if isinstance(v.get(k), datetime) else v.get(k))
                   for k in ("state", "startedAt", "terminatedAt", "imageVersion") if k in v}
    return out


def in_vm_wall(records: list[dict]) -> float | None:
    """Seconds from the VM's `lease accepted` line to its `passed ...` line (CloudWatch timestamps, ms)."""
    accepted = [r["ts_ms"] for r in records if r.get("msg") == "lease accepted"]
    passed = [r["ts_ms"] for r in records if str(r.get("msg", "")).startswith("passed ")]
    if accepted and passed:
        return round((min(passed) - min(accepted)) / 1000, 3)
    return None


# ----------------------------------------------------------------------------- checks
def _passed(payload) -> bool:
    return bool(isinstance(payload, dict) and isinstance(payload.get("result"), dict)
                and payload["result"].get("passed"))


def check_scenario(sid: str, s: dict, run: dict, a: dict, vm_states: dict,
                   extra: dict) -> tuple[dict, list[str]]:
    """Named booleans the SPEC's expected column implies; matches_expected is their conjunction."""
    meta, out = run["meta"], run["output"]
    entered = a["states_entered"]
    checks: dict[str, bool] = {}
    notes: list[str] = []
    terminated = all(v.get("state") == "TERMINATED" for v in vm_states.values()) and bool(vm_states)
    by_machine = {t.split(":", 1)[1] for t in a["terminated_by_machine"]}
    terminated_by_machine = terminated and all(vm in by_machine for vm in a["vm_ids"])
    if sid == "single":
        checks["succeeded"] = meta["status"] == "SUCCEEDED"
        checks["passed_true"] = _passed(out)
        checks["three_steps"] = _passed(out) and len(out["result"].get("steps", [])) == 3
        checks["vm_terminated_by_machine"] = terminated_by_machine
        checks["lease_terminate_done"] = entered[:3] == ["Lease", "Terminate", "Done"]
    elif sid == "parallel":
        comp, ca = extra["comparison"], extra["comparison_analysis"]
        p_dur = [r["duration_s"] for r in out["result"]["steps"]] if _passed(out) else []
        c_out = comp["output"]
        s_dur = [r["duration_s"] for r in c_out["result"]["steps"]] if _passed(c_out) else []
        p_wall, s_wall = a.get("in_vm_wall_s"), ca.get("in_vm_wall_s")
        extra["comparison_summary"] = {
            "parallel_wall_s_in_vm_from_logs": p_wall, "sequential_wall_s_in_vm_from_logs": s_wall,
            "parallel_step_durations_s": p_dur, "sequential_step_durations_s": s_dur,
            "parallel_wall_from_step_durations_s": round(max(p_dur), 3) if p_dur else None,
            "sequential_wall_from_step_durations_s": round(sum(s_dur), 3) if s_dur else None,
            "parallel_payload_elapsed_s": out.get("elapsed_s") if isinstance(out, dict) else None,
            "sequential_payload_elapsed_s": (comp["output"].get("elapsed_s")
                                             if isinstance(comp["output"], dict) else None),
            "parallel_seconds_by_service": meta["seconds_by_service"],
            "sequential_seconds_by_service": comp["meta"]["seconds_by_service"]}
        notes.append("in-VM wall time is `lease accepted` -> `passed 4 step(s)` in vm-logs.txt; the "
                     "payload's elapsed_s also counts the completer's boto3 import before `lease accepted` "
                     "and is several seconds larger")
        checks["both_succeeded"] = meta["status"] == "SUCCEEDED" and comp["meta"]["status"] == "SUCCEEDED"
        checks["parallel_wall_about_3s"] = p_wall is not None and p_wall < 6
        checks["sequential_wall_about_12s"] = s_wall is not None and 12 <= s_wall < 18
        checks["vms_terminated_by_machine"] = terminated_by_machine
    elif sid == "retryable-fail":
        cause = out.get("cause") if isinstance(out, dict) else None
        # the Fail state's Cause is the caught error {Error, Cause}; the inner Cause is the VM's payload
        inner = _json_or_none(cause.get("Cause")) if isinstance(cause, dict) else None
        err = inner.get("error", {}) if isinstance(inner, dict) else {}
        checks["failed"] = meta["status"] == "FAILED"
        checks["path_onleaseerror_terminatefailed_reap_failed"] = entered == [
            "Lease", "OnLeaseError", "TerminateFailed", "Reap", "TerminateStale", "Failed"]
        checks["error_lease_failed"] = meta.get("error") == "LeaseFailed"
        checks["typed_error_in_output"] = (isinstance(cause, dict) and cause.get("Error") == "Injected"
                                           and err.get("error_type") == "Injected"
                                           and err.get("retryable") is True)
        checks["terminate_failed_named_the_vm"] = any(
            t.startswith("TerminateFailed:") and t.split(":", 1)[1] in a["vm_ids"]
            for t in a["terminated_by_machine"])
        failed_at = next((t.get("TaskFailed") for t in a["lease_tasks"] if t["state"] == "Lease"), None)
        gaps = {}
        for vm, st in vm_states.items():
            if failed_at and st.get("terminatedAt"):
                gap = datetime.fromisoformat(st["terminatedAt"]) - datetime.fromisoformat(failed_at)
                gaps[vm] = round(gap.total_seconds(), 3)
        extra["observations"] = {"lease_task_failed_at": failed_at,
                                 "seconds_from_task_failed_to_vm_terminated_at": gaps}
        checks["vm_terminated_within_10s_of_failure"] = (bool(gaps)
                                                          and all(-1 <= g <= 10 for g in gaps.values()))
        if not terminated_by_machine:
            notes.append("deviation: the failed lease's VM was not terminated by the machine; run.py's "
                         "straggler check terminated it (SPEC: terminate everything you launch).")
    elif sid == "hang":
        lease_tasks = [t for t in a["lease_tasks"] if t["state"] == "Lease"]
        checks["failed"] = meta["status"] == "FAILED"
        checks["states_timeout"] = any(t.get("error") == "States.Timeout" for t in lease_tasks)
        timed_out_at = next((t.get("TaskTimedOut") for t in lease_tasks if t.get("TaskTimedOut")), None)
        budget_gap = None
        if timed_out_at:
            budget_gap = (datetime.fromisoformat(timed_out_at) - meta["startDate"]).total_seconds()
        checks["timeout_at_60s"] = budget_gap is not None and 59 <= budget_gap <= 65
        checks["path_onleaseerror_reap_failed"] = entered == [
            "Lease", "OnLeaseError", "Reap", "TerminateStale", "Failed"]
        checks["nothing_terminated_by_machine"] = not a["terminated_by_machine"]
        lifetimes = {vm: st.get("lifetime_s") for vm, st in vm_states.items()}
        by_platform = {vm: st.get("terminated_by", "").startswith("platform") for vm, st in vm_states.items()}
        extra["observations"] = {"lease_task_timed_out_at": timed_out_at,
                                 "seconds_from_start_to_timeout": (round(budget_gap, 3)
                                                                   if budget_gap else None),
                                 "vm_lifetime_s": lifetimes, "vm_ended_by_platform": by_platform,
                                 "maximum_duration_in_seconds": 120}
        checks["vm_ended_by_platform_cap_about_120s"] = bool(lifetimes) and all(
            by_platform[vm] and lt is not None and 115 <= lt <= 135 for vm, lt in lifetimes.items())
        for vm, lt in lifetimes.items():
            notes.append(f"{vm}: startedAt {vm_states[vm].get('startedAt')}, terminatedAt "
                         f"{vm_states[vm].get('terminatedAt')}, lifetime {lt} s against "
                         "MaximumDurationInSeconds 120 (budget 60 + slack 60); terminated by "
                         f"{vm_states[vm].get('terminated_by')}")
    elif sid in ("fanout-4", "fanout-8"):
        n = s["shards"]
        plan = extra.get("plan") or {}
        results = out if isinstance(out, list) else []
        checks["succeeded"] = meta["status"] == "SUCCEEDED"
        checks[f"{n}_results"] = len(results) == n and all(_passed(r) for r in results)
        checks["plan_concurrency_4"] = plan.get("concurrency") == 4
        checks[f"plan_waves_{1 if n == 4 else 2}"] = plan.get("waves") == (1 if n == 4 else 2)
        checks["peak_in_flight_le_4"] = 0 < a["peak_in_flight"] <= 4
        checks["vms_terminated_by_machine"] = terminated_by_machine
        checks["no_approval_gate"] = "RequestApproval" not in entered
        if a["throttling_retries"]:
            notes.append(f"RunMicrovm throttling: {a['throttling_retries']} retried launch(es), "
                         f"{a['runmicrovm_calls']} RunMicrovm calls for {n} shards")
    elif sid == "fanout-approved":
        results = out if isinstance(out, list) else []
        appr = extra.get("approval") or {}
        checks["request_approval_published"] = "TaskSubmitted" in a["approval"]
        checks["approver_send_task_success"] = appr.get("call") == "SendTaskSuccess"
        checks["fanout_ran"] = "Fanout" in entered
        checks["succeeded_12_results"] = meta["status"] == "SUCCEEDED" and len(results) == 12
        checks["vms_terminated_by_machine"] = terminated_by_machine
    elif sid == "fanout-denied":
        appr = extra.get("approval") or {}
        checks["approver_send_task_failure"] = appr.get("call") == "SendTaskFailure"
        checks["denied_state"] = "Denied" in entered
        checks["failed_approval_denied"] = (meta["status"] == "FAILED"
                                            and meta.get("error") == "ApprovalDenied")
        checks["nothing_launched"] = a["runmicrovm_calls"] == 0 and not a["vm_ids"]
    return checks, notes


# ----------------------------------------------------------------------------- scenario driver
def run_scenario(aws: Aws, sid: str) -> dict:
    s = SCENARIOS[sid]
    d = RESULTS / sid
    d.mkdir(parents=True, exist_ok=True)
    log(f"== {sid}: {s['expected']}")
    started_at = datetime.now(timezone.utc)
    stamp = started_at.strftime("%Y%m%d-%H%M%S")
    notes: list[str] = []
    summary = {"scenario": sid, "orchestrator": "stepfunctions", "status": None, "expected": s["expected"],
               "matches_expected": False, "start": None, "stop": None, "seconds_by_service": None,
               "vm_ids": [], "runmicrovm_calls": 0, "notes": notes,
               "microvm_ctl": {"version": microvm.__version__, "path": microvm.__file__}}
    extra: dict = {}

    if s["machine"] == "map":
        plan_text, code, plan = lease_plan(s["shards"], d)
        (d / "plan.txt").write_text(plan_text)
        extra["plan"] = plan
        extra["plan_exit_code"] = code
        log(f"  mvm lease plan --shards {s['shards']}: exit {code}"
            + (f", {plan['summary']}" if plan else ""))
        if sid == "fanout-refused":
            dump(d / "input.json", {"would_have_started": s["input"], "started": False})
            dump(d / "output.json", plan)
            dump(d / "history.json", [])
            (d / "history.txt").write_text("no execution: run.py refused to start it because "
                                           f"`mvm lease plan --shards {s['shards']}` exited {code}\n")
            (d / "vm-logs.txt").write_text("no execution, no VM, nothing launched\n")
            (d / "orchestrator-logs.txt").write_text("no execution was started\n")
            refused = code == 2
            summary.update(status="REFUSED" if refused else "NOT_REFUSED", start=started_at.isoformat(),
                           stop=now_iso(), seconds_by_service=0.0,
                           matches_expected=refused and bool(plan and plan.get("rejected")),
                           plan=plan, plan_exit_code=code,
                           checks={"plan_exit_2": code == 2,
                                   "plan_rejected_reason": bool(plan and plan.get("rejected")),
                                   "no_execution_started": True})
            if refused:
                notes.append(f"refused before start: {plan['rejected'] if plan else 'exit 2'}")
            else:
                notes.append(f"deviation: the plan exited {code}, not 2; no execution was started anyway")
            check, n2 = straggler_check(aws, sid, started_at, [])
            notes.extend(n2)
            summary["fleet_check"] = check
            dump(d / "summary.json", summary)
            return summary
        if code == 3:
            notes.append("plan exit 3 (needs approval): the machine's Gate handles approval, so run.py "
                         "started it")
        elif code != 0:
            notes.append(f"deviation: plan exit {code} for {s['shards']} shards; the execution was not "
                         "started")
            summary.update(status="NOT_STARTED", start=started_at.isoformat(), stop=now_iso())
            dump(d / "summary.json", summary)
            return summary

    notes.extend(wait_for_room(aws, 4 if s["machine"] == "map" else 1))
    run = run_execution(aws, s["machine"], f"{sid}-{stamp}", s["input"], s.get("approver"))
    if run["approval"]:
        extra["approval"] = run["approval"]
    inputs = {"stateMachineArn": run["stateMachineArn"], "execution": run["meta"]["executionArn"],
              "input": s["input"]}
    a, n2 = write_execution(d, run, aws, s["machine"])
    notes.extend(n2)

    if sid == "parallel":
        log("  comparison run: the same 4 steps sequentially")
        comp = run_execution(aws, s["machine"], f"{sid}-sequential-{stamp}", s["comparison"])
        cd = d / "sequential"
        cd.mkdir(exist_ok=True)
        dump(cd / "input.json", {"stateMachineArn": comp["stateMachineArn"],
                                 "execution": comp["meta"]["executionArn"], "input": s["comparison"]})
        ca, n3 = write_execution(cd, comp, aws, s["machine"])
        notes.extend(f"sequential/: {n}" for n in n3)
        extra["comparison"] = comp
        extra["comparison_analysis"] = ca
        inputs["comparison"] = {"execution": comp["meta"]["executionArn"], "input": s["comparison"],
                                "files": "sequential/"}
        a["vm_ids"] = a["vm_ids"] + [v for v in ca["vm_ids"] if v not in a["vm_ids"]]
        a["runmicrovm_calls"] += ca["runmicrovm_calls"]
        a["terminated_by_machine"] += ca["terminated_by_machine"]
        notes.append(f"two executions: {run['meta']['executionArn']} (parallel, top-level files) and "
                     f"{comp['meta']['executionArn']} (sequential, sequential/)")
    dump(d / "input.json", inputs)

    fleet, n5 = straggler_check(aws, sid, started_at, a["vm_ids"], leave=bool(s.get("leave_vm")))
    for vm in fleet["terminated"]:
        if vm not in a["vm_ids"]:
            a["vm_ids"].append(vm)
    vm_states = final_vm_states(aws, a["vm_ids"], polls=60 if s.get("leave_vm") else 12)
    for vm, st in vm_states.items():
        if st.get("startedAt") and st.get("terminatedAt"):
            life = (datetime.fromisoformat(st["terminatedAt"]) - datetime.fromisoformat(st["startedAt"]))
            st["lifetime_s"] = round(life.total_seconds(), 3)
            by = ("machine" if any(t.endswith(f":{vm}") for t in a["terminated_by_machine"])
                  else "run.py straggler check" if vm in fleet["terminated"]
                  else "platform (MaximumDurationInSeconds cap or idle policy)")
            st["terminated_by"] = by
            if by.startswith("platform"):
                notes.append(f"{vm} was ended by the platform {st['lifetime_s']} s after start "
                             "(neither the machine nor run.py terminated it)")
    checks, n4 = check_scenario(sid, s, run, a, vm_states, extra)
    notes.extend(n4)
    notes.extend(n5)
    a.pop("vm_log_records", None)
    meta = run["meta"]
    summary.update(
        status=meta["status"], start=iso(meta["startDate"]), stop=iso(meta.get("stopDate")),
        seconds_by_service=meta["seconds_by_service"], vm_ids=a["vm_ids"],
        runmicrovm_calls=a["runmicrovm_calls"],
        matches_expected=bool(checks) and all(checks.values()), checks=checks,
        execution_arn=meta["executionArn"], state_machine_arn=run["stateMachineArn"], error=meta.get("error"),
        states_entered=a["states_entered"], lease_tasks=a["lease_tasks"], peak_in_flight=a["peak_in_flight"],
        throttling_retries=a["throttling_retries"], terminated_by_machine=a["terminated_by_machine"],
        in_vm_wall_s=a.get("in_vm_wall_s"), vm_states=vm_states, fleet_check=fleet,
    )
    for k in ("plan", "plan_exit_code", "approval", "comparison_summary", "observations"):
        if k in extra:
            summary[k] = extra[k]
    if "comparison" in extra:
        cm = extra["comparison"]["meta"]
        summary["comparison_execution"] = {"execution_arn": cm["executionArn"], "status": cm["status"],
                                           "seconds_by_service": cm["seconds_by_service"]}
    dump(d / "summary.json", summary)
    log(f"  matches_expected={summary['matches_expected']} checks={checks}")
    return summary


# ----------------------------------------------------------------------------- report
def _history_line(s: dict) -> str:
    if s["status"] == "REFUSED":
        plan = s.get("plan") or {}
        return (f"no execution; `mvm lease plan --shards 40` exited {s.get('plan_exit_code')}: "
                f"{plan.get('rejected')}")
    states = s.get("states_entered", [])
    top = []
    for st in states:
        base = st.split("[")[0]
        if "[" in st and base in ("Lease", "Terminate", "ShardDone", "Reap", "TerminateStale", "Failed",
                                  "TerminateOne", "AlreadyGone"):
            continue
        if not top or top[-1] != base:
            top.append(base)
    shard_states = sorted({st.split("[")[0] for st in states if "[" in st})
    line = " -> ".join(top)
    if shard_states:
        line += f" (per shard: {', '.join(shard_states)})"
    if s.get("error"):
        line += f"; error {s['error']}"
    if s.get("peak_in_flight"):
        line += f"; peak {s['peak_in_flight']} in flight"
    if s.get("throttling_retries"):
        line += f"; {s['throttling_retries']} throttled launch(es) retried"
    if s.get("approval"):
        line += f"; approver called {s['approval'].get('call')}"
    cs = s.get("comparison_summary")
    if cs:
        line += (f"; in-VM wall {cs['parallel_wall_s_in_vm_from_logs']} s parallel vs "
                 f"{cs['sequential_wall_s_in_vm_from_logs']} s sequential")
    return line


FIRST_PASS = "first-pass-0.3.0"


def _table(rows: list[dict], link_prefix: str = "") -> list[str]:
    lines = ["| scenario | microvm-ctl | status | seconds (service) | VMs | RunMicrovm calls | "
             "matches expected | what the history shows |", "|---|---|---|---|---|---|---|---|"]
    for s in rows:
        sid = s["scenario"]
        link = f"{sid}/{link_prefix}"
        lines.append(f"| [{sid}]({link}) | {s['microvm_ctl']['version']} | {s['status']} | "
                     f"{s['seconds_by_service']} | {len(s['vm_ids'])} | {s['runmicrovm_calls']} | "
                     f"{'yes' if s['matches_expected'] else 'NO'} | {_history_line(s)} |")
    return lines


def _deviations(rows: list[dict], link_prefix: str = "") -> list[str]:
    lines = []
    for s in rows:
        failed = [k for k, v in (s.get("checks") or {}).items() if not v]
        dev_notes = [n for n in s["notes"] if n.startswith("deviation") or n.startswith("straggler")]
        if failed or dev_notes or not s["matches_expected"]:
            sid = s["scenario"]
            lines.append(f"- **{sid}** (expected: {s['expected']})")
            for k in failed:
                lines.append(f"  - check `{k}` is false ([summary.json]({sid}/{link_prefix}summary.json))")
            for n in dev_notes:
                lines.append(f"  - {n}")
    return lines or ["None: every scenario matched its expected column."]


def write_report() -> Path:
    rows, first = [], []
    for sid in SCENARIOS:
        p = RESULTS / sid / "summary.json"
        if p.exists():
            rows.append(json.loads(p.read_text()))
        fp = RESULTS / sid / FIRST_PASS / "summary.json"
        if fp.exists():
            first.append(json.loads(fp.read_text()))
    lines = ["# Step Functions results", "",
             "Every row is one live execution in account 643603452951 (us-east-1) driven by "
             "`stepfunctions/run.py`; the linked directory holds the input, the full history, the output, "
             "the VM's log lines, the machine's log lines, and `summary.json`. Seconds come from "
             "`describe_execution` startDate/stopDate. RunMicrovm calls are the `TaskScheduled` events for "
             "`runMicrovm` in the history (retries count). `matches expected` is the conjunction of the "
             "named checks in each `summary.json`. The `microvm-ctl` column is the library version the "
             "machine was generated from and run.py drove it with.", ""]
    if first:
        rerun = {s["scenario"] for s in first}
        lines += ["## First pass on microvm-ctl 0.3.0", "",
                  "The whole matrix ran once on 0.3.0. Two scenarios deviated from SPEC.md; their files are "
                  f"kept under `<scenario>/{FIRST_PASS}/` and the deviations are quoted verbatim from those "
                  "summaries:", ""]
        lines += _deviations(first, f"{FIRST_PASS}/")
        lines += ["", "First-pass rows for the scenarios that were rerun:", ""]
        lines += _table(first, f"{FIRST_PASS}/")
        lines += ["", "## Second pass on microvm-ctl 0.3.1", "",
                  "0.3.1 adds `OnLeaseError` (Choice) and `TerminateFailed` between the Catch and `Reap`: "
                  "a typed failure's cause names the VM and it is terminated at once; a timeout still "
                  "carries no id and that VM is bounded by `MaximumDurationInSeconds`. The stacks were "
                  "regenerated and redeployed and these scenarios were rerun; the other rows below are the "
                  "first-pass results, which 0.3.1 does not change:", ""]
        lines += _table([s for s in rows if s["scenario"] in rerun])
        lines += ["", "## All scenarios (current files)", ""]
    lines += _table(rows)
    lines += ["", "## Deviations from SPEC.md (current files)", ""]
    lines += _deviations(rows)
    lines += ["", "## Notes", ""]
    for s in rows:
        other = [n for n in s["notes"] if not (n.startswith("deviation") or n.startswith("straggler"))]
        for n in other:
            lines.append(f"- {s['scenario']}: {n}")
    if rows:
        versions = sorted({s["microvm_ctl"]["version"] for s in rows + first})
        lines += ["", f"microvm-ctl {', '.join(versions)} from `{rows[0]['microvm_ctl']['path']}`; report "
                  f"generated {now_iso()} by `python3 stepfunctions/run.py --report`."]
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / "README.md"
    out.write_text("\n".join(lines) + "\n")
    return out


# ----------------------------------------------------------------------------- main
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="*", help=f"subset of {', '.join(SCENARIOS)} (default: all)")
    ap.add_argument("--report", action="store_true", help="only regenerate results/stepfunctions/README.md")
    args = ap.parse_args()
    if args.report:
        print(f"wrote {write_report()}")
        return
    if "site-packages" not in microvm.__file__:
        raise SystemExit(f"microvm is imported from {microvm.__file__}; run from outside the source tree so "
                         "the PyPI 0.3.0 package is used")
    unknown = [s for s in args.scenarios if s not in SCENARIOS]
    if unknown:
        raise SystemExit(f"unknown scenario(s): {unknown}")
    aws = Aws()
    results = []
    for sid in args.scenarios or list(SCENARIOS):
        results.append(run_scenario(aws, sid))
    print(f"wrote {write_report()}")
    for r in results:
        print(f"{r['scenario']:<16} {r['status']:<10} {r['seconds_by_service']!s:>8} s  "
              f"VMs {len(r['vm_ids'])}  RunMicrovm {r['runmicrovm_calls']}  "
              f"matches_expected={r['matches_expected']}")


if __name__ == "__main__":
    main()
