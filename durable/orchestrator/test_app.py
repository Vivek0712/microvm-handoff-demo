"""The demo orchestrator end to end on the AWS durable testing SDK, with a faked FleetManager.

    .../dvenv/bin/pytest -q durable/orchestrator/test_app.py

Covers the event routing (single -> lease_with_relaunch with the policy and the budget
override; fanout -> lease_map; anything else -> bad_request), the rejected plan (40 shards
on the SPEC policy exceed max_vm_seconds 3000: nothing launched, nothing published), and
the approval gate both ways (12 shards need approval at $0.015: denied launches nothing,
approved runs 12 leases at most 4 at a time). The plan arithmetic is the real one
(`microvm.lease.plan_fanout`) on a fake 8 GB quota with the SPEC policy (budget 120 s, slack 60 s,
max_vm_seconds 3000, approval_usd 0.015); no AWS account is touched.
"""

from __future__ import annotations

import json
import os
import sys

import pytest

os.environ.update({
    "MVM_IMAGE": "demo-agent", "MVM_REGION": "us-east-1", "BASELINE_MIB": "512",
    "MVM_EXECUTION_ROLE_ARN": "arn:aws:iam::123456789012:role/agent",
    "APPROVALS_QUEUE_URL": "https://sqs.us-east-1.amazonaws.com/123456789012/mvm-demo-durable-approvals",
    "MVM_LEASE_BUDGET_S": "120", "MVM_LEASE_HEARTBEAT_TIMEOUT_S": "30", "MVM_LEASE_SLACK_S": "60",
    "MVM_LEASE_MAX_CONCURRENCY": "4", "MVM_LEASE_MAX_VM_SECONDS": "3000", "MVM_LEASE_APPROVAL_USD": "0.015",
})
sys.path.insert(0, os.path.dirname(__file__))

import app  # noqa: E402
from aws_durable_execution_sdk_python.execution import ErrorObject  # noqa: E402
from aws_durable_execution_sdk_python_testing import DurableFunctionTestRunner  # noqa: E402

TASK = {"steps": ["uname -m", "python3 -c 'print(2+2)'", "sleep 2"]}
APPROVAL_CB = "shard-approval create callback id"  # the SDK's name for wait_for_callback's inner callback


class _Vm:
    def __init__(self, i):
        self.microvm_id, self.endpoint = f"microvm-fake-{i}", f"fake{i}.lambda-microvm.us-east-1.on.aws"


class _Exceptions:
    class ResourceNotFoundException(Exception):
        pass


class FakeFleetManager:
    api = type("Api", (), {"exceptions": _Exceptions})()
    quota_gb, launch_rate = 8.0, 0.8

    def __init__(self):
        self.leases, self.terminated, self.plans = [], [], []

    def plan(self, shards, baseline_mib, policy=None):
        from microvm.lease import LeasePolicy, plan_fanout
        self.plans.append({"shards": shards, "baseline_mib": baseline_mib, "policy": policy})
        return plan_fanout(shards, baseline_mib, policy or LeasePolicy(),
                           memory_quota_gb=self.quota_gb, launch_rate=self.launch_rate)

    def lease(self, image, lease, task, policy=None, *, version=None, execution_role=None,
              ingress=None, egress=None):
        lease.validate()
        self.leases.append({"image": image, "lease": lease, "task": task, "policy": policy})
        return _Vm(len(self.leases))

    def terminate(self, microvm_id):
        self.terminated.append(microvm_id)


@pytest.fixture()
def fm(monkeypatch):
    fake = FakeFleetManager()
    monkeypatch.setattr(app, "_fm", fake)
    published: list[dict] = []
    monkeypatch.setattr(app, "publish_approval", published.append)
    fake.published = published
    return fake


def _outcome(res):
    return json.loads(res.result) if isinstance(res.result, str) else res.result


def _succeed(runner, cb, i):
    completion = {"microvm_id": f"microvm-fake-{i}", "lease_id": "x", "elapsed_s": 1.0,
                  "result": {"passed": True, "steps": [], "shard": i}}
    runner.send_callback_success(cb, json.dumps(completion).encode())


def test_single_routes_to_lease_with_relaunch(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"mode": "single", "task": TASK})
        cb = runner.wait_for_callback(arn, name="lease-0-callback", timeout=30)
        launch = fm.leases[0]
        assert launch["image"] == "demo-agent" and launch["task"] == TASK
        assert launch["lease"].kind == "durable" and launch["lease"].token == cb
        assert launch["lease"].heartbeat_s == 10  # the library's clamp: a third of the 30 s heartbeat timeout
        # the launch step rebuilds the policy from its time fields; the ceilings live in the plan
        pol = launch["policy"]
        assert (pol.budget_s, pol.heartbeat_timeout_s, pol.slack_s) == (120, 30, 60)
        _succeed(runner, cb, 1)
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "done" and out["attempt"] == 0 and out["result"]["passed"] is True
    assert out["vm"]["microvm_id"] == "microvm-fake-1" and fm.terminated == ["microvm-fake-1"]


def test_single_budget_override_and_retryable_relaunch(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"mode": "single", "task": {"fail_after_s": 2}, "budget_s": 3})
        cb0 = runner.wait_for_callback(arn, name="lease-0-callback", timeout=30)
        assert fm.leases[0]["policy"].budget_s == 3 and fm.leases[0]["policy"].max_duration() == 63
        injected = {"error": {"error_type": "Injected", "message": "failed on purpose", "retryable": True,
                              "data": {}}}
        err = ErrorObject(message="failed on purpose", type="Injected", data=json.dumps(injected),
                          stack_trace=None)
        runner.send_callback_failure(cb0, err)
        cb1 = runner.wait_for_callback(arn, name="lease-1-callback", timeout=30)
        runner.send_callback_failure(cb1, err)
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "failed" and out["retryable"] is True and out["attempt"] == 1
    assert out["error"]["error_type"] == "Injected" and len(fm.leases) == 2
    assert fm.terminated == ["microvm-fake-1", "microvm-fake-2"]


def test_execution_label_is_the_name_not_the_id():
    class Ctx:
        class execution_context:
            durable_execution_arn = ("arn:aws:lambda:us-east-1:123456789012:function:mvm-demo-durable-orch:1"
                                     "/durable-execution/mvm-demo-durable-fanout-approved-20260921-010756/34caf69c")

    assert app.execution_label(Ctx()) == "mvm-demo-durable-fanout-approved-20260921-010756"


def test_unknown_mode_and_bad_shapes_are_bad_requests(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        assert _outcome(runner.run({"mode": "nope"}, timeout=30))["status"] == "bad_request"
        assert _outcome(runner.run({"mode": "single"}, timeout=30))["status"] == "bad_request"
        assert _outcome(runner.run({"mode": "fanout", "shards": []}, timeout=30))["status"] == "bad_request"
    assert fm.leases == [] and fm.plans == []


def test_fanout_rejected_plan_launches_nothing(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        out = _outcome(runner.run({"mode": "fanout", "shards": [TASK] * 40}, timeout=30))
    assert out["status"] == "rejected"
    assert "worst case 7200 VM-seconds exceeds policy max_vm_seconds 3000" in out["reason"]
    assert fm.plans == [{"shards": 40, "baseline_mib": 512, "policy": app.POLICY}]
    assert fm.leases == [] and fm.terminated == [] and fm.published == []


def test_fanout_below_threshold_runs_without_approval(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"mode": "fanout", "shards": [TASK] * 4})
        cbs = [runner.wait_for_callback(arn, name=f"shard-{i}-0-callback", timeout=30) for i in range(4)]
        for i, cb in enumerate(cbs):
            _succeed(runner, cb, i + 1)
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "done" and out["succeeded"] == 4 and out["failed"] == 0
    assert out["plan"]["concurrency"] == 4 and out["plan"]["waves"] == 1
    assert out["plan"]["needs_approval"] is False
    assert fm.published == [] and len(set(cbs)) == 4


def test_fanout_denied_launches_nothing(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"mode": "fanout", "shards": [TASK] * 12})
        cb = runner.wait_for_callback(arn, name=APPROVAL_CB, timeout=30)
        assert len(fm.published) == 1 and set(fm.published[0]) == {"callback_id", "plan", "execution"}
        assert fm.published[0]["callback_id"] == cb and fm.published[0]["plan"]["needs_approval"] is True
        runner.send_callback_failure(cb, ErrorObject(message="denied by approver.py", type="ApprovalDenied",
                                                     data=None, stack_trace=None))
        out = _outcome(runner.wait_for_result(arn, timeout=30))
    assert out["status"] == "denied" and "denied by approver.py" in out["reason"]
    assert out["plan"]["shards"] == 12 and out["plan"]["waves"] == 3
    assert fm.leases == [] and fm.terminated == []


def test_fanout_approved_then_runs_in_waves_of_four(fm):
    runner = DurableFunctionTestRunner(handler=app.handler, poll_interval=0.1)
    with runner:
        arn = runner.run_async({"mode": "fanout", "shards": [{**TASK, "shard": i} for i in range(12)]})
        cb = runner.wait_for_callback(arn, name=APPROVAL_CB, timeout=30)
        assert fm.leases == []  # nothing launched before the approval
        runner.send_callback_success(cb, json.dumps({"approved": True}).encode())
        for i in range(12):
            shard_cb = runner.wait_for_callback(arn, name=f"shard-{i}-0-callback", timeout=30)
            assert len(fm.leases) <= i + 4  # never more than 4 in flight
            _succeed(runner, shard_cb, i + 1)
        out = _outcome(runner.wait_for_result(arn, timeout=60))
    assert out["status"] == "done" and out["succeeded"] == 12 and out["failed"] == 0 and out["errors"] == []
    assert out["plan"]["concurrency"] == 4 and out["plan"]["waves"] == 3
    assert sorted(x["task"]["shard"] for x in fm.leases) == list(range(12))
    assert sorted(fm.terminated) == sorted(f"microvm-fake-{i}" for i in range(1, 13))
