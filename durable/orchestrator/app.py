"""mvm-demo-durable orchestrator: every way a durable function hands a task to a MicroVM.

Two event modes, both built on `microvm.integrations.durable`:

    {"mode": "single", "task": {...}, "budget_s": 60}     -> lease_with_relaunch (one relaunch)
    {"mode": "fanout", "shards": [{...}, ...]}            -> lease_map (plan, approval gate, map)

The lease policy is the platform's, read once from MVM_LEASE_* (`LeasePolicy.from_env()`):
budget 120 s, heartbeat 30 s, slack 60 s, max_concurrency 4, max_vm_seconds 3000,
approval_usd 0.015. A `single` event may lower the budget (`budget_s`), which is how the
`hang` scenario waits 60 s instead of 120 s. `fanout` sizes the run with the plane's plan
(baseline 512 MiB): a plan over max_vm_seconds returns `status: rejected` before anything
launches; a plan over approval_usd publishes `{"callback_id", "plan", "execution"}` to the
approvals queue and waits (600 s) for approver.py to complete the callback.

Rules kept from the library: the FleetManager is built lazily inside steps (its
constructor reads Service Quotas, a side effect), and `callback.result()` is never
wrapped in try/finally because the SDK suspends by raising a BaseException.
"""

from __future__ import annotations

import dataclasses
import importlib.metadata
import json
import logging
import os

from aws_durable_execution_sdk_python import DurableContext, durable_execution
from botocore.exceptions import ClientError
from microvm import FleetManager, LeasePolicy, PlaneConfig
from microvm.integrations.durable import execution_name, lease_map, lease_with_relaunch

log = logging.getLogger("mvm-demo-durable")
log.setLevel(logging.INFO)

IMAGE = os.environ.get("MVM_IMAGE", "demo-agent")
IMAGE_VERSION = os.environ.get("MVM_IMAGE_VERSION") or None
BASELINE_MIB = int(os.environ.get("BASELINE_MIB", "512"))
APPROVALS_QUEUE_URL = os.environ.get("APPROVALS_QUEUE_URL", "")
MAX_RELAUNCHES = 1
APPROVAL_TIMEOUT_S = 600
POLICY = LeasePolicy.from_env()
# The VM's heartbeat interval is the library's: LeasePolicy.heartbeat_every clamps it to a third of the
# heartbeat timeout (microvm-ctl >= 0.3.1), so a 30 s timeout gets a 10 s heartbeat.
MICROVM_CTL_VERSION = importlib.metadata.version("microvm-ctl")

_fm: FleetManager | None = None


def _instrument(fm: FleetManager) -> None:
    """Log every RunMicrovm API call, including the throttled retries the plane's token
    bucket hides, so run.py can count them from the orchestrator's log group."""
    throttled = getattr(fm, "_run", None)
    inner = getattr(throttled, "fn", None)
    if inner is None:
        return

    def counted(**kw):
        log.info("RunMicrovm call clientToken=%s", kw.get("clientToken"))
        try:
            return inner(**kw)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            log.warning("RunMicrovm error %s clientToken=%s", code, kw.get("clientToken"))
            raise

    throttled.fn = counted


def fleet_manager() -> FleetManager:
    """Built lazily inside steps: the constructor reads Service Quotas, a side effect that
    has no business running while the SDK replays the handler up to the next step."""
    global _fm
    if _fm is None:
        fm = FleetManager(PlaneConfig())  # MVM_REGION / MVM_EXECUTION_ROLE_ARN from the environment
        _instrument(fm)
        _fm = fm
    return _fm


class _LazyFleet:
    """What the library's steps receive: resolves to the real FleetManager on first use."""

    def __getattr__(self, name):
        return getattr(fleet_manager(), name)


FM = _LazyFleet()


# ----------------------------------------------------------------- pure helpers
def policy_for(event: dict) -> LeasePolicy:
    """The platform policy, with the one per-event override a scenario may make: a lower budget."""
    budget = event.get("budget_s")
    if budget is None:
        return POLICY
    budget = int(budget)
    if not 1 <= budget <= POLICY.budget_s:
        raise ValueError(f"budget_s must be between 1 and the policy's {POLICY.budget_s}, got {budget}")
    return dataclasses.replace(POLICY, budget_s=budget)


def publish_approval(message: dict) -> None:
    """Runs inside the approval callback's submitter step, never during replay."""
    import boto3

    body = json.dumps(message, default=str)
    boto3.client("sqs").send_message(QueueUrl=APPROVALS_QUEUE_URL, MessageBody=body)
    log.info("approval requested: callback %s for execution %s", message["callback_id"], message["execution"])


def execution_label(context: DurableContext) -> str | None:
    """The execution *name* (what run.py chose as DurableExecutionName): the segment after
    `durable-execution/` in the ARN. The library's `execution_name` returns the last segment,
    which is the execution id, and approver.py filters requests by name."""
    arn = getattr(getattr(context, "execution_context", None), "durable_execution_arn", None) or ""
    if "/durable-execution/" in arn:
        return arn.split("/durable-execution/", 1)[1].split("/", 1)[0] or None
    return execution_name(context)


def approver(context: DurableContext):
    """The `approve` callable lease_map hands its callback id to."""
    execution = execution_label(context)

    def approve(callback_id: str, plan: dict) -> None:
        publish_approval({"callback_id": callback_id, "plan": plan, "execution": execution})

    return approve


# ----------------------------------------------------------------- handler
@durable_execution
def handler(event: dict, context: DurableContext) -> dict:
    log.info("microvm-ctl %s handling mode=%s", MICROVM_CTL_VERSION, event.get("mode", "single"))
    mode = event.get("mode", "single")
    if mode == "single":
        task = event.get("task")
        if not isinstance(task, dict):
            return {"status": "bad_request", "reason": "mode single needs a task object"}
        return lease_with_relaunch(context, FM, IMAGE, task, max_relaunches=MAX_RELAUNCHES, label="lease",
                                   policy=policy_for(event), version=IMAGE_VERSION)
    if mode == "fanout":
        shards = event.get("shards")
        if not isinstance(shards, list) or not shards or not all(isinstance(s, dict) for s in shards):
            return {"status": "bad_request", "reason": "mode fanout needs a non-empty list of task objects"}
        return lease_map(context, FM, IMAGE, shards, policy=policy_for(event), label="shard",
                         baseline_mib=BASELINE_MIB, max_concurrency=POLICY.max_concurrency,
                         max_relaunches=MAX_RELAUNCHES, approve=approver(context),
                         approval_timeout_s=APPROVAL_TIMEOUT_S, version=IMAGE_VERSION)
    return {"status": "bad_request", "reason": f"unknown mode {mode!r}"}
