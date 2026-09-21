# Architecture

Three figures, each rendered from a Mermaid source in `img/src/`. The numbers are the SPEC policy (budget 120 s, heartbeat timeout 30 s, slack 60 s, `max_concurrency` 4, `max_vm_seconds` 3000, `approval_usd` 0.015) applied to `demo-agent` at 512 MiB on an account with an 8 GB memory quota and RunMicrovm at 1/s, on microvm-ctl 0.3.1.

## One lease end to end

![One lease end to end](img/lease-contract.png)

The orchestrator (the `mvm-demo-sfn-lease` state machine or the `mvm-demo-durable-orchestrator:live` function) calls RunMicrovm with a `runHookPayload` of `{lease, task}`. The hook runtime answers `POST /run` with 200 at once, sends the first heartbeat immediately, then heartbeats every 10 s while `work(task, lease)` runs the steps in a thread. The VM completes the lease itself with SendTaskSuccess or SendTaskFailure (sfn) or SendDurableExecutionCallbackSuccess or Failure (durable); every payload names `microvm_id`, `lease_id`, and `elapsed_s`. The orchestrator resumes, terminates the VM, and outputs the result. The three failure exits differ by whether the orchestrator knows which VM to kill: a typed failure names the VM in its cause, so Step Functions terminates it at once (`TerminateFailed`, 0.3.1); a budget or heartbeat timeout carries no id, so that VM is bounded by `MaximumDurationInSeconds` (budget plus slack, 180 s (120 s on the short machine the hang scenario used: budget 60 + slack 60)) and the reaper. The durable function holds the id from its launch step and terminates in every branch.

## Governed fan-out

![Governed fan-out](img/governed-fanout.png)

Both orchestrators size a fan-out with the same plan before anything launches. `fanout_limit` is the smaller of the memory quota divided by the baseline (8 GB / 512 MiB = 16) and the policy's `max_concurrency` (4), so concurrency is 4. Worst case is N x (budget + slack) VM-seconds priced at 512 MiB. Forty shards (7200 VM-s) exceed `max_vm_seconds` 3000: `mvm lease plan` exits 2 and `run.py` never starts the execution; `lease_map` returns `status: rejected`, zero RunMicrovm calls. Twelve shards (2160 VM-s, $0.0189) exceed `approval_usd` 0.015: the state machine's Gate (more than 8 shards) publishes to SNS `mvm-demo-approvals` with a task token and the approver reads SQS `mvm-demo-approvals-q`; the durable function publishes its callback id to SQS `mvm-demo-durable-approvals` and `approver.py` completes the callback. Approve runs the Map or `context.map` at `MaxConcurrency` 4; deny reaches the `Denied` state or `status: denied` with nothing launched. Four shards (720 VM-s, one wave) and eight shards (1440 VM-s, $0.0126, two waves) pass both checks.

## The deployed stack

![The deployed stack](img/stack.png)

`demo-agent` is one 512 MiB image built from `agent/`. `mvm-demo-sfn` deploys two state machines, `mvm-demo-sfn-lease` (budget 120 s) and `mvm-demo-sfn-lease-short` (budget 60 s, for the hang scenario), because `TimeoutSeconds` is fixed at deploy time. `mvm-demo-sfn-map` adds the Map machine, the SNS topic, the SQS queue subscribed with raw delivery, and the approver in `stepfunctions/run.py`. `mvm-demo-durable` deploys the function behind the `live` alias with the policy in `MVM_LEASE_*` environment variables, its approvals queue, and `durable/approver.py`. Every VM writes one stream to `/aws/lambda-microvms/demo-agent`. The CLI builds the image, prints the plan, runs a token-less lease (`cli-none`), and watches VMs through `GET /status` and `GET /events`. Each orchestrator role holds `lambda:RunMicrovm`, `GetMicrovm`, `TerminateMicrovm`, `ListMicrovms`, `lambda:PassNetworkConnector`, and `iam:PassRole` on its agent role; each agent role holds the log permissions plus `states:SendTask*` on its machines or `lambda:SendDurableExecutionCallback*` on the function.

## What each layer owns

- Orchestrator (Step Functions or the durable function): the token, the budget and heartbeat clocks, the idempotent launch (`ClientToken`, or an at-most-once step), termination, the plan check, the approval gate, and the Map concurrency.
- Plane (`FleetManager`, `LeasePolicy`, `mvm`): `MaximumDurationInSeconds` and the idle policy, the fan-out limit from the quota and the policy, the worst-case cost, the RunMicrovm token bucket, and the reaper by age.
- VM runtime (`microvm_hooks` plus `agent/app.py`): accepting `/run`, heartbeats, running the steps, job telemetry, the completer for the lease kind, and stopping work once the lease is lost.
- Approver (`run.py` or `approver.py`): reading the queue and completing the task token or callback with success or failure.

## Failure exits

| Exit | Step Functions | Durable function | Since |
|---|---|---|---|
| Typed failure (`LeaseError`, `Unexpected`) | VM sends SendTaskFailure; the cause names `microvm_id`; `OnLeaseError` routes to `TerminateFailed`, then `Reap`, `TerminateStale`, `Failed` | VM sends the callback failure; the function terminates the VM it launched and returns `status: failed`; `lease_with_relaunch` retries once when `retryable` | 0.2.0 typed failures; 0.3.1 `TerminateFailed` |
| Budget timeout | `States.Timeout` at `TimeoutSeconds`; no id in the error, so the VM runs until `MaximumDurationInSeconds` (budget + slack); `Reap` and `TerminateStale` cover anything older | `CallbackTimeoutError` at the budget; the function terminates the VM and returns `status: timed_out`, then relaunches once | 0.2.0 |
| Heartbeat timeout | `States.HeartbeatTimeout` after 30 s without a heartbeat; same path as the budget timeout | `heartbeat_timeout` raises `CallbackTimeoutError`; terminate, `timed_out` | 0.2.0; 0.3.1 clamps `heartbeat_s` to a third of the timeout and sends the first heartbeat at once, after a 30 s interval lost a durable lease at 30.1 s |
| Lease lost (token closed while the VM works) | A heartbeat answers `TaskTimedOut`; the runtime sets `lease.lost` and the next `lease.check()` raises `LeaseLost`, so no further step is spent | A heartbeat answers `CallbackTimeoutException`; same runtime behaviour | 0.2.0 |
| Throttling (`ThrottlingException`, `ServiceQuotaExceededException`) | `Retry` on the lease and terminate states: 2 s, backoff 2, full jitter, 8 attempts | `FleetManager`'s token bucket paces RunMicrovm at the applied rate and retries with jitter | 0.1.0 token bucket; 0.3.0 the 2 s first retry |
