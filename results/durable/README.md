# Durable functions results

Every row is one live execution of `mvm-demo-durable-orchestrator:live` (stack `mvm-demo-durable`, account 643603452951, us-east-1) driven by [`durable/run.py`](../../durable/run.py). Seconds are the service's own `StartTimestamp` to `EndTimestamp` from `get_durable_execution`; VMs and RunMicrovm calls are counted from the execution history and the orchestrator's CloudWatch log; `matches expected` is computed by `run.py` from the captured output and history against the SPEC row. Each directory holds `input.json`, `output.json`, `history.json`, `history.txt`, `vm-logs.txt`, `orchestrator-logs.txt`, and `summary.json` (`hang` also `vm-status.json`, the VM's own `GET /status` snapshots sampled while it hung). The microvm-ctl column is the version the deployed function logged for that execution (`microvm-ctl X.Y.Z handling ...` in `orchestrator-logs.txt`).

## Results (latest run of every scenario)

| scenario | microvm-ctl | execution status / outcome | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|---|
| [`single`](single/) | 0.3.1 | SUCCEEDED / `done` | 7.2 | 1 | 1 | yes | 1 launch step; 1 lease callback succeeded; 1 terminate step; ExecutionSucceeded with status done |
| [`parallel`](parallel/) | 0.3.0 [1] | SUCCEEDED / `done` | 15.8 | 2 | 2 | yes | plan step; 2 launch steps; 2 lease callback succeeded; 2 terminate steps; ExecutionSucceeded with status done |
| [`retryable-fail`](retryable-fail/) | 0.3.0 [1] | SUCCEEDED / `failed` | 8.5 | 2 | 2 | yes | 2 launch steps; 2 lease callback failed; 2 terminate steps; ExecutionSucceeded with status failed |
| [`hang`](hang/) | 0.3.1 | SUCCEEDED / `timed_out` | 120.7 | 2 | 2 | yes | 2 launch steps; 2 lease callback timed out; 2 terminate steps; ExecutionSucceeded with status timed_out |
| [`fanout-4`](fanout-4/) | 0.3.0 [1] | SUCCEEDED / `done` | 7.3 | 4 | 4 | yes | plan step; 4 launch steps; 4 lease callback succeeded; 4 terminate steps; ExecutionSucceeded with status done |
| [`fanout-8`](fanout-8/) | 0.3.0 [1] | SUCCEEDED / `done` | 13.1 | 8 | 8 | yes | plan step; 8 launch steps; 8 lease callback succeeded; 8 terminate steps; ExecutionSucceeded with status done |
| [`fanout-refused`](fanout-refused/) | 0.3.1 | SUCCEEDED / `rejected` | 0.2 | 0 | 0 | yes | plan step; ExecutionSucceeded with status rejected |
| [`fanout-approved`](fanout-approved/) | 0.3.0 [1] | SUCCEEDED / `done` | 17.8 | 12 | 12 | yes | plan step; approval callback succeeded; 12 launch steps; 12 lease callback succeeded; 12 terminate steps; ExecutionSucceeded with status done |
| [`fanout-denied`](fanout-denied/) | 0.3.1 | SUCCEEDED / `denied` | 0.7 | 0 | 0 | yes | plan step; approval callback failed (denied); ExecutionSucceeded with status denied |

[1] Function versions 1 and 2 (the first pass) were built from `microvm-ctl[durable]>=0.3.0` before the orchestrator logged its library version; `microvm_ctl-0.3.0.dist-info` was in `.aws-sam/build/Orchestrator` at deploy time.

## Notes and deviations

Every line below is copied from the `notes` of that scenario's `summary.json`.

- `hang`: microvm-a3651a60-9b49-345d-bee0-e32cc1e9138a: GET /status sampled 18 times, heartbeats 0 -> 5 between 2026-09-21T01:35:18.813Z and 2026-09-21T01:36:16.742Z, last phase hang
- `hang`: microvm-d6881350-9e6d-3cc0-8204-4b7dcce104a4: GET /status sampled 6 times, heartbeats 0 -> 5 between 2026-09-21T01:36:25.874Z and 2026-09-21T01:37:09.251Z, last phase hang
- `hang`: the library reports the SPEC's `status: timeout` as `status: timed_out` (lease_microvm's outcome)
- `fanout-4`: RunMicrovm throttling retries seen in the orchestrator log: 0
- `fanout-8`: RunMicrovm throttling retries seen in the orchestrator log: 0
- `fanout-approved`: approver.py approved callback Ab9hZXi2YXJu... for execution mvm-demo-durable-fanout-approved-20260921-013000 (12 shards on 0.5 GB: 4 at a time (policy max_concurrency 4), 3 waves, all running in ~22 s, worst case 2160 VM-s = $0.02, needs approval (above $0.015))
- `fanout-denied`: approver.py denied callback Ab9hZXi0YXJu... for execution mvm-demo-durable-fanout-denied-20260921-013922 (12 shards on 0.5 GB: 4 at a time (policy max_concurrency 4), 3 waves, all running in ~22 s, worst case 2160 VM-s = $0.02, needs approval (above $0.015))

## First pass on microvm-ctl 0.3.0 (kept under `<scenario>/first-pass-0.3.0/`)

| scenario | microvm-ctl | execution status / outcome | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|---|
| [`single`](single/first-pass-0.3.0/) | 0.3.0 [1] | SUCCEEDED / `done` | 7.1 | 1 | 1 | yes | 1 launch step; 1 lease callback succeeded; 1 terminate step; ExecutionSucceeded with status done |
| [`hang`](hang/first-pass-0.3.0/) | 0.3.0 [1] | SUCCEEDED / `timed_out` | 120.7 | 2 | 2 | yes | 2 launch steps; 2 lease callback timed out; 2 terminate steps; ExecutionSucceeded with status timed_out |
| [`hang (first attempt)`](hang/first-pass-0.3.0/) | 0.3.0 [1] | SUCCEEDED / `timed_out` | 60.6 | 2 | 2 | no | 2 launch steps; 2 lease callback timed out; 2 terminate steps; ExecutionSucceeded with status timed_out |
| [`fanout-refused`](fanout-refused/first-pass-0.3.0/) | 0.3.0 [1] | SUCCEEDED / `rejected` | 0.2 | 0 | 0 | yes | plan step; ExecutionSucceeded with status rejected |
| [`fanout-approved (first attempt)`](fanout-approved/) | 0.3.0 [1] | SUCCEEDED / `denied` | 602.7 | 0 | 0 | no | plan step; 1 lease callback timed out; ExecutionSucceeded with status denied |
| [`fanout-denied`](fanout-denied/first-pass-0.3.0/) | 0.3.0 [1] | SUCCEEDED / `denied` | 0.7 | 0 | 0 | yes | plan step; approval callback failed (denied); ExecutionSucceeded with status denied |

### Deviation: `hang` lost the lease at +30 s, not at the 60 s budget

First attempt, default heartbeat interval ([history](hang/first-pass-0.3.0/first-attempt-history.txt), [VM logs](hang/first-pass-0.3.0/first-attempt-vm-logs.txt)). Verbatim from the history:

```
   +0.101  2026-09-21T01:04:53.921Z  CallbackStarted              Callback   lease-0-callback                             callback_id=Ab9hZXirYXJu... HeartbeatTimeout=30 Timeout=60
  +30.101  2026-09-21T01:05:23.921Z  CallbackTimedOut             Callback   lease-0-callback                             error=None: None
  +30.331  2026-09-21T01:05:24.151Z  CallbackStarted              Callback   lease-1-callback                             callback_id=Ab9hZXirYXJu... HeartbeatTimeout=30 Timeout=60
  +60.331  2026-09-21T01:05:54.151Z  CallbackTimedOut             Callback   lease-1-callback                             error=None: None
  +60.642  2026-09-21T01:05:54.462Z  ExecutionSucceeded                      mvm-demo-durable-hang-20260921-010453        result={"status": "timed_out", "retryable": true, "vm": {"microvm_id": "microvm-3c0f910e-2a29-3825-8a8c-a7c329d77746", "endpoint": "0cd060bf-867b-6a1e-28af-f6469b9cd01...
```

Both callbacks ended with `CallbackTimedOut` 30 s after `CallbackStarted` (`HeartbeatTimeout=30 Timeout=60`), so the heartbeat timeout fired, not the budget: the library's default VM heartbeat interval was 30 s, equal to the policy's heartbeat timeout, and the orchestrator's clock starts before RunMicrovm returns, so no heartbeat could land in time (the VM logs show none). The second 0.3.0 attempt ([history](hang/first-pass-0.3.0/history.txt)) passed `heartbeat_s=10` from the orchestrator as a workaround and reached the 60 s budget. microvm-ctl 0.3.1 fixes it in the library (`LeasePolicy.heartbeat_every` clamps the interval to a third of the heartbeat timeout and the hook runtime heartbeats on accept); the workaround was removed before the second pass.

### Deviation: `fanout-approved` first attempt was never answered

([history](fanout-approved/first-attempt-history.txt), [summary](fanout-approved/first-attempt-summary.json), [orchestrator log](fanout-approved/first-attempt-orchestrator-logs.txt)). Verbatim notes:

- approver.py saw no approval request within 300 s
- expected approval callback succeeded
- expected 12 shards done (got denied, None)

The approval request reached the queue, but its `execution` field carried the execution *id* (the library's `execution_name` returns the last ARN segment) while `approver.py` filters by the execution *name*; `lease_map` returned `status: denied` after its 600 s approval timeout with nothing launched. The orchestrator now publishes the name segment of the ARN (`execution_label` in `app.py`); the rows above are the rerun.

## Second pass on microvm-ctl 0.3.1

`single` and `hang` rerun after `requirements.txt` moved to `microvm-ctl[durable]>=0.3.1` and the orchestrator stopped passing a heartbeat interval of its own (function version 3).

| scenario | microvm-ctl | execution status / outcome | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|---|
| [`single`](single/) | 0.3.1 | SUCCEEDED / `done` | 7.2 | 1 | 1 | yes | 1 launch step; 1 lease callback succeeded; 1 terminate step; ExecutionSucceeded with status done |
| [`hang`](hang/) | 0.3.1 | SUCCEEDED / `timed_out` | 120.7 | 2 | 2 | yes | 2 launch steps; 2 lease callback timed out; 2 terminate steps; ExecutionSucceeded with status timed_out |

For `hang` the expected picture is `CallbackStarted ... Timeout=60`, `CallbackTimedOut` exactly 60 s later for each attempt, a terminate step per attempt, `status: timed_out`, and the VM's own `GET /status` counter of heartbeats rising every 10 s in between ([vm-status.json](hang/vm-status.json), summarised in the notes above). The `demo-agent` image is still version 1.0, whose hook runtime predates 0.3.1's heartbeat-on-accept, so the counter reaches 1 about 10 s after the lease is accepted and then rises by one every ~10 s; the 10 s interval is the one the 0.3.1 library wrote into the lease payload (`LeasePolicy.heartbeat_every`). Verbatim from the second-pass history:

```
   +0.092  2026-09-21T01:35:16.799Z  CallbackStarted              Callback   lease-0-callback                             callback_id=Ab9hZXirYXJu... HeartbeatTimeout=30 Timeout=60
  +60.092  2026-09-21T01:36:16.799Z  CallbackTimedOut             Callback   lease-0-callback                             error=None: None
  +60.304  2026-09-21T01:36:17.011Z  StepStarted                  Step       lease-0-terminate
  +60.304  2026-09-21T01:36:17.011Z  StepSucceeded                Step       lease-0-terminate                            result={"t": "m", "v": {"terminated": {"t": "s", "v": "microvm-a3651a60-9b49-345d-bee0-e32cc1e9138a"}}} RetryDetails={'CurrentAttempt': 1}
  +60.345  2026-09-21T01:36:17.052Z  CallbackStarted              Callback   lease-1-callback                             callback_id=Ab9hZXirYXJu... HeartbeatTimeout=30 Timeout=60
 +120.345  2026-09-21T01:37:17.052Z  CallbackTimedOut             Callback   lease-1-callback                             error=None: None
 +120.538  2026-09-21T01:37:17.245Z  StepStarted                  Step       lease-1-terminate
 +120.567  2026-09-21T01:37:17.274Z  StepSucceeded                Step       lease-1-terminate                            result={"t": "m", "v": {"terminated": {"t": "s", "v": "microvm-d6881350-9e6d-3cc0-8204-4b7dcce104a4"}}} RetryDetails={'CurrentAttempt': 1}
 +120.693  2026-09-21T01:37:17.400Z  ExecutionSucceeded                      mvm-demo-durable-hang-20260921-013516        result={"status": "timed_out", "retryable": true, "vm": {"microvm_id": "microvm-d6881350-9e6d-3cc0-8204-4b7dcce104a4", "endpoint": "4cd060cd-a977-5307-abc0-2fdd318bf99...
```
