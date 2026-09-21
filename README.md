# microvm-handoff-demo

One agent image, `demo-agent`, is leased by AWS Step Functions and by a Lambda durable function through the microvm-ctl lease contract, and every scenario in the matrix was run live in us-east-1 (account 643603452951). The execution histories, VM logs, orchestrator logs, benchmarks, and console screenshots are checked in under [`results/`](results/) and [`benchmarks/results/`](benchmarks/results/); nothing in them is typed in by hand.

The library is microvm-ctl 0.3.1 from PyPI. The first pass ran on 0.3.0 and found two things that became 0.3.1 ([CHANGELOG](https://github.com/Vivek0712/microvm-ctl/blob/main/CHANGELOG.md)); the rows that changed were rerun and the first-pass files are kept under `<scenario>/first-pass-0.3.0/`.

## What was run

Nine scenarios from [`scenarios/scenarios.json`](scenarios/scenarios.json) on both orchestrators, plus one lease driven by the CLI alone. Status and seconds are the service's own clock as tabulated in [`results/RESULTS.md`](results/RESULTS.md); each cell links to the directory with the input, full history, output, VM log lines, orchestrator log lines, and the `summary.json` the row came from.

| scenario | Step Functions | Lambda durable function |
|---|---|---|
| `single` | [SUCCEEDED, 4.3 s, 1 VM](results/stepfunctions/single/) | [SUCCEEDED / `done`, 7.2 s, 1 VM](results/durable/single/) |
| `parallel` | [SUCCEEDED, 5.5 s, 2 VMs](results/stepfunctions/parallel/) | [SUCCEEDED / `done`, 15.8 s, 2 VMs](results/durable/parallel/) |
| `retryable-fail` | [FAILED (`LeaseFailed`), 4.3 s, 1 VM](results/stepfunctions/retryable-fail/) | [SUCCEEDED / `failed`, retryable, 8.5 s, 2 VMs](results/durable/retryable-fail/) |
| `hang` | [FAILED (`States.Timeout`), 60.3 s, 1 VM](results/stepfunctions/hang/) | [SUCCEEDED / `timed_out`, 120.7 s, 2 VMs](results/durable/hang/) |
| `fanout-4` | [SUCCEEDED, 5.5 s, 4 VMs](results/stepfunctions/fanout-4/) | [SUCCEEDED / `done`, 7.3 s, 4 VMs](results/durable/fanout-4/) |
| `fanout-8` | [SUCCEEDED, 11.1 s, 8 VMs](results/stepfunctions/fanout-8/) | [SUCCEEDED / `done`, 13.1 s, 8 VMs](results/durable/fanout-8/) |
| `fanout-refused` | [REFUSED before start, 0 VMs](results/stepfunctions/fanout-refused/) | [SUCCEEDED / `rejected`, 0.2 s, 0 VMs](results/durable/fanout-refused/) |
| `fanout-approved` | [SUCCEEDED, 17.3 s, 12 VMs](results/stepfunctions/fanout-approved/) | [SUCCEEDED / `done`, 17.8 s, 12 VMs](results/durable/fanout-approved/) |
| `fanout-denied` | [FAILED (`ApprovalDenied`), 0.6 s, 0 VMs](results/stepfunctions/fanout-denied/) | [SUCCEEDED / `denied`, 0.7 s, 0 VMs](results/durable/fanout-denied/) |
| `cli-none` | [`mvm lease run --kind none --wait`, `mvm watch` for 5.9 s, `passed: true`, `heartbeats: 0`](results/cli/) | |

Every row matches its expected column ([Step Functions README](results/stepfunctions/README.md), [durable README](results/durable/README.md)). `parallel` is two executions per orchestrator; in-VM wall time from the VM's step durations is 3.088 s parallel against 12.113 s sequential on Step Functions and about 3.10 s against 12.08 s on the durable side.

## Two things the validation found

**A typed lease failure named its VM, but Step Functions did not terminate it.** The VM's `SendTaskFailure` cause carries `microvm_id`, yet the 0.3.0 machine's only cleanup after the Catch was `Reap`, which terminates fleet members older than budget plus slack. Verbatim from [`results/stepfunctions/README.md`](results/stepfunctions/README.md):

> deviation: the failed lease's VM was not terminated by the machine. TerminateStale only terminates members older than budget + slack (180 s) and this VM was ~10 s old, so the Map ran over an empty list and the VM stayed RUNNING until run.py's straggler check terminated it (SPEC: terminate everything you launch).

0.3.1 adds `OnLeaseError` (a Choice on whether the cause contains `"microvm_id"`) and `TerminateFailed` before `Reap`. In the rerun ([`results/stepfunctions/retryable-fail/`](results/stepfunctions/retryable-fail/)) the path is `Lease -> OnLeaseError -> TerminateFailed -> Reap -> TerminateStale -> Failed`, and `summary.json` records `seconds_from_task_failed_to_vm_terminated_at` of 0.557 with `terminated_by: machine`.

**A 30 s heartbeat interval against a 30 s heartbeat timeout lost the durable lease before the first heartbeat.** Verbatim from [`results/durable/README.md`](results/durable/README.md):

> Both callbacks ended with `CallbackTimedOut` 30 s after `CallbackStarted` (`HeartbeatTimeout=30 Timeout=60`), so the heartbeat timeout fired, not the budget: the library's default VM heartbeat interval was 30 s, equal to the policy's heartbeat timeout, and the orchestrator's clock starts before RunMicrovm returns, so no heartbeat could land in time (the VM logs show none).

The first-attempt history has `CallbackStarted` at +0.101 and `CallbackTimedOut` at +30.101 ([`first-attempt-history.txt`](results/durable/hang/first-pass-0.3.0/first-attempt-history.txt)). 0.3.1 clamps the interval to a third of `heartbeat_timeout_s` (`LeasePolicy.heartbeat_every`), so the payload now says `heartbeat_s: 10`. In the rerun ([`results/durable/hang/`](results/durable/hang/)) each callback starts and times out exactly 60 s apart (+0.092 to +60.092, +60.345 to +120.345 in `history.txt`), and the VM's own `GET /status` counter climbs from 0 to 5 heartbeats over 58 s, one every 10 s ([`vm-status.json`](results/durable/hang/vm-status.json)).

**The timeout bound on Step Functions.** A `States.Timeout` carries no VM id, so the machine cannot terminate that VM and relies on `MaximumDurationInSeconds`. In [`results/stepfunctions/hang/summary.json`](results/stepfunctions/hang/summary.json) the task timed out 60.122 s after start, the machine terminated nothing, and the VM's own `startedAt` and `terminatedAt` give a lifetime of 122.03 s against a `MaximumDurationInSeconds` of 120 (budget 60 plus slack 60). `run.py` left that VM alone and polled `GetMicrovm` until the platform ended it.

## Benchmarks

[`benchmarks/bench.py`](benchmarks/bench.py) ran `single` five times per orchestrator and `fanout-4` and `fanout-8` twice each, one run at a time. Every number is the orchestrator's own start and stop (`describe_execution`, `get_durable_execution`) and each VM's own `startedAt` and `terminatedAt` from `GetMicrovm`; nothing is timed from a poll loop. Cost is VM-seconds at 512 MiB priced by `microvm.lease.VM_USD_PER_GB_S`. Table from [`bench.md`](benchmarks/results/bench.md); raw runs in [`bench.json`](benchmarks/results/bench.json), all 18 on microvm-ctl 0.3.1 (both stacks redeployed before the run).

| orchestrator | shards | runs ok | p50 end to end | max | VM-s (p50) | USD per run (p50) |
|---|---|---|---|---|---|---|
| durable | 1 | 5/5 | 4.3 s | 7.0 s | 4 | $0.00004 |
| sfn | 1 | 5/5 | 4.3 s | 4.5 s | 4 | $0.00004 |
| durable | 4 | 2/2 | 7.9 s | 8.2 s | 18 | $0.00016 |
| sfn | 4 | 2/2 | 4.6 s | 4.7 s | 18 | $0.00016 |
| durable | 8 | 2/2 | 13.7 s | 13.8 s | 38 | $0.00033 |
| sfn | 8 | 2/2 | 8.9 s | 9.1 s | 35 | $0.00031 |

![p50 end to end by the orchestrator's own clock](benchmarks/results/bench.png)

The bench started at 2026-09-21T01:18Z (`meta.started` in `bench.json`), before the 0.3.1 stacks were deployed, so the chart's subtitle says 0.3.0.

## Screenshots

Console pages were captured by [`tools/console_shot.py`](tools/console_shot.py) (federation sign-in token, headless Chrome); the playground by [`tools/capture_playground.py`](tools/capture_playground.py) during a real 4-shard execution.

| | |
|---|---|
| ![](results/screenshots/sfn-single-execution.png) | `single-20260921-013142` on `mvm-demo-sfn-lease`, Succeeded on 0.3.1: `Lease`, `Terminate`, `Done` lit; `OnLeaseError`, `TerminateFailed`, and `Reap` untouched ([directory](results/stepfunctions/single/)). |
| ![](results/screenshots/sfn-retryable-fail-execution.png) | `retryable-fail-20260921-013159` on the 0.3.1 machine, Failed with `LeaseFailed`: `OnLeaseError` and `TerminateFailed` green on the way to `Reap` and `Failed` ([results](results/stepfunctions/retryable-fail/)). |
| ![](results/screenshots/sfn-hang-execution.png) | `hang-20260921-013219` on `mvm-demo-sfn-lease-short`, Failed after 1:00.349: `OnLeaseError` took its default edge past `TerminateFailed` because the timeout named no VM ([results](results/stepfunctions/hang/)). |
| ![](results/screenshots/sfn-fanout-8-execution.png) | `fanout-8-20260921-011051` on `mvm-demo-sfn-map-lease`, Succeeded in 11.118 s: `Gate` skipped `RequestApproval`, the `Fanout` Map ran 8 shards ([results](results/stepfunctions/fanout-8/)). |
| ![](results/screenshots/sfn-fanout-approved-execution.png) | `fanout-approved-20260921-011125`, Succeeded in 17.283 s: `Gate` sent 12 shards through `RequestApproval` before the Map ([results](results/stepfunctions/fanout-approved/)). |
| ![](results/screenshots/sfn-fanout-denied-execution.png) | `fanout-denied-20260921-011205`, Failed with `ApprovalDenied` in 0.639 s: the `Denied` state, nothing launched ([results](results/stepfunctions/fanout-denied/)). |
| ![](results/screenshots/durable-executions-list.png) | The Durable executions tab of `mvm-demo-durable-orchestrator`: the whole matrix on function version 2, all Succeeded, from 224 ms (`fanout-refused`) to 2 min 653 ms (`hang`). |
| ![](results/screenshots/durable-single-execution.png) | `mvm-demo-durable-single-20260921-013442` on version 3: `lease-0-callback` 4.558 s, `lease-0-launch` 753 ms, `lease-0-terminate` 36 ms, ten events ([results](results/durable/single/)). |
| ![](results/screenshots/durable-hang-execution.png) | `mvm-demo-durable-hang-20260921-013516`: both callbacks Timed out at 1 min, each followed by its terminate step ([results](results/durable/hang/)). |
| ![](results/screenshots/durable-fanout-8-execution.png) | `mvm-demo-durable-fanout-8-20260921-012851`: the `shard-plan` step, then the `shard-map` Map (12.77 s) with its `map-item-N` contexts and callbacks ([results](results/durable/fanout-8/)). |
| ![](results/screenshots/durable-fanout-approved-execution.png) | `mvm-demo-durable-fanout-approved-20260921-013000`: `shard-plan`, the `shard-approval` WaitForCallback (548 ms, answered by `approver.py`), then the 12-shard `shard-map` ([results](results/durable/fanout-approved/)). |
| ![](results/screenshots/cloudwatch-demo-agent-log-group.png) | Log group `/aws/lambda-microvms/demo-agent`: one stream per VM, named `2026/09/21[1.0]<microvm id>`, more than 100 after the runs. |
| ![](results/screenshots/playground-fleet-jobs-running.png) | `mvm playground` during `playground-fanout-4-20260920-184240` ([manifest](results/screenshots/playground-capture.json)): four `demo-agent` VMs RUNNING and the fleet job panel with each shard's lease id, phase `step 2/3`, 25 s elapsed. |
| ![](results/screenshots/playground-lease-form.png) | The playground's Call a VM page after the run, with no active microVMs left to pick. |
| ![](results/screenshots/playground-api-trace.png) | The playground's API trace: the `ListMicrovms` calls the fleet job panel made while polling, with status and milliseconds. |
| ![](results/cli/watch.png) | `mvm watch microvm-af779375-4397-3675-9522-41a334a5620f` from a recording console: the steps streaming, then the job panel at `done`, `4/4`, lease `none`, `heartbeats=0` ([results](results/cli/)). |

## How it is built

[`docs/architecture.md`](docs/architecture.md) explains each figure in detail; [`docs/how-it-works.md`](docs/how-it-works.md) covers the payloads and the tooling.

![One lease end to end](docs/img/lease-contract.png)

The orchestrator calls `RunMicrovm` with a `runHookPayload` of `{lease, task}`; the injected hook runtime answers `POST /run` at once, heartbeats every 10 s while `work(task, lease)` runs the steps in a thread, and completes the lease itself with `SendTaskSuccess`/`SendTaskFailure` or the durable callback equivalents. The failure exits differ by whether the orchestrator knows which VM to kill: a typed failure names it in the cause, a timeout does not.

![Governed fan-out](docs/img/governed-fanout.png)

Both orchestrators size a fan-out with the same plan before anything launches: concurrency is the smaller of quota over baseline (16) and `max_concurrency` (4); worst case is N x (budget + slack) VM-seconds. Forty shards are refused, twelve need approval, four and eight run in one and two waves ([`results/cli/lease-plan-*.txt`](results/cli/)).

![The deployed stack](docs/img/stack.png)

Three CloudFormation stacks (`mvm-demo-sfn`, `mvm-demo-sfn-map`, `mvm-demo-durable`), one image, one log group, and two approvers that read a queue and complete a task token or a callback.

```
agent/          Dockerfile + app.py, the leased image
stepfunctions/  generate.py (ASL from the library), lease.asl.json, map.asl.json, templates, deploy.sh, run.py
durable/        template.yaml, orchestrator/app.py, approver.py, deploy.sh, run.py
scenarios/      scenarios.json, the shared matrix
benchmarks/     bench.py and results/
results/        stepfunctions/, durable/, cli/, screenshots/, RESULTS.md
tools/          report.py, console_shot.py, capture_cli.py, capture_playground.py
docs/           architecture.md, how-it-works.md, img/
```

## Run it yourself

Prerequisites: an AWS account with Lambda MicroVMs, microvm-ctl 0.3.1 (`pip install "microvm-ctl[durable]"`), the AWS CLI and SAM CLI, Python 3.13 on PATH for `sam build` (the function runtime is `python3.13`), Google Chrome for the screenshots, and the variables the CLI reads: `AWS_PROFILE`, `MVM_REGION`, `MVM_ARTIFACT_BUCKET`, `MVM_BUILD_ROLE_ARN`, `MVM_EXECUTION_ROLE_ARN`. The account used here had an 8 GB microVM memory quota and RunMicrovm at 1/s ([`results/cli/quotas.txt`](results/cli/quotas.txt)). The policy everywhere is budget 120 s, heartbeat timeout 30 s, slack 60 s, `max_concurrency` 4, `max_vm_seconds` 3000, `approval_usd` 0.015 ([`SPEC.md`](SPEC.md)), with `ApproveAboveShards` 8 on the Map machine and a 60 s budget on the short machine for `hang`.

```sh
mvm image build demo-agent agent/                       # results/cli/image-build.txt
./stepfunctions/deploy.sh                                # generates the ASL, deploys mvm-demo-sfn and mvm-demo-sfn-map
./durable/deploy.sh                                      # sam build + deploy mvm-demo-durable
python3 stepfunctions/run.py                             # every sfn scenario -> results/stepfunctions/
python3 durable/run.py                                   # every durable scenario -> results/durable/
python3 tools/capture_cli.py                             # cli-none -> results/cli/
python3 benchmarks/bench.py --sfn-arn ... --sfn-map-arn ... --durable-function mvm-demo-durable-orchestrator:live
python3 tools/console_shot.py --out results/screenshots/x.png "<console url>"   # --click TEXT for the durable pages
python3 tools/capture_playground.py --sfn-map-arn ...    # playground screenshots during a 4-shard run
python3 tools/report.py                                  # results/RESULTS.md
```

Both `run.py` scripts accept a subset of scenario ids, list the fleet after each scenario, and terminate any `demo-agent` VM they left running (except the `hang` VM, which is watched until the platform ends it).

## Honest limits

- The `demo-agent` image is still version 1.0, built with the 0.3.0 hook runtime ([`results/cli/image-build.txt`](results/cli/image-build.txt)). 0.3.1's heartbeat on accept was not rebuilt into it, so the first heartbeat lands about 10 s after the lease is accepted; the 10 s interval is the one the 0.3.1 library wrote into the payload.
- Not every row was rerun on 0.3.1. Step Functions `single`, `retryable-fail`, and `hang` are 0.3.1; `parallel` and the five fan-outs are 0.3.0 ([table](results/stepfunctions/README.md)). Durable `single`, `hang`, `fanout-refused`, and `fanout-denied` are 0.3.1 (function version 3); the rest are 0.3.0 on versions 1 and 2 ([table](results/durable/README.md)). The benchmark was rerun on 0.3.1 after the second pass (`meta.microvm_ctl` in [`bench.json`](benchmarks/results/bench.json)).
- Every console screenshot carries the first-visit "Service menu" tooltip, because each capture starts from a fresh Chrome profile.
- The durable `fanout-approved` first attempt was never answered: the request carried the execution id while `approver.py` filtered by name, so `lease_map` returned `denied` after 600 s with nothing launched ([`first-attempt-summary.json`](results/durable/fanout-approved/first-attempt-summary.json)). That was this demo's bug, fixed by `execution_label` in `app.py`; the row shown is the rerun.
- The `parallel` payload's `elapsed_s` includes the completer's boto3 import before `lease accepted`, so in-VM wall time is read from the VM log lines instead.
