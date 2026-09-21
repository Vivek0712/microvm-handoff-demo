# How it works

The short version of what moves between the orchestrator, the VM, and the tools in this repo. [`architecture.md`](architecture.md) has the figures; every number here is the one in the linked file.

## The lease payload

Both orchestrators launch `demo-agent` with `RunMicrovm` and a `runHookPayload` of two objects ([`agent/app.py`](../agent/app.py)):

```
{"lease": {"kind": "sfn|durable|http|sqs|eventbridge|none", "token": "...", "id": "...",
           "region": "us-east-1", "heartbeat_s": 10},
 "task":  {"steps": ["<shell>", ...], "workdir": "/tmp/job", "env": {"K": "V"},
           "parallel": false, "max_parallel": 4}}
```

In the generated Step Functions machine ([`stepfunctions/lease.asl.json`](../stepfunctions/lease.asl.json)) the `Lease` state is `runMicrovm.waitForTaskToken` with `TimeoutSeconds` 120, `HeartbeatSeconds` 30, `MaximumDurationInSeconds` 180, an idle policy of 120 s with no auto-resume, and a `ClientToken` built from the execution and state name so a retried launch is idempotent. The lease's `token` is `$states.context.Task.Token`, its `id` the execution name, and the task is the execution input. The durable function ([`durable/orchestrator/app.py`](../durable/orchestrator/app.py)) reads the same policy from `MVM_LEASE_*` environment variables set by [`durable/template.yaml`](../durable/template.yaml), creates a callback with the budget and heartbeat timeout, and launches in an at-most-once step so a replay never launches twice; a `single` event may lower the budget (`budget_s: 60` for `hang`). The task's test aids are `hang_s` (sleep after the steps) and `fail_after_s` (raise a retryable `Injected` error).

## What the VM does on /run

The hook runtime injected into the image decodes the lease, answers `POST /run` with 200 at once, and runs `work(task, lease)` in a thread while heartbeating every `heartbeat_s`. `work` calls `lease.check()` before each step so a lost lease stops further spend, runs each step with `bash -c`, and reports phase, progress, and output through `lease.job` (`GET /status`, `GET /events`, the VM's CloudWatch stream). Sequential steps fail the lease on the first non-zero exit with `StepFailed`; `"parallel": true` runs them on a pool of `max_parallel` workers and raises one `StepFailed` listing every failing step after all finish. The return value is `{passed, steps, parallel, microvm_id, heartbeats}`.

## The completer per orchestrator

The VM completes its own lease. For `kind: sfn` the completer calls `SendTaskSuccess` with the result or `SendTaskFailure` with a cause that names `microvm_id`, `lease_id`, `elapsed_s`, and the typed error; heartbeats are `SendTaskHeartbeat`, and the agent role holds those three actions ([`results/cli/lease-policy-sfn.txt`](../results/cli/lease-policy-sfn.txt)). For `kind: durable` the calls are `SendDurableExecutionCallbackSuccess`, `Failure`, and `Heartbeat` ([`lease-policy-durable.txt`](../results/cli/lease-policy-durable.txt)). For `kind: none` (the CLI scenario) there is nothing to complete and the log line reads `lease success delivered {"kind": "none"}` with `heartbeats: 0` ([`results/cli/status.txt`](../results/cli/status.txt)). Step Functions then resumes at `Terminate`, or on failure at `OnLeaseError`, `TerminateFailed` when the cause names a VM, `Reap`, and `TerminateStale`; the durable function holds the VM id from its launch step, terminates in every branch, and `lease_with_relaunch` retries once when the failure is retryable.

## The plan and the approval gate

The plan sizes a fan-out before anything launches: with an 8 GB quota, a 512 MiB baseline, and `max_concurrency` 4, concurrency is 4, and worst case is N x (120 + 60) VM-seconds at 512 MiB. The four plans under [`results/cli/`](../results/cli/):

| shards | waves | worst case | outcome | file |
|---|---|---|---|---|
| 4 | 1 | 720 VM-s, $0.0063 | runs, exit 0 | [`lease-plan-4.txt`](../results/cli/lease-plan-4.txt) |
| 8 | 2 | 1440 VM-s, $0.0126 | runs, exit 0 | [`lease-plan-8.txt`](../results/cli/lease-plan-8.txt) |
| 12 | 3 | 2160 VM-s, $0.0189 | needs approval (above $0.015), exit 3 | [`lease-plan-12.txt`](../results/cli/lease-plan-12.txt) |
| 40 | 10 | 7200 VM-s | rejected, exceeds `max_vm_seconds` 3000, exit 2 | [`lease-plan-40.txt`](../results/cli/lease-plan-40.txt) |

On Step Functions, `stepfunctions/run.py` runs `mvm lease plan` first and refuses to start the execution on exit 2; on exit 3 it starts the Map machine, whose `Gate` routes more than `ApproveAboveShards` (8) shards to `RequestApproval`, an SNS publish with a task token to `mvm-demo-approvals`. The approver in `run.py` reads the subscribed queue `mvm-demo-approvals-q` and calls `SendTaskSuccess` or `SendTaskFailure`. In the durable function, `lease_map`'s plan step returns `status: rejected` for 40 shards with zero `RunMicrovm` calls ([`results/durable/fanout-refused/output.json`](../results/durable/fanout-refused/output.json)); for 12 it publishes `{callback_id, plan, execution}` to `mvm-demo-durable-approvals` and waits up to 600 s for [`durable/approver.py`](../durable/approver.py) to complete the callback. Approved runs go through the Map at `MaxConcurrency` 4 (or `context.map`); denied ones reach the `Denied` state or `status: denied` with nothing launched.

## How run.py drives and captures a scenario

Each `run.py` holds the scenario table (machine or event mode, input, expected outcome, approver decision) and runs one scenario at a time. [`stepfunctions/run.py`](../stepfunctions/run.py) starts one execution of the right machine (`mvm-demo-sfn-lease`, `-lease-short` for `hang`, `-map-lease` for fan-outs), waits for it to end, and writes `input.json`, every page of `get_execution_history` as `history.json` plus a readable `history.txt` (event, state, timestamp, seconds since `ExecutionStarted`), `output.json`, the VM's lines from `/aws/lambda-microvms/demo-agent` as `vm-logs.txt`, the machine's vended log lines as `orchestrator-logs.txt`, `plan.txt` for fan-outs, and `summary.json`, whose `matches_expected` is the conjunction of named checks computed from what was captured. Task tokens are redacted. [`durable/run.py`](../durable/run.py) invokes `mvm-demo-durable-orchestrator:live` asynchronously with a deterministic `DurableExecutionName`, answers the approval request for the two gate scenarios, polls `get_durable_execution` until the execution ends, and writes the same set from `get_durable_execution_history` and the function's log group. Both list the fleet afterwards and terminate any `demo-agent` VM still alive, except the Step Functions `hang` VM, which is watched until the platform ends it. Seconds come from the service's timestamps, never the poll loop; `tools/report.py` folds the summaries into `results/RESULTS.md`.

## How the benchmark times things

[`benchmarks/bench.py`](../benchmarks/bench.py) starts one execution at a time so the bench itself never contends for the 1 RunMicrovm/s and 8 GB quota. For each run it records the orchestrator's start and stop (`describe_execution` startDate/stopDate; `get_durable_execution` StartTimestamp/EndTimestamp), the VM ids the output names, and each VM's `startedAt` to `terminatedAt` from `GetMicrovm` (polled up to fifteen times, because `terminatedAt` lands a few seconds after the orchestrator stops). VM-seconds times 512 MiB times `microvm.lease.VM_USD_PER_GB_S` gives the cost. `summarize` takes the median and max per orchestrator and shard count, `svg` draws grouped bars, and headless Chrome renders the PNG.

## How console screenshots are made

[`tools/console_shot.py`](../tools/console_shot.py) calls `sts.get_federation_token` with a read-only policy (states, lambda, logs, cloudwatch, sns, sqs, cloudformation describe and list actions), posts the credentials to `signin.aws.amazon.com/federation?Action=getSigninToken`, and builds an `Action=login` URL whose `Destination` is the console page. The plain path hands that URL to headless Chrome with `--screenshot` and a `--virtual-time-budget` so the SPA settles. The Lambda durable pages need clicks first, so `--click TEXT` drives a real Chrome over the DevTools protocol instead: navigate, wait, evaluate a script that finds the element with exactly that text and clicks it, wait again, then `Page.captureScreenshot`. Temporary credentials live only in the process and the sign-in token is single use. [`tools/capture_playground.py`](../tools/capture_playground.py) reuses that DevTools driver for `mvm playground`, which polls and streams and so never goes idle for the one-shot mode, starting a 4-shard execution on the Map machine and screenshotting the fleet job panel while the shards run. [`tools/capture_cli.py`](../tools/capture_cli.py) renders `mvm watch` from a rich `Console(record=True)` to SVG and then PNG.
