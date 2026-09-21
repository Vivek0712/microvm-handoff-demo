# Durable functions results

Every row is one live execution of `mvm-demo-durable-orchestrator:live` (stack `mvm-demo-durable`, account 643603452951, us-east-1) driven by [`durable/run.py`](../../durable/run.py). Seconds are the service's own `StartTimestamp` to `EndTimestamp` from `get_durable_execution`; VMs and RunMicrovm calls are counted from the execution history and the orchestrator's CloudWatch log; `matches expected` is computed by `run.py` from the captured output and history against the SPEC row. Each directory holds `input.json`, `output.json`, `history.json`, `history.txt`, `vm-logs.txt`, `orchestrator-logs.txt`, and `summary.json`.

| scenario | execution status / outcome | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|
| [`single`](single/) | SUCCEEDED / `done` | 7.7 | 1 | 1 | yes | 1 launch step; 1 lease callback succeeded; 1 terminate step; ExecutionSucceeded with status done |
| [`parallel`](parallel/) | not run | | | | | |
| [`retryable-fail`](retryable-fail/) | not run | | | | | |
| [`hang`](hang/) | not run | | | | | |
| [`fanout-4`](fanout-4/) | not run | | | | | |
| [`fanout-8`](fanout-8/) | not run | | | | | |
| [`fanout-refused`](fanout-refused/) | not run | | | | | |
| [`fanout-approved`](fanout-approved/) | not run | | | | | |
| [`fanout-denied`](fanout-denied/) | not run | | | | | |

## Notes and deviations

Every line below is copied from the `notes` of that scenario's `summary.json`.

- `single`: 1 demo-agent VM(s) not ours were live at start: microvm-31d9643e-e60a-3222-b05f-3f62a7523a6e
