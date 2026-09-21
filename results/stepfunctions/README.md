# Step Functions results

Every row is one live execution in account 643603452951 (us-east-1) driven by `stepfunctions/run.py`; the linked directory holds the input, the full history, the output, the VM's log lines, the machine's log lines, and `summary.json`. Seconds come from `describe_execution` startDate/stopDate. RunMicrovm calls are the `TaskScheduled` events for `runMicrovm` in the history (retries count). `matches expected` is the conjunction of the named checks in each `summary.json`.

| scenario | status | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|
| [single](single/) | SUCCEEDED | 4.166 | 1 | 1 | yes | Lease -> Terminate -> Done; peak 1 in flight |
| [parallel](parallel/) | SUCCEEDED | 5.549 | 2 | 2 | yes | Lease -> Terminate -> Done; peak 1 in flight; in-VM wall 3.088 s parallel vs 12.113 s sequential |
| [retryable-fail](retryable-fail/) | FAILED | 3.827 | 1 | 1 | yes | Lease -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |
| [hang](hang/) | FAILED | 60.318 | 1 | 1 | NO | Lease -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |
| [fanout-4](fanout-4/) | SUCCEEDED | 5.542 | 4 | 4 | yes | Gate -> Fanout -> Done (per shard: Done, Lease, ShardDone, Terminate); peak 4 in flight |
| [fanout-8](fanout-8/) | SUCCEEDED | 11.118 | 8 | 8 | yes | Gate -> Fanout -> Done (per shard: Done, Lease, ShardDone, Terminate); peak 4 in flight |
| [fanout-refused](fanout-refused/) | REFUSED | 0.0 | 0 | 0 | yes | no execution; `mvm lease plan --shards 40` exited 2: worst case 7200 VM-seconds exceeds policy max_vm_seconds 3000 |
| [fanout-approved](fanout-approved/) | SUCCEEDED | 17.283 | 12 | 12 | yes | Gate -> RequestApproval -> Fanout -> Done (per shard: Done, Lease, ShardDone, Terminate); peak 4 in flight; approver called SendTaskSuccess |
| [fanout-denied](fanout-denied/) | FAILED | 0.639 | 0 | 0 | yes | Gate -> RequestApproval -> Denied; error ApprovalDenied; approver called SendTaskFailure |

## Deviations from SPEC.md

- **retryable-fail** (expected: sfn: Lease -> Catch -> Reap -> TerminateStale -> Failed with the typed error in the output)
  - deviation: the failed lease's VM was not terminated by the machine. TerminateStale only terminates members older than budget + slack (180 s) and this VM was ~10 s old, so the Map ran over an empty list and the VM stayed RUNNING until run.py's straggler check terminated it (SPEC: terminate everything you launch).
  - straggler: microvm-8a168ff7-d287-342d-934f-b16cd996fd70 was still RUNNING after the execution ended; run.py terminated it
- **hang** (expected: sfn: States.Timeout -> Reap terminates the VM -> Failed)
  - check `reap_terminated_the_vm` is false ([summary.json](hang/summary.json))
  - deviation: Reap did not terminate the hung VM. TerminateStale only terminates members older than budget + slack (120 s here) and the VM was ~60 s old at States.Timeout, so the Map ran over an empty list; the VM kept running on its own MaximumDurationInSeconds cap until run.py's straggler check terminated it.
  - straggler: microvm-a1f16a50-2105-3030-aafb-63c45e2176cf was still RUNNING after the execution ended; run.py terminated it

## Notes

- parallel: two executions: arn:aws:states:us-east-1:643603452951:execution:mvm-demo-sfn-lease:parallel-20260921-011310 (parallel, top-level files) and arn:aws:states:us-east-1:643603452951:execution:mvm-demo-sfn-lease:parallel-sequential-20260921-011310 (sequential, sequential/)
- parallel: in-VM wall time is `lease accepted` -> `passed 4 step(s)` in vm-logs.txt; the payload's elapsed_s also counts the completer's boto3 import before `lease accepted` and is several seconds larger
- fanout-refused: refused before start: worst case 7200 VM-seconds exceeds policy max_vm_seconds 3000
- fanout-approved: plan exit 3 (needs approval): the machine's Gate handles approval, so run.py started it
- fanout-denied: plan exit 3 (needs approval): the machine's Gate handles approval, so run.py started it

microvm-ctl 0.3.0 from `/Users/vivekrajaps/Library/Python/3.9/lib/python/site-packages/microvm/__init__.py`; report generated 2026-09-21T01:15:38+00:00 by `python3 stepfunctions/run.py --report`.
