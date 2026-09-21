# Step Functions results

Every row is one live execution in account 643603452951 (us-east-1) driven by `stepfunctions/run.py`; the linked directory holds the input, the full history, the output, the VM's log lines, the machine's log lines, and `summary.json`. Seconds come from `describe_execution` startDate/stopDate. RunMicrovm calls are the `TaskScheduled` events for `runMicrovm` in the history (retries count). `matches expected` is the conjunction of the named checks in each `summary.json`. The `microvm-ctl` column is the library version the machine was generated from and run.py drove it with.

## First pass on microvm-ctl 0.3.0

The whole matrix ran once on 0.3.0. Two scenarios deviated from SPEC.md; their files are kept under `<scenario>/first-pass-0.3.0/` and the deviations are quoted verbatim from those summaries:

- **retryable-fail** (expected: sfn: Lease -> Catch -> Reap -> TerminateStale -> Failed with the typed error in the output)
  - deviation: the failed lease's VM was not terminated by the machine. TerminateStale only terminates members older than budget + slack (180 s) and this VM was ~10 s old, so the Map ran over an empty list and the VM stayed RUNNING until run.py's straggler check terminated it (SPEC: terminate everything you launch).
  - straggler: microvm-8a168ff7-d287-342d-934f-b16cd996fd70 was still RUNNING after the execution ended; run.py terminated it
- **hang** (expected: sfn: States.Timeout -> Reap terminates the VM -> Failed)
  - check `reap_terminated_the_vm` is false ([summary.json](hang/first-pass-0.3.0/summary.json))
  - deviation: Reap did not terminate the hung VM. TerminateStale only terminates members older than budget + slack (120 s here) and the VM was ~60 s old at States.Timeout, so the Map ran over an empty list; the VM kept running on its own MaximumDurationInSeconds cap until run.py's straggler check terminated it.
  - straggler: microvm-a1f16a50-2105-3030-aafb-63c45e2176cf was still RUNNING after the execution ended; run.py terminated it

First-pass rows for the scenarios that were rerun:

| scenario | microvm-ctl | status | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|---|
| [single](single/first-pass-0.3.0/) | 0.3.0 | SUCCEEDED | 4.166 | 1 | 1 | yes | Lease -> Terminate -> Done; peak 1 in flight |
| [retryable-fail](retryable-fail/first-pass-0.3.0/) | 0.3.0 | FAILED | 3.827 | 1 | 1 | yes | Lease -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |
| [hang](hang/first-pass-0.3.0/) | 0.3.0 | FAILED | 60.318 | 1 | 1 | NO | Lease -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |

## Second pass on microvm-ctl 0.3.1

0.3.1 adds `OnLeaseError` (Choice) and `TerminateFailed` between the Catch and `Reap`: a typed failure's cause names the VM and it is terminated at once; a timeout still carries no id and that VM is bounded by `MaximumDurationInSeconds`. The stacks were regenerated and redeployed and these scenarios were rerun; the other rows below are the first-pass results, which 0.3.1 does not change:

| scenario | microvm-ctl | status | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|---|
| [single](single/) | 0.3.1 | SUCCEEDED | 4.307 | 1 | 1 | yes | Lease -> Terminate -> Done; peak 1 in flight |
| [retryable-fail](retryable-fail/) | 0.3.1 | FAILED | 4.336 | 1 | 1 | yes | Lease -> OnLeaseError -> TerminateFailed -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |
| [hang](hang/) | 0.3.1 | FAILED | 60.349 | 1 | 1 | yes | Lease -> OnLeaseError -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |

## All scenarios (current files)

| scenario | microvm-ctl | status | seconds (service) | VMs | RunMicrovm calls | matches expected | what the history shows |
|---|---|---|---|---|---|---|---|
| [single](single/) | 0.3.1 | SUCCEEDED | 4.307 | 1 | 1 | yes | Lease -> Terminate -> Done; peak 1 in flight |
| [parallel](parallel/) | 0.3.0 | SUCCEEDED | 5.549 | 2 | 2 | yes | Lease -> Terminate -> Done; peak 1 in flight; in-VM wall 3.088 s parallel vs 12.113 s sequential |
| [retryable-fail](retryable-fail/) | 0.3.1 | FAILED | 4.336 | 1 | 1 | yes | Lease -> OnLeaseError -> TerminateFailed -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |
| [hang](hang/) | 0.3.1 | FAILED | 60.349 | 1 | 1 | yes | Lease -> OnLeaseError -> Reap -> TerminateStale -> Failed; error LeaseFailed; peak 1 in flight |
| [fanout-4](fanout-4/) | 0.3.0 | SUCCEEDED | 5.542 | 4 | 4 | yes | Gate -> Fanout -> Done (per shard: Done, Lease, ShardDone, Terminate); peak 4 in flight |
| [fanout-8](fanout-8/) | 0.3.0 | SUCCEEDED | 11.118 | 8 | 8 | yes | Gate -> Fanout -> Done (per shard: Done, Lease, ShardDone, Terminate); peak 4 in flight |
| [fanout-refused](fanout-refused/) | 0.3.0 | REFUSED | 0.0 | 0 | 0 | yes | no execution; `mvm lease plan --shards 40` exited 2: worst case 7200 VM-seconds exceeds policy max_vm_seconds 3000 |
| [fanout-approved](fanout-approved/) | 0.3.0 | SUCCEEDED | 17.283 | 12 | 12 | yes | Gate -> RequestApproval -> Fanout -> Done (per shard: Done, Lease, ShardDone, Terminate); peak 4 in flight; approver called SendTaskSuccess |
| [fanout-denied](fanout-denied/) | 0.3.0 | FAILED | 0.639 | 0 | 0 | yes | Gate -> RequestApproval -> Denied; error ApprovalDenied; approver called SendTaskFailure |

## Deviations from SPEC.md (current files)

None: every scenario matched its expected column.

## Notes

- parallel: two executions: arn:aws:states:us-east-1:643603452951:execution:mvm-demo-sfn-lease:parallel-20260921-011310 (parallel, top-level files) and arn:aws:states:us-east-1:643603452951:execution:mvm-demo-sfn-lease:parallel-sequential-20260921-011310 (sequential, sequential/)
- parallel: in-VM wall time is `lease accepted` -> `passed 4 step(s)` in vm-logs.txt; the payload's elapsed_s also counts the completer's boto3 import before `lease accepted` and is several seconds larger
- hang: microvm-ce0cb515-1378-37d7-b878-90ed30ce5487 was ended by the platform 122.03 s after start (neither the machine nor run.py terminated it)
- hang: microvm-ce0cb515-1378-37d7-b878-90ed30ce5487: startedAt 2026-09-21T01:32:25.432+00:00, terminatedAt 2026-09-21T01:34:27.462+00:00, lifetime 122.03 s against MaximumDurationInSeconds 120 (budget 60 + slack 60); terminated by platform (MaximumDurationInSeconds cap or idle policy)
- hang: microvm-ce0cb515-1378-37d7-b878-90ed30ce5487 was still RUNNING after the execution ended; run.py did not terminate it and polled GetMicrovm until the platform did
- fanout-refused: refused before start: worst case 7200 VM-seconds exceeds policy max_vm_seconds 3000
- fanout-approved: plan exit 3 (needs approval): the machine's Gate handles approval, so run.py started it
- fanout-denied: plan exit 3 (needs approval): the machine's Gate handles approval, so run.py started it

microvm-ctl 0.3.0, 0.3.1 from `/Users/vivekrajaps/Library/Python/3.9/lib/python/site-packages/microvm/__init__.py`; report generated 2026-09-21T01:34:37+00:00 by `python3 stepfunctions/run.py --report`.
