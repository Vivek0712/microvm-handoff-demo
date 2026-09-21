"""Approve or deny a durable fan-out that is waiting on the mvm-demo-durable-approvals queue.

    AWS_PROFILE=heisenberg python3 durable/approver.py --queue-url URL --decision approve --execution NAME
    AWS_PROFILE=heisenberg python3 durable/approver.py --queue-url URL --decision deny

`lease_map`'s approval step published {"callback_id", "plan", "execution"}; this reads the
queue, keeps the message for `--execution` (others are left for their owner and return to
the queue after the visibility timeout), and completes the callback with
SendDurableExecutionCallbackSuccess (the fan-out runs) or ...Failure (lease_map returns
`status: denied` and launches nothing). run.py imports `decide` for the two approval scenarios.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import boto3


def decide(queue_url: str, decision: str, *, execution: str | None = None, wait_s: float = 300,
           session: boto3.Session | None = None) -> list[dict]:
    """Complete the first matching approval request; returns what was handled (empty on timeout)."""
    if decision not in ("approve", "deny"):
        raise ValueError(f"decision must be approve or deny, got {decision!r}")
    session = session or boto3.Session()
    sqs, lam = session.client("sqs"), session.client("lambda")
    deadline = time.time() + wait_s
    handled: list[dict] = []
    while time.time() < deadline:
        wait = max(1, min(20, int(deadline - time.time())))
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=wait)
        for msg in resp.get("Messages", []):
            try:
                body = json.loads(msg["Body"])
            except ValueError:
                continue
            callback_id = body.get("callback_id")
            if not callback_id or (execution and body.get("execution") != execution):
                continue  # not ours: the visibility timeout hands it back to the queue
            plan = body.get("plan") or {}
            if decision == "approve":
                result = {"approved": True, "by": "approver.py", "plan_summary": plan.get("summary")}
                lam.send_durable_execution_callback_success(CallbackId=callback_id,
                                                            Result=json.dumps(result).encode())
            else:
                lam.send_durable_execution_callback_failure(
                    CallbackId=callback_id,
                    Error={"ErrorType": "ApprovalDenied",
                           "ErrorMessage": f"denied by approver.py: {plan.get('summary') or 'no summary'}",
                           "ErrorData": json.dumps({"approved": False, "by": "approver.py"})})
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])
            handled.append({"decision": decision, "callback_id": callback_id,
                            "execution": body.get("execution"), "plan_summary": plan.get("summary"),
                            "decided_at": time.time()})
            return handled
    return handled


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    ap.add_argument("--queue-url", required=True)
    ap.add_argument("--decision", choices=("approve", "deny"), required=True)
    ap.add_argument("--execution", help="only answer the request for this durable execution name")
    ap.add_argument("--wait", type=float, default=300, help="seconds to wait for a request (default 300)")
    args = ap.parse_args(argv)
    handled = decide(args.queue_url, args.decision, execution=args.execution, wait_s=args.wait)
    if not handled:
        print("no approval request arrived", file=sys.stderr)
        return 1
    for h in handled:
        print(json.dumps(h))
    return 0


if __name__ == "__main__":
    sys.exit(main())
