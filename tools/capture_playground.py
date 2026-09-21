"""Screenshots of the playground's fleet job panel while a real fan-out runs.

    AWS_PROFILE=heisenberg python3 tools/capture_playground.py --sfn-map-arn <arn>

Starts `mvm playground` on a local port (dry run off, the profile from the environment), starts a
4-shard execution on the Map state machine, and screenshots the fleet job panel twice while the
shards run, then once more after the execution ends; also the lease form. Then stops the server.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import boto3

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "results", "screenshots")
CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
SHARD = {"steps": ["uname -m", "sleep 45", "echo shard done"]}  # long enough to be watched


def shot(url: str, out: str, wait_s: float = 8.0, scroll_to: str | None = None) -> None:
    """Drive a real Chrome over DevTools: the playground polls and streams, so it never goes
    idle for Chrome's one-shot --screenshot."""
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from console_shot import shoot_with_clicks

    shoot_with_clicks(url, out, width=1500, height=1100, wait_s=wait_s, clicks=[], scroll_to=scroll_to)
    print(f"  {out} ({os.path.getsize(out)} bytes)" if os.path.exists(out) else f"  {out} MISSING")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sfn-map-arn", required=True)
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--shards", type=int, default=4)
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    base = f"http://127.0.0.1:{a.port}"
    server = subprocess.Popen([sys.executable, "-m", "microvm.cli", "playground", "--port", str(a.port), "--no-open"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        time.sleep(4)
        shot(f"{base}/#call", os.path.join(OUT, "playground-lease-form.png"), scroll_to="Lease a VM one task")
        sfn = boto3.Session(profile_name=os.environ.get("AWS_PROFILE")).client("stepfunctions", region_name="us-east-1")
        name = f"playground-fanout-{a.shards}-{time.strftime('%Y%m%d-%H%M%S')}"
        arn = sfn.start_execution(stateMachineArn=a.sfn_map_arn, name=name,
                                  input=json.dumps({"shards": [SHARD] * a.shards}))["executionArn"]
        print("started", arn)
        time.sleep(8)
        shot(f"{base}/#fleet?fleetjob=demo-agent", os.path.join(OUT, "playground-fleet-jobs-running.png"), wait_s=22)
        shot(f"{base}/#fleet?fleetjob=demo-agent", os.path.join(OUT, "playground-fleet-jobs-later.png"), wait_s=22)
        while sfn.describe_execution(executionArn=arn)["status"] == "RUNNING":
            time.sleep(3)
        d = sfn.describe_execution(executionArn=arn)
        print("execution", d["status"], round((d["stopDate"] - d["startDate"]).total_seconds(), 1), "s")
        shot(f"{base}/#fleet", os.path.join(OUT, "playground-fleet-after.png"))
        shot(f"{base}/#trace", os.path.join(OUT, "playground-api-trace.png"))
        with open(os.path.join(OUT, "playground-capture.json"), "w") as f:
            json.dump({"execution_arn": arn, "status": d["status"],
                       "seconds": round((d["stopDate"] - d["startDate"]).total_seconds(), 3)}, f, indent=2)
    finally:
        server.terminate()
    return 0


if __name__ == "__main__":
    sys.exit(main())
