"""The `cli-none` scenario: lease demo-agent with no orchestrator, watch it, capture everything.

    AWS_PROFILE=heisenberg MVM_REGION=us-east-1 MVM_EXECUTION_ROLE_ARN=... python3 tools/capture_cli.py

Writes results/cli/lease-run-none.txt (the `mvm lease run ... --kind none --wait` transcript),
results/cli/watch.svg + watch.png (`mvm watch <id>` as it rendered, from a recording console),
results/cli/status.txt (`mvm status <id>` once the job is done), results/cli/ls.txt and
results/cli/logs.txt (`mvm logs demo-agent` for the VM), then terminates the VM and writes
results/cli/summary.json. Nothing here is typed in; every file is the command's own output.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

from rich.console import Console

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUT = os.path.join(ROOT, "results", "cli")
CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
TASK = {"steps": ["uname -m", "python3 -c 'print(2+2)'", "sleep 2", "echo done"], "workdir": "/tmp/job"}
MVM = [sys.executable, "-m", "microvm.cli"]


def run(cmd: list[str], path: str, *, env: dict | None = None, timeout: int = 600) -> subprocess.CompletedProcess:
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
    with open(path, "w") as f:
        f.write("$ " + " ".join(cmd).replace(sys.executable + " -m microvm.cli", "mvm") + "\n")
        f.write(r.stdout)
        if r.stderr.strip():
            f.write(r.stderr)
        f.write(f"exit {r.returncode}\n")
    return r


def png(svg_path: str, png_path: str) -> bool:
    if not os.path.exists(CHROME):
        return False
    prof = os.path.join(OUT, ".chrome-profile")
    cmd = [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--user-data-dir={prof}",
           "--window-size=1200,900", "--force-device-scale-factor=2", f"--screenshot={png_path}",
           "file://" + os.path.abspath(svg_path)]
    try:
        subprocess.run(cmd, capture_output=True, timeout=120)  # Chrome may exit non-zero after writing the file
    except subprocess.TimeoutExpired:
        pass
    finally:
        shutil.rmtree(prof, ignore_errors=True)
    return os.path.exists(png_path)


def vm_logs(image: str, vm_id: str, path: str) -> int:
    """The VM's own stream under /aws/lambda-microvms/<image>: the stream name ends with the VM id."""
    import boto3

    logs = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                         region_name=os.environ.get("MVM_REGION", "us-east-1")).client("logs")
    group = f"/aws/lambda-microvms/{image}"
    streams = logs.describe_log_streams(logGroupName=group, orderBy="LastEventTime", descending=True, limit=50)
    names = [st["logStreamName"] for st in streams.get("logStreams", []) if st["logStreamName"].endswith(vm_id)]
    lines = []
    for name in names:
        token = None
        while True:
            kw = {"nextToken": token} if token else {}
            page = logs.get_log_events(logGroupName=group, logStreamName=name, startFromHead=True, **kw)
            for e in page.get("events", []):
                ts = time.strftime("%H:%M:%S", time.gmtime(e["timestamp"] / 1000))
                lines.append(f"{ts}.{e['timestamp'] % 1000:03d}Z {e['message'].rstrip()}")
            if not page.get("nextForwardToken") or page["nextForwardToken"] == token:
                break
            token = page["nextForwardToken"]
    with open(path, "w") as f:
        f.write(f"# log group {group}, stream(s) {names or 'none found'}\n")
        f.write("\n".join(lines) + ("\n" if lines else ""))
    return len(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--image", default="demo-agent")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    env = dict(os.environ, COLUMNS="110", TERM="xterm-256color")
    started = time.time()

    # 1. launch with the CLI, kind none, and wait for RUNNING
    lease_id = f"cli-none-{time.strftime('%Y%m%d-%H%M%S')}"
    r = run(MVM + ["lease", "run", a.image, "--kind", "none", "--id", lease_id, "--task", json.dumps(TASK), "--wait"],
            os.path.join(OUT, "lease-run-none.txt"), env=env)
    vm_id = next((tok for tok in r.stdout.split() if tok.startswith("microvm-")), None)
    if r.returncode != 0 or not vm_id:
        print(r.stdout, r.stderr)
        return 1
    print(f"launched {vm_id}")

    # 2. what `mvm watch <id>` shows, on a recording console: every log line as it streams, then the
    #    final snapshot table (the live table redraws in place on a terminal; a recording keeps one copy)
    import microvm.cli as cli
    from microvm import PlaneConfig
    from microvm.endpoint import EndpointClient

    rec = Console(record=True, width=110, force_terminal=True, color_system="truecolor")
    rec.print(f"[bold]$ mvm watch {vm_id}[/]")
    client = EndpointClient(PlaneConfig(), vm_id)
    t0 = time.time()
    snap = client.status()
    for obj in client.watch(timeout=240):
        if not isinstance(obj, dict):
            continue
        if "msg" in obj:
            rec.print(cli._log_line(obj))
        else:
            snap = obj
    rec.print(cli._snapshot_table(snap, f"job on {vm_id}"))
    watch_s = round(time.time() - t0, 1)
    with open(os.path.join(OUT, "watch.txt"), "w") as f:
        f.write(rec.export_text(clear=False))
    rec.save_svg(os.path.join(OUT, "watch.svg"), title=f"mvm watch {vm_id}")
    has_png = png(os.path.join(OUT, "watch.svg"), os.path.join(OUT, "watch.png"))
    print(f"watched for {watch_s}s; svg saved, png={has_png}")

    # 3. the job's final status, the fleet listing, the VM's own log lines, then terminate
    run(MVM + ["status", vm_id], os.path.join(OUT, "status.txt"), env=env)
    run(MVM + ["ls", "--all"], os.path.join(OUT, "ls.txt"), env=env)
    status = snap  # the last snapshot the watch stream delivered: the job's final state
    run(MVM + ["terminate", vm_id], os.path.join(OUT, "terminate.txt"), env=env)
    time.sleep(30)  # log delivery lags the VM by a few seconds
    run(MVM + ["logs", a.image, "--minutes", "10"], os.path.join(OUT, "logs.txt"), env=env)
    vm_log_lines = vm_logs(a.image, vm_id, os.path.join(OUT, "vm-logs.txt"))
    print(f"{vm_log_lines} runtime log lines from the VM's stream")
    summary = {"scenario": "cli-none", "orchestrator": "cli", "lease_id": lease_id, "vm_id": vm_id,
               "watch_seconds": watch_s, "wall_seconds": round(time.time() - started, 1),
               "status": status or "see status.txt", "vm_log_lines": vm_log_lines, "files": sorted(os.listdir(OUT))}
    with open(os.path.join(OUT, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
