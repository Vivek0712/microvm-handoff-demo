"""Time the lease handoff from the services' own clocks, never from a poll loop.

    AWS_PROFILE=heisenberg python3 benchmarks/bench.py \
        --sfn-arn arn:aws:states:...:stateMachine:mvm-demo-sfn-lease \
        --sfn-map-arn arn:aws:states:...:stateMachine:mvm-demo-sfn-map-lease \
        --durable-function mvm-demo-durable-orchestrator:live \
        --single-runs 5 --fanouts 4,8 --fanout-runs 2

For every run it records: the orchestrator's start and stop (Step Functions describe_execution
startDate/stopDate; durable get_durable_execution StartTimestamp/EndTimestamp), the VM ids the
output names, each VM's GetMicrovm startedAt -> terminatedAt (VM-seconds), and the cost at
microvm.lease.VM_USD_PER_GB_S for a 512 MiB image. One run at a time so the account's 1 RunMicrovm/s
and 8 GB quota are never contended by the bench itself. Writes results/bench.json, bench.svg and,
when headless Chrome is present, bench.png. `--summarize` re-renders from an existing bench.json.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import subprocess
import sys
import time
from datetime import datetime, timezone

import boto3

from microvm.client import microvm_client
from microvm.lease import VM_USD_PER_GB_S

HERE = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(HERE, "results")
CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
TASK = {"steps": ["uname -m", "python3 -c 'print(2+2)'", "sleep 2"]}
SHARD = {"steps": ["uname -m", "sleep 2"]}
TERMINAL_SFN = {"SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"}
TERMINAL_DURABLE = {"SUCCEEDED", "FAILED", "TIMED_OUT", "STOPPED"}
BASELINE_MIB = 512


def _epoch(v) -> float | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.timestamp()
    return float(v)


def _vm_ids(obj) -> list[str]:
    """Every microvm id named anywhere in an orchestrator output, in first-seen order."""
    found: list[str] = []

    def walk(o):
        if isinstance(o, dict):
            for k, v in o.items():
                if k in ("microvm_id", "microvmId") and isinstance(v, str) and v not in found:
                    found.append(v)
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(obj)
    return found


class Bench:
    def __init__(self, a: argparse.Namespace):
        self.a = a
        self.session = boto3.Session(profile_name=a.profile, region_name=a.region)
        self.sfn = self.session.client("stepfunctions")
        self.lam = self.session.client("lambda")
        self.vms = microvm_client(a.region, a.profile)
        self.stamp = time.strftime("%Y%m%d-%H%M%S")

    # -- start ------------------------------------------------------------------------------
    def start(self, kind: str, shards: int, i: int) -> dict:
        name = f"bench-{kind}-{shards or 'single'}-{self.stamp}-{i}"
        if kind == "sfn":
            arn = self.a.sfn_map_arn if shards else self.a.sfn_arn
            payload = {"shards": [SHARD] * shards} if shards else TASK  # the single machine reads $states.input as the task
            r = self.sfn.start_execution(stateMachineArn=arn, name=name, input=json.dumps(payload))
            return {"name": name, "arn": r["executionArn"]}
        payload = {"mode": "fanout", "shards": [SHARD] * shards} if shards else {"mode": "single", "task": TASK}
        r = self.lam.invoke(FunctionName=self.a.durable_function, InvocationType="Event",
                            Payload=json.dumps(payload).encode(), DurableExecutionName=name)
        return {"name": name, "arn": r.get("DurableExecutionArn")}

    # -- describe ---------------------------------------------------------------------------
    def describe(self, kind: str, h: dict) -> dict | None:
        if kind == "sfn":
            d = self.sfn.describe_execution(executionArn=h["arn"])
            if d["status"] not in TERMINAL_SFN:
                return None
            out = d.get("output")
            return {"status": d["status"], "start": _epoch(d["startDate"]), "stop": _epoch(d.get("stopDate")),
                    "output": json.loads(out) if out else None}
        if not h.get("arn"):
            fn = self.a.durable_function.split(":")[0]
            items = self.lam.list_durable_executions_by_function(
                FunctionName=fn, DurableExecutionName=h["name"]).get("DurableExecutions", [])
            if not items:
                return None
            h["arn"] = items[0]["DurableExecutionArn"]
        d = self.lam.get_durable_execution(DurableExecutionArn=h["arn"])
        if d["Status"] not in TERMINAL_DURABLE:
            return None
        res = d.get("Result")
        try:
            output = json.loads(res) if isinstance(res, str) else res
        except ValueError:
            output = res
        return {"status": d["Status"], "start": _epoch(d.get("StartTimestamp")),
                "stop": _epoch(d.get("EndTimestamp")), "output": output}

    def wait(self, kind: str, h: dict, timeout_s: int) -> dict:
        t0 = time.time()
        while time.time() - t0 < timeout_s:
            d = self.describe(kind, h)
            if d:
                return d
            time.sleep(2)
        raise TimeoutError(f"{h['name']} not terminal after {timeout_s}s")

    def vm_seconds(self, vm_ids: list[str]) -> list[dict]:
        rows = []
        for vid in vm_ids:
            for _ in range(15):  # terminatedAt lands a few seconds after the orchestrator stops
                m = self.vms.get_microvm(microvmIdentifier=vid)
                started, ended = _epoch(m.get("startedAt")), _epoch(m.get("terminatedAt"))
                if started and ended:
                    break
                time.sleep(2)
            rows.append({"id": vid, "state": m.get("state"), "startedAt": started, "terminatedAt": ended,
                         "seconds": round(ended - started, 3) if started and ended else None})
        return rows

    # -- one run ----------------------------------------------------------------------------
    def run(self, kind: str, shards: int, i: int) -> dict:
        h = self.start(kind, shards, i)
        d = self.wait(kind, h, timeout_s=600)
        vm_ids = _vm_ids(d["output"])
        vms = self.vm_seconds(vm_ids)
        vm_s = sum(v["seconds"] or 0 for v in vms)
        ok = d["status"] == "SUCCEEDED" and (len(vm_ids) == max(1, shards))
        if shards and kind == "durable" and isinstance(d["output"], dict):
            ok = ok and d["output"].get("succeeded") == shards
        row = {"kind": kind, "shards": shards, "name": h["name"], "arn": h["arn"], "status": d["status"], "ok": ok,
               "start": d["start"], "stop": d["stop"], "seconds": round(d["stop"] - d["start"], 3),
               "vms": vms, "vm_seconds": round(vm_s, 1),
               "usd": round(vm_s * BASELINE_MIB / 1024 * VM_USD_PER_GB_S, 6)}
        print(f"  {kind:8} {shards or 'single':>6}  {row['status']:9} {row['seconds']:7.1f}s  "
              f"vms={len(vm_ids)} vm-s={row['vm_seconds']:.0f} ${row['usd']:.5f}")
        return row


# -- report -------------------------------------------------------------------------------------
def summarize(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault((r["kind"], r["shards"]), []).append(r)
    out = []
    for (kind, shards), rs in sorted(groups.items(), key=lambda kv: (kv[0][1], kv[0][0])):
        ok = [r for r in rs if r["ok"]]
        secs = [r["seconds"] for r in ok] or [r["seconds"] for r in rs]
        out.append({"kind": kind, "shards": shards, "runs": len(rs), "ok": len(ok),
                    "p50_s": round(statistics.median(secs), 1), "max_s": round(max(secs), 1),
                    "vm_seconds_p50": round(statistics.median([r["vm_seconds"] for r in rs]), 0),
                    "usd_p50": round(statistics.median([r["usd"] for r in rs]), 5)})
    return out


def table(summary: list[dict]) -> str:
    lines = ["| orchestrator | shards | runs ok | p50 end to end | max | VM-s (p50) | USD per run (p50) |",
             "|---|---|---|---|---|---|---|"]
    for s in summary:
        lines.append(f"| {s['kind']} | {s['shards'] or 1} | {s['ok']}/{s['runs']} | {s['p50_s']} s | {s['max_s']} s | "
                     f"{s['vm_seconds_p50']:.0f} | ${s['usd_p50']:.5f} |")
    return "\n".join(lines)


def svg(summary: list[dict]) -> str:
    """Grouped bars: p50 end to end per shard count, one hue per orchestrator, labels on every bar."""
    hues = {"sfn": "#2f6fdd", "durable": "#d9822b"}
    sizes = sorted({s["shards"] for s in summary})
    kinds = [k for k in ("sfn", "durable") if any(s["kind"] == k for s in summary)]
    w, h, left, top, bottom = 760, 360, 70, 50, 70
    plot_w, plot_h = w - left - 30, h - top - bottom
    vmax = max(s["p50_s"] for s in summary) * 1.2 or 1
    group_w = plot_w / max(1, len(sizes))
    bar_w = min(70, group_w / (len(kinds) + 1))
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" font-family="Helvetica, Arial, sans-serif" font-size="13">',
             f'<rect width="{w}" height="{h}" fill="#ffffff"/>',
             f'<text x="{left}" y="24" font-size="16" font-weight="600" fill="#1c1c1c">Lease handoff, p50 end to end by the orchestrator\'s own clock</text>',
             f'<text x="{left}" y="42" fill="#5a5a5a">demo-agent 512 MiB, us-east-1, microvm-ctl 0.3.0</text>']
    for i in range(5):
        v = vmax * i / 4
        y = top + plot_h - plot_h * i / 4
        parts.append(f'<line x1="{left}" x2="{w - 30}" y1="{y:.1f}" y2="{y:.1f}" stroke="#e6e6e6"/>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" text-anchor="end" fill="#5a5a5a">{v:.0f}s</text>')
    for gi, size in enumerate(sizes):
        gx = left + gi * group_w + group_w / 2
        for ki, kind in enumerate(kinds):
            s = next((s for s in summary if s["kind"] == kind and s["shards"] == size), None)
            if not s:
                continue
            x = gx + (ki - (len(kinds) - 1) / 2) * (bar_w + 8) - bar_w / 2
            bh = plot_h * s["p50_s"] / vmax
            y = top + plot_h - bh
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w}" height="{bh:.1f}" rx="4" fill="{hues[kind]}"/>')
            parts.append(f'<text x="{x + bar_w / 2:.1f}" y="{y - 6:.1f}" text-anchor="middle" fill="#1c1c1c">{s["p50_s"]}s</text>')
        label = "single lease" if not size else f"fan-out of {size}"
        parts.append(f'<text x="{gx:.1f}" y="{top + plot_h + 20}" text-anchor="middle" fill="#1c1c1c">{label}</text>')
    lx = left
    for kind in kinds:
        parts.append(f'<rect x="{lx}" y="{h - 28}" width="14" height="14" rx="3" fill="{hues[kind]}"/>')
        name = "Step Functions" if kind == "sfn" else "Lambda durable function"
        parts.append(f'<text x="{lx + 20}" y="{h - 16}" fill="#1c1c1c">{name}</text>')
        lx += 200
    parts.append("</svg>")
    return "\n".join(parts)


def render_png(svg_path: str, png_path: str) -> bool:
    if not os.path.exists(CHROME):
        return False
    prof = os.path.join(OUT_DIR, ".chrome-profile")
    cmd = [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", f"--user-data-dir={prof}",
           "--window-size=1520,720", "--force-device-scale-factor=2", f"--screenshot={png_path}",
           "file://" + os.path.abspath(svg_path)]
    try:
        subprocess.run(cmd, capture_output=True, timeout=120)  # Chrome may exit non-zero after writing the file
    except subprocess.TimeoutExpired:
        pass
    finally:
        shutil.rmtree(prof, ignore_errors=True)
    return os.path.exists(png_path)


def write_outputs(rows: list[dict], meta: dict) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    summary = summarize(rows)
    with open(os.path.join(OUT_DIR, "bench.json"), "w") as f:
        json.dump({"meta": meta, "summary": summary, "runs": rows}, f, indent=2, default=str)
    with open(os.path.join(OUT_DIR, "bench.svg"), "w") as f:
        f.write(svg(summary))
    with open(os.path.join(OUT_DIR, "bench.md"), "w") as f:
        f.write(table(summary) + "\n")
    png = render_png(os.path.join(OUT_DIR, "bench.svg"), os.path.join(OUT_DIR, "bench.png"))
    print(table(summary))
    print(f"results -> {OUT_DIR}/bench.json, bench.svg, bench.md" + (", bench.png" if png else " (no png: Chrome missing)"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--region", default=os.environ.get("MVM_REGION", "us-east-1"))
    ap.add_argument("--sfn-arn")
    ap.add_argument("--sfn-map-arn")
    ap.add_argument("--durable-function")
    ap.add_argument("--kinds", default="sfn,durable")
    ap.add_argument("--single-runs", type=int, default=5)
    ap.add_argument("--fanouts", default="4,8")
    ap.add_argument("--fanout-runs", type=int, default=2)
    ap.add_argument("--summarize", action="store_true", help="re-render from results/bench.json, launch nothing")
    a = ap.parse_args()
    if a.summarize:
        with open(os.path.join(OUT_DIR, "bench.json")) as f:
            data = json.load(f)
        write_outputs(data["runs"], data["meta"])
        return 0
    kinds = [k for k in a.kinds.split(",") if k]
    if "sfn" in kinds and not (a.sfn_arn and a.sfn_map_arn):
        ap.error("--sfn-arn and --sfn-map-arn are required for sfn")
    if "durable" in kinds and not a.durable_function:
        ap.error("--durable-function is required for durable")
    b = Bench(a)
    rows: list[dict] = []
    meta = {"started": datetime.now(timezone.utc).isoformat(), "region": a.region, "image": "demo-agent",
            "baseline_mib": BASELINE_MIB, "usd_per_gb_s": VM_USD_PER_GB_S, "args": vars(a)}
    try:
        for kind in kinds:
            print(f"single lease x{a.single_runs} on {kind}")
            for i in range(a.single_runs):
                rows.append(b.run(kind, 0, i))
            for size in [int(s) for s in a.fanouts.split(",") if s]:
                print(f"fan-out of {size} x{a.fanout_runs} on {kind}")
                for i in range(a.fanout_runs):
                    rows.append(b.run(kind, size, i))
    finally:
        if rows:
            write_outputs(rows, meta)
    return 0 if all(r["ok"] for r in rows) else 1


if __name__ == "__main__":
    sys.exit(main())
