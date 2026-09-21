"""Screenshot an AWS console page as the current profile, with no browser login.

    python3 tools/console_shot.py --profile heisenberg --out results/screenshots/x.png \
        "https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/<arn>"

Flow: sts.get_federation_token (read-only policy) -> signin.aws.amazon.com/federation
getSigninToken -> a login URL that redirects to the destination -> headless Chrome
--screenshot. Nothing is written but the PNG; the temporary credentials live only in this
process. The sign-in token is single use and expires in minutes.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

import boto3

CHROME = os.environ.get("CHROME", "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
READ_POLICY = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": ["states:Describe*", "states:Get*", "states:List*", "lambda:Get*", "lambda:List*",
                   "lambda:Describe*", "logs:Describe*", "logs:Get*", "logs:FilterLogEvents",
                   "logs:StartQuery", "logs:StopQuery", "logs:StartLiveTail", "cloudwatch:Describe*",
                   "cloudwatch:Get*", "cloudwatch:List*", "sns:Get*", "sns:List*", "sqs:Get*", "sqs:List*",
                   "iam:ListRoles", "iam:GetRole", "tag:GetResources", "cloudformation:Describe*",
                   "cloudformation:List*", "cloudformation:Get*", "xray:Get*", "xray:BatchGet*"],
        "Resource": "*",
    }],
}


def login_url(profile: str, destination: str, duration_s: int = 3600) -> str:
    sts = boto3.Session(profile_name=profile).client("sts")
    creds = sts.get_federation_token(Name="console-shot", Policy=json.dumps(READ_POLICY),
                                     DurationSeconds=duration_s)["Credentials"]
    session = json.dumps({"sessionId": creds["AccessKeyId"], "sessionKey": creds["SecretAccessKey"],
                          "sessionToken": creds["SessionToken"]})
    q = urllib.parse.urlencode({"Action": "getSigninToken", "SessionDuration": str(duration_s),
                                "Session": session})
    with urllib.request.urlopen("https://signin.aws.amazon.com/federation?" + q, timeout=30) as r:
        token = json.load(r)["SigninToken"]
    q = urllib.parse.urlencode({"Action": "login", "Issuer": "microvm-handoff-demo",
                                "Destination": destination, "SigninToken": token})
    return "https://signin.aws.amazon.com/federation?" + q


def shoot(url: str, out: str, *, width: int, height: int, wait_ms: int) -> None:
    profile_dir = tempfile.mkdtemp(prefix="console-shot-")
    try:
        cmd = [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
               f"--user-data-dir={profile_dir}", f"--window-size={width},{height}",
               f"--virtual-time-budget={wait_ms}", f"--screenshot={out}", url]
        subprocess.run(cmd, check=True, capture_output=True, timeout=180)
    finally:
        shutil.rmtree(profile_dir, ignore_errors=True)


# ---- driving the SPA over the DevTools protocol (for pages that need a click first)
def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _Page:
    """A minimal DevTools client: navigate, evaluate JavaScript, screenshot."""

    def __init__(self, ws_url: str):
        import websocket  # websocket-client

        self.ws = websocket.create_connection(ws_url, suppress_origin=True, timeout=120)
        self.n = 0

    def call(self, method: str, **params):
        self.n += 1
        self.ws.send(json.dumps({"id": self.n, "method": method, "params": params}))
        while True:
            msg = json.loads(self.ws.recv())
            if msg.get("id") == self.n:
                if "error" in msg:
                    raise RuntimeError(f"{method}: {msg['error']}")
                return msg.get("result", {})

    def js(self, expr: str):
        r = self.call("Runtime.evaluate", expression=expr, returnByValue=True, awaitPromise=True)
        return r.get("result", {}).get("value")


CLICK_JS = """(() => {
  const want = %s;
  const nodes = [...document.querySelectorAll('a, button, [role=tab], [role=button], span, div, td')];
  const hit = nodes.find(n => n.children.length === 0 && n.textContent.trim() === want)
           || nodes.find(n => n.textContent.trim() === want);
  if (!hit) return 'not found: ' + want;
  (hit.closest('a, button, [role=tab], [role=button], tr') || hit).click();
  return 'clicked: ' + want;
})()"""


SCROLL_JS = """(() => {
  const want = %s;
  const hit = [...document.querySelectorAll('h1, h2, h3, h4, legend, label, span, div')]
    .find(n => n.textContent.trim().startsWith(want));
  if (!hit) return 'not found: ' + want;
  hit.scrollIntoView({block: 'start'});
  return 'scrolled to: ' + want;
})()"""


def shoot_with_clicks(url: str, out: str, *, width: int, height: int, wait_s: float, clicks: list[str],
                      scroll_to: str | None = None) -> None:
    port = _free_port()
    profile_dir = tempfile.mkdtemp(prefix="console-shot-")
    cmd = [CHROME, "--headless=new", "--disable-gpu", "--hide-scrollbars", "--no-first-run",
           f"--user-data-dir={profile_dir}", f"--window-size={width},{height}",
           f"--remote-debugging-port={port}", "about:blank"]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(50):
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/json", timeout=2) as r:
                    targets = json.load(r)
                break
            except Exception:
                time.sleep(0.2)
        page = _Page(next(t["webSocketDebuggerUrl"] for t in targets if t["type"] == "page"))
        page.call("Page.enable")
        page.call("Emulation.setDeviceMetricsOverride", width=width, height=height, deviceScaleFactor=1, mobile=False)
        page.call("Page.navigate", url=url)
        time.sleep(wait_s)
        for text in clicks:
            print("  ", page.js(CLICK_JS % json.dumps(text)))
            time.sleep(wait_s)
        if scroll_to:
            print("  ", page.js(SCROLL_JS % json.dumps(scroll_to)))
            time.sleep(1)
        data = page.call("Page.captureScreenshot", format="png")["data"]
        with open(out, "wb") as f:
            f.write(base64.b64decode(data))
    finally:
        proc.terminate()
        shutil.rmtree(profile_dir, ignore_errors=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("destination")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE", "default"))
    ap.add_argument("--out", required=True)
    ap.add_argument("--width", type=int, default=1600)
    ap.add_argument("--height", type=int, default=1000)
    ap.add_argument("--wait-ms", type=int, default=45000, help="virtual time budget for the SPA to settle")
    ap.add_argument("--click", action="append", default=[], metavar="TEXT",
                    help="after the page settles, click the element with exactly this text (repeatable, in order); "
                         "drives a real Chrome over DevTools instead of the one-shot --screenshot")
    ap.add_argument("--wait-s", type=float, default=12.0, help="seconds to wait after navigation and after each click")
    a = ap.parse_args()
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    if a.click:
        shoot_with_clicks(login_url(a.profile, a.destination), a.out, width=a.width, height=a.height,
                          wait_s=a.wait_s, clicks=a.click)
    else:
        shoot(login_url(a.profile, a.destination), a.out, width=a.width, height=a.height, wait_ms=a.wait_ms)
    size = os.path.getsize(a.out)
    print(f"{a.out} ({size} bytes)")
    return 0 if size > 10_000 else 1


if __name__ == "__main__":
    sys.exit(main())
