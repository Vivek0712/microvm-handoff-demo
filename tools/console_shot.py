"""Screenshot an AWS console page as the current profile, with no browser login.

    python3 tools/console_shot.py --profile heisenberg --out results/screenshots/x.png \
        "https://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/v2/executions/details/<arn>"

Flow: sts.get_federation_token (read-only policy) -> signin.aws.amazon.com/federation
getSigninToken -> a login URL that redirects to the destination -> headless Chrome
--screenshot. Nothing is written but the PNG; the temporary credentials live only in this
process. The sign-in token is single use and expires in minutes.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("destination")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE", "default"))
    ap.add_argument("--out", required=True)
    ap.add_argument("--width", type=int, default=1600)
    ap.add_argument("--height", type=int, default=1000)
    ap.add_argument("--wait-ms", type=int, default=45000, help="virtual time budget for the SPA to settle")
    a = ap.parse_args()
    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    shoot(login_url(a.profile, a.destination), a.out, width=a.width, height=a.height, wait_ms=a.wait_ms)
    size = os.path.getsize(a.out)
    print(f"{a.out} ({size} bytes)")
    return 0 if size > 10_000 else 1


if __name__ == "__main__":
    sys.exit(main())
