#!/usr/bin/env bash
# Build and deploy the mvm-demo-durable stack. Nothing local goes into the function: the
# requirements pin microvm-ctl[durable]>=0.3.1 from PyPI, built with Python 3.13 (SAM CLI 1.166).
set -euo pipefail
cd "$(dirname "$0")"

DVENV_BIN="${DVENV_BIN:-/private/tmp/claude-501/-Users-vivekrajaps-microvm-ctl/f2158993-27f3-4ba8-a3d9-31b91885a0a7/scratchpad/dvenv/bin}"
[ -d "$DVENV_BIN" ] && export PATH="$DVENV_BIN:$PATH"
export PIP_NO_CACHE_DIR=1
export AWS_PROFILE="${AWS_PROFILE:-heisenberg}"
export AWS_REGION="${AWS_REGION:-us-east-1}" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
STACK="${STACK:-mvm-demo-durable}"
IMAGE_NAME="${IMAGE_NAME:-demo-agent}"

python3 --version
sam --version
sam validate --lint
sam build
sam deploy --stack-name "$STACK" --resolve-s3 --no-confirm-changeset --no-fail-on-empty-changeset \
  --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
  --parameter-overrides "ImageName=$IMAGE_NAME"
aws cloudformation describe-stacks --stack-name "$STACK" \
  --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output table
