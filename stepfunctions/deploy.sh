#!/usr/bin/env bash
# Validate and deploy both Step Functions stacks with CloudFormation.
#
#   ./deploy.sh                 # regenerates the templates first, then deploys mvm-demo-sfn and mvm-demo-sfn-map
#   ./deploy.sh --no-generate   # deploy the committed templates as they are
#
# Profile heisenberg, us-east-1, CAPABILITY_NAMED_IAM (the roles carry fixed names). Idempotent:
# an unchanged stack is a no-op. Stack names are the SPEC's; nothing else is touched.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
export AWS_PROFILE="${AWS_PROFILE:-heisenberg}"
export AWS_REGION="${AWS_REGION:-us-east-1}"
export AWS_DEFAULT_REGION="$AWS_REGION"
IMAGE="${IMAGE:-demo-agent}"

if [ "${1:-}" != "--no-generate" ]; then
  (cd "$HERE" && python3 generate.py)
fi

for t in template.yaml template-map.yaml; do
  echo "validate $t"
  aws cloudformation validate-template --template-body "file://$HERE/$t" --query Description --output text
done

echo "deploy mvm-demo-sfn"
aws cloudformation deploy --stack-name mvm-demo-sfn --template-file "$HERE/template.yaml" \
  --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
  --parameter-overrides ImageName="$IMAGE" Budget=120 ShortBudget=60

echo "deploy mvm-demo-sfn-map"
aws cloudformation deploy --stack-name mvm-demo-sfn-map --template-file "$HERE/template-map.yaml" \
  --capabilities CAPABILITY_NAMED_IAM --no-fail-on-empty-changeset \
  --parameter-overrides ImageName="$IMAGE" Budget=120 MaxConcurrency=4 ApproveAboveShards=8

for s in mvm-demo-sfn mvm-demo-sfn-map; do
  echo "--- $s outputs"
  aws cloudformation describe-stacks --stack-name "$s" --query 'Stacks[0].Outputs[].[OutputKey,OutputValue]' --output text
done
