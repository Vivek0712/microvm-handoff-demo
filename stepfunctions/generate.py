"""Generate lease.asl.json, map.asl.json, template.yaml, and template-map.yaml for the demo.

    python3 generate.py            # microvm-ctl 0.3.0 from PyPI; no network

Every state comes from `microvm.integrations.stepfunctions.lease_state_machine` (the same
generator behind `mvm lease asl`); this script only substitutes CloudFormation placeholders
and wraps the definitions in templates. The SPEC policy is fixed here: budget 120 s,
heartbeat timeout 30 s (the VM heartbeats every 10 s), slack 60 s, max_concurrency 4,
max_vm_seconds 3000, approval_usd 0.015.

`lease.asl.json` is the single-lease machine as deployed (account and region of the demo).
`map.asl.json` is the fan-out machine: a Map over `$states.input.shards`, MaxConcurrency 4,
and a Gate that publishes to the approval topic with a task token above 8 shards.

`template.yaml` (stack mvm-demo-sfn) carries two machines, because TimeoutSeconds is fixed
at deploy time: `mvm-demo-sfn-lease` with `${Budget}` (120) and `mvm-demo-sfn-lease-short`
with `${ShortBudget}` (60) for the `hang` scenario. `template-map.yaml` (stack
mvm-demo-sfn-map) creates the SNS topic, the SQS queue subscribed with raw delivery, and
`mvm-demo-sfn-map-lease`. JSONata uses `{% %}` and `$name`, never `${`, so `Fn::Sub` leaves
it alone; the script checks that before writing.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path

from microvm.integrations.stepfunctions import RUN_WAIT, FanoutSpec, lease_state_machine
from microvm.lease import LeasePolicy

HERE = Path(__file__).parent
ACCOUNT = "643603452951"
REGION = "us-east-1"
IMAGE = "demo-agent"

# ---- the SPEC policy
BUDGET_S = 120
SHORT_BUDGET_S = 60  # the hang scenario: budget timeout while the VM keeps heartbeating
HEARTBEAT_TIMEOUT_S = 30
HEARTBEAT_EVERY_S = 10
SLACK_S = 60
MAX_CONCURRENCY = 4
MAX_VM_SECONDS = 3000
APPROVAL_USD = 0.015
APPROVE_ABOVE_SHARDS = 8

SUB_NAMES = {"ImageName", "AgentExecutionRole.Arn", "AWS::Region", "AWS::AccountId", "AWS::StackName",
             "Budget", "ShortBudget", "MaxConcurrency", "ApproveAboveShards", "ApprovalTopic"}
# integers no real machine would carry: swapped for ${MaxConcurrency} / ${ApproveAboveShards} after json.dumps
MAX_CONCURRENCY_SENTINEL = 987654301
APPROVE_ABOVE_SENTINEL = 987654302
CFN_IMAGE_ARN = "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:microvm-image:${ImageName}"
CFN_ROLE_ARN = "${AgentExecutionRole.Arn}"


def policy(budget: int = BUDGET_S) -> LeasePolicy:
    return LeasePolicy(budget_s=budget, heartbeat_timeout_s=HEARTBEAT_TIMEOUT_S, slack_s=SLACK_S,
                       max_concurrency=MAX_CONCURRENCY, max_vm_seconds=MAX_VM_SECONDS,
                       approval_usd=APPROVAL_USD)


def _machine(image_arn: str, role_arn: str, region: str, budget: int,
             fanout: FanoutSpec | None = None) -> dict:
    kw = {"fanout": fanout} if fanout is not None else {}
    return lease_state_machine(image_arn=image_arn, execution_role_arn=role_arn, region=region,
                               heartbeat_s=HEARTBEAT_EVERY_S, policy=policy(budget), **kw)


def _fanout(approval_topic_arn: str) -> FanoutSpec:
    return FanoutSpec(items_expr="$states.input.shards", max_concurrency=MAX_CONCURRENCY,
                      approval_topic_arn=approval_topic_arn, approve_above_shards=APPROVE_ABOVE_SHARDS)


def plain_asl() -> dict:
    """lease.asl.json: the deployed single-lease machine with concrete ARNs."""
    return _machine(f"arn:aws:lambda:{REGION}:{ACCOUNT}:microvm-image:{IMAGE}",
                    f"arn:aws:iam::{ACCOUNT}:role/mvm-demo-sfn-agent", REGION, BUDGET_S)


def map_asl() -> dict:
    """map.asl.json: the deployed fan-out machine with the approval gate."""
    return _machine(f"arn:aws:lambda:{REGION}:{ACCOUNT}:microvm-image:{IMAGE}",
                    f"arn:aws:iam::{ACCOUNT}:role/mvm-demo-sfn-map-agent", REGION, BUDGET_S,
                    fanout=_fanout(f"arn:aws:sns:{REGION}:{ACCOUNT}:mvm-demo-approvals"))


def _nodes(obj):
    """Every dict in the machine, depth first (a Map's item processor included)."""
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _nodes(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _nodes(v)


def _parameterise_budget(asl: dict, budget: int, param: str) -> dict:
    """Budget-derived numbers cannot be computed by Fn::Sub, so TimeoutSeconds and the idle
    cap take `${param}` literally and the two derived values become JSONata arithmetic."""
    asl = copy.deepcopy(asl)
    sentinel = f"__{param}__"
    stale = f"- {(budget + SLACK_S) * 1000}]]"
    hits = 0
    for node in _nodes(asl):
        if node.get("Resource") == RUN_WAIT:
            node["TimeoutSeconds"] = sentinel
            node["Arguments"]["IdlePolicy"]["MaxIdleDurationSeconds"] = sentinel
            node["Arguments"]["MaximumDurationInSeconds"] = f"{{% ${{{param}}} + {SLACK_S} %}}"
            hits += 1
        elif node.get("Type") == "Map" and stale in str(node.get("Items", "")):
            node["Items"] = node["Items"].replace(stale, f"- (${{{param}}} + {SLACK_S}) * 1000]]")
            hits += 1
    assert hits == 2, f"expected one lease state and one stale filter, patched {hits}"
    return asl, sentinel


def _definition(asl: dict, sentinel: str, param: str, swaps: dict[int, str] | None = None) -> str:
    """json.dumps with the sentinels swapped for `${Name}` and a check that nothing else
    looks like an Fn::Sub placeholder."""
    text = json.dumps(asl, indent=2).replace(f'"{sentinel}"', f"${{{param}}}")
    for literal, name in (swaps or {}).items():
        n = text.count(str(literal))
        if n != 1:
            raise SystemExit(f"sentinel {literal} for ${{{name}}} appears {n} times, expected once")
        text = text.replace(str(literal), f"${{{name}}}")
    unexpected = set(re.findall(r"\$\{([^}]*)\}", text)) - SUB_NAMES
    if unexpected:
        raise SystemExit(f"DefinitionString has ${{...}} that Fn::Sub would mangle: {sorted(unexpected)}")
    return text


def parameterised_asl(budget: int, param: str) -> str:
    """A single-lease DefinitionString body whose budget is the CloudFormation parameter `param`."""
    asl, sentinel = _parameterise_budget(
        _machine(CFN_IMAGE_ARN, CFN_ROLE_ARN, "${AWS::Region}", budget), budget, param)
    asl["Comment"] = (f"microvm-ctl lease of ${{ImageName}}: budget ${{{param}}}s, VM cap "
                      f"{param} + {SLACK_S}s (generated by generate.py, do not edit by hand)")
    return _definition(asl, sentinel, param)


def parameterised_map_asl() -> str:
    """The fan-out DefinitionString body: Map over $states.input.shards, `${MaxConcurrency}`,
    Gate -> RequestApproval on `${ApprovalTopic}` above `${ApproveAboveShards}` shards."""
    spec = FanoutSpec(items_expr="$states.input.shards", max_concurrency=MAX_CONCURRENCY_SENTINEL,
                      approval_topic_arn="${ApprovalTopic}", approve_above_shards=APPROVE_ABOVE_SENTINEL)
    asl, sentinel = _parameterise_budget(
        _machine(CFN_IMAGE_ARN, CFN_ROLE_ARN, "${AWS::Region}", BUDGET_S, fanout=spec), BUDGET_S, "Budget")
    asl["Comment"] = (f"microvm-ctl lease fan-out of ${{ImageName}}: ${{MaxConcurrency}} at a time, budget "
                      f"${{Budget}}s, VM cap Budget + {SLACK_S}s, approval above ${{ApproveAboveShards}} "
                      "shards (generated by generate.py, do not edit by hand)")
    swaps = {MAX_CONCURRENCY_SENTINEL: "MaxConcurrency", APPROVE_ABOVE_SENTINEL: "ApproveAboveShards"}
    return _definition(asl, sentinel, "Budget", swaps)


# The orchestrator role: microvm.integrations.stepfunctions.orchestrator_statements as YAML, plus the
# vended-log permissions Step Functions needs to write its own CloudWatch log group.
ORCHESTRATOR_STATEMENTS = """\
              - Effect: Allow
                Action: [lambda:RunMicrovm, lambda:TerminateMicrovm, lambda:ListMicrovms, lambda:GetMicrovm]
                Resource: "*"
              - Effect: Allow
                Action: iam:PassRole
                Resource: !GetAtt AgentExecutionRole.Arn
              - Effect: Allow
                Action: lambda:PassNetworkConnector
                Resource: arn:aws:lambda:*:aws:network-connector:aws-network-connector:*
              - Effect: Allow
                Action: [logs:CreateLogDelivery, logs:GetLogDelivery, logs:UpdateLogDelivery,
                         logs:DeleteLogDelivery, logs:ListLogDeliveries, logs:PutResourcePolicy,
                         logs:DescribeResourcePolicies, logs:DescribeLogGroups, logs:PutLogEvents,
                         logs:CreateLogStream]
                Resource: "*"
"""

AGENT_LOG_STATEMENT = """\
              - Effect: Allow
                Action: [logs:CreateLogGroup, logs:CreateLogStream, logs:PutLogEvents]
                Resource:
                  - !Sub arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda-microvms/*
                  - !Sub arn:aws:logs:${AWS::Region}:${AWS::AccountId}:log-group:/aws/lambda/microvms/*
"""

LOGGING = """\
      LoggingConfiguration:
        Level: ALL
        IncludeExecutionData: true
        Destinations:
          - CloudWatchLogsLogGroup:
              LogGroupArn: !GetAtt StateMachineLogs.Arn
"""

TEMPLATE = """\
AWSTemplateFormatVersion: '2010-09-09'
Description: >
  mvm-demo-sfn: Step Functions machines that lease the demo-agent Lambda MicroVM to one task
  with runMicrovm.waitForTaskToken and let the VM complete the task token itself. Two
  machines because TimeoutSeconds is fixed at deploy time: mvm-demo-sfn-lease (Budget) and
  mvm-demo-sfn-lease-short (ShortBudget, for the hang scenario). Generated by generate.py
  from microvm.integrations.stepfunctions (microvm-ctl 0.3.0); do not edit by hand.

Parameters:
  ImageName:
    Type: String
    Default: {image}
    Description: Name of the agent MicroVM image built with `mvm image build`.
  Budget:
    Type: Number
    Default: {budget}
    Description: Seconds mvm-demo-sfn-lease waits for the VM (TimeoutSeconds); the VM cap is Budget + {slack}.
  ShortBudget:
    Type: Number
    Default: {short_budget}
    Description: Seconds mvm-demo-sfn-lease-short waits for the VM; the VM cap is ShortBudget + {slack}.

Resources:
  # ---- the role the leased VM runs as: logs on both group prefixes, task token calls back to these machines
  AgentExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub ${{AWS::StackName}}-agent
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {{ Service: lambda.amazonaws.com }}
            Action: [sts:AssumeRole, sts:TagSession]
      Policies:
        - PolicyName: agent
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
{agent_logs}\
              - Sid: CompleteTheLease
                Effect: Allow
                Action: [states:SendTaskSuccess, states:SendTaskFailure, states:SendTaskHeartbeat]
                # the machines are named below so these ARNs are known before they exist (no circular ref)
                Resource:
                  - !Sub "arn:aws:states:${{AWS::Region}}:${{AWS::AccountId}}:stateMachine:\
${{AWS::StackName}}-lease"
                  - !Sub "arn:aws:states:${{AWS::Region}}:${{AWS::AccountId}}:stateMachine:\
${{AWS::StackName}}-lease-short"

  # ---- the role the state machines run as: launch, list, terminate, pass the agent role, write their logs
  StateMachineRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub ${{AWS::StackName}}-states
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {{ Service: states.amazonaws.com }}
            Action: sts:AssumeRole
      Policies:
        - PolicyName: orchestrator
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
{orchestrator}
  StateMachineLogs:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub /aws/vendedlogs/states/${{AWS::StackName}}
      RetentionInDays: 14

  # ---- Lease (waitForTaskToken) -> Terminate -> Done; failures -> Reap -> TerminateStale -> Failed
  StateMachine:
    Type: AWS::StepFunctions::StateMachine
    Properties:
      StateMachineName: !Sub ${{AWS::StackName}}-lease
      StateMachineType: STANDARD
      RoleArn: !GetAtt StateMachineRole.Arn
{logging}\
      DefinitionString: !Sub |
{definition}

  # ---- the same machine with ShortBudget: the hang scenario times out here
  ShortStateMachine:
    Type: AWS::StepFunctions::StateMachine
    Properties:
      StateMachineName: !Sub ${{AWS::StackName}}-lease-short
      StateMachineType: STANDARD
      RoleArn: !GetAtt StateMachineRole.Arn
{logging}\
      DefinitionString: !Sub |
{short_definition}

Outputs:
  StateMachineArn:
    Value: !Ref StateMachine
    Description: Start executions here with the task object as input; run.py reads it from the stack.
  ShortStateMachineArn:
    Value: !Ref ShortStateMachine
    Description: The ShortBudget machine, used by the hang scenario.
  AgentExecutionRoleArn:
    Value: !GetAtt AgentExecutionRole.Arn
    Description: Pass as MVM_EXECUTION_ROLE_ARN when leasing the agent image by hand.
  LogGroupName:
    Value: !Ref StateMachineLogs
    Description: Where both machines write their execution logs.
"""

TEMPLATE_MAP = """\
AWSTemplateFormatVersion: '2010-09-09'
Description: >
  mvm-demo-sfn-map: a Step Functions machine that leases one demo-agent Lambda MicroVM per
  element of the input's `shards` array with a Map over runMicrovm.waitForTaskToken, at most
  MaxConcurrency at a time, behind an SNS approval gate above ApproveAboveShards shards. The
  stack owns the approval topic and an SQS queue subscribed with raw delivery for the approver.
  Generated by generate.py from microvm.integrations.stepfunctions (microvm-ctl 0.3.0);
  do not edit by hand.

Parameters:
  ImageName:
    Type: String
    Default: {image}
    Description: Name of the agent MicroVM image built with `mvm image build`.
  Budget:
    Type: Number
    Default: {budget}
    Description: Seconds each shard's lease waits for its VM (TimeoutSeconds); the VM cap is Budget + {slack}.
  MaxConcurrency:
    Type: Number
    Default: {max_concurrency}
    MinValue: 1
    Description: Shards in flight at once (the policy's max_concurrency; `mvm lease plan` prints it).
  ApproveAboveShards:
    Type: Number
    Default: {approve_above}
    MinValue: 0
    Description: Executions with more shards than this publish to the approval topic and wait for a token.
  ApprovalTopicName:
    Type: String
    Default: mvm-demo-approvals
    Description: Name of the SNS topic the Gate publishes to.
  ApprovalQueueName:
    Type: String
    Default: mvm-demo-approvals-q
    Description: Name of the SQS queue subscribed to the topic; the approver reads task tokens here.

Resources:
  # ---- approval plumbing: RequestApproval publishes here; the approver reads the queue
  ApprovalTopic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: !Ref ApprovalTopicName

  ApprovalQueue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: !Ref ApprovalQueueName
      MessageRetentionPeriod: 3600
      ReceiveMessageWaitTimeSeconds: 10

  ApprovalQueuePolicy:
    Type: AWS::SQS::QueuePolicy
    Properties:
      Queues: [!Ref ApprovalQueue]
      PolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {{ Service: sns.amazonaws.com }}
            Action: sqs:SendMessage
            Resource: !GetAtt ApprovalQueue.Arn
            Condition:
              ArnEquals: {{ aws:SourceArn: !Ref ApprovalTopic }}

  ApprovalSubscription:
    Type: AWS::SNS::Subscription
    Properties:
      TopicArn: !Ref ApprovalTopic
      Protocol: sqs
      Endpoint: !GetAtt ApprovalQueue.Arn
      RawMessageDelivery: true

  # ---- the role the leased VM runs as: logs on both group prefixes, task token calls back to this machine
  AgentExecutionRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub ${{AWS::StackName}}-agent
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {{ Service: lambda.amazonaws.com }}
            Action: [sts:AssumeRole, sts:TagSession]
      Policies:
        - PolicyName: agent
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
{agent_logs}\
              - Sid: CompleteTheLease
                Effect: Allow
                Action: [states:SendTaskSuccess, states:SendTaskFailure, states:SendTaskHeartbeat]
                # the machine is named below so this ARN is known before it exists (no circular reference)
                Resource: !Sub "arn:aws:states:${{AWS::Region}}:${{AWS::AccountId}}:stateMachine:\
${{AWS::StackName}}-lease"

  # ---- the role the state machine runs as: launch, list, terminate, pass the agent role, publish the gate
  StateMachineRole:
    Type: AWS::IAM::Role
    Properties:
      RoleName: !Sub ${{AWS::StackName}}-states
      AssumeRolePolicyDocument:
        Version: '2012-10-17'
        Statement:
          - Effect: Allow
            Principal: {{ Service: states.amazonaws.com }}
            Action: sts:AssumeRole
      Policies:
        - PolicyName: orchestrator
          PolicyDocument:
            Version: '2012-10-17'
            Statement:
{orchestrator}\
              - Effect: Allow
                Action: sns:Publish
                Resource: !Ref ApprovalTopic

  StateMachineLogs:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub /aws/vendedlogs/states/${{AWS::StackName}}
      RetentionInDays: 14

  # ---- Gate -> RequestApproval -> Fanout (Map: Lease -> Terminate -> ShardDone per shard) -> Done
  StateMachine:
    Type: AWS::StepFunctions::StateMachine
    Properties:
      StateMachineName: !Sub ${{AWS::StackName}}-lease
      StateMachineType: STANDARD
      RoleArn: !GetAtt StateMachineRole.Arn
{logging}\
      DefinitionString: !Sub |
{definition}

Outputs:
  StateMachineArn:
    Value: !Ref StateMachine
    Description: 'Start executions here with {{"shards": [task, ...]}}; run.py reads it from the stack.'
  ApprovalTopicArn:
    Value: !Ref ApprovalTopic
    Description: RequestApproval publishes the shard count, image, execution, and task token here.
  ApprovalQueueUrl:
    Value: !Ref ApprovalQueue
    Description: The approver reads raw SNS messages here and calls SendTaskSuccess or SendTaskFailure.
  AgentExecutionRoleArn:
    Value: !GetAtt AgentExecutionRole.Arn
    Description: Pass as MVM_EXECUTION_ROLE_ARN when leasing the agent image by hand.
  LogGroupName:
    Value: !Ref StateMachineLogs
    Description: Where the machine writes its execution logs.
"""


def _indent(definition: str, spaces: int) -> str:
    pad = " " * spaces
    return "\n".join(f"{pad}{line}" if line else "" for line in definition.splitlines())


def main() -> None:
    common = {"image": IMAGE, "budget": BUDGET_S, "slack": SLACK_S, "agent_logs": AGENT_LOG_STATEMENT,
              "orchestrator": ORCHESTRATOR_STATEMENTS, "logging": LOGGING}
    (HERE / "lease.asl.json").write_text(json.dumps(plain_asl(), indent=2) + "\n")
    (HERE / "map.asl.json").write_text(json.dumps(map_asl(), indent=2) + "\n")
    (HERE / "template.yaml").write_text(TEMPLATE.format(
        short_budget=SHORT_BUDGET_S, definition=_indent(parameterised_asl(BUDGET_S, "Budget"), 8),
        short_definition=_indent(parameterised_asl(SHORT_BUDGET_S, "ShortBudget"), 8), **common))
    (HERE / "template-map.yaml").write_text(TEMPLATE_MAP.format(
        max_concurrency=MAX_CONCURRENCY, approve_above=APPROVE_ABOVE_SHARDS,
        definition=_indent(parameterised_map_asl(), 8), **common))
    print(f"wrote lease.asl.json, map.asl.json, template.yaml, template-map.yaml "
          f"(budget {BUDGET_S}s / short {SHORT_BUDGET_S}s, heartbeat timeout {HEARTBEAT_TIMEOUT_S}s every "
          f"{HEARTBEAT_EVERY_S}s, slack {SLACK_S}s, max_concurrency {MAX_CONCURRENCY}, "
          f"approval above {APPROVE_ABOVE_SHARDS} shards)")


if __name__ == "__main__":
    main()
