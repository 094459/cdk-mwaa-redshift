# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#!/usr/bin/env python3

import aws_cdk as cdk 

from mwaa_cdk.mwaa_cdk_vpc import MwaaCdkStackVPC
from mwaa_cdk.mwaa_cdk_dev_env import MwaaCdkStackDevEnv
from mwaa_cdk.mwaa_cdk_redshift import MwaaRedshiftStack

env_EU=cdk.Environment(region="eu-west-1", account="XXXXXXX")
mwaa_props = {
    'dagss3location': 'mwaa-094459-243-demo',
    'mwaa_env' : 'mwaa-243-demo',
    'mwaa_secrets_var':'airflow/variables',
    'mwaa_secrets_conn':'airflow/connections',
    'redshifts3location': 'mwaa-094459-redshift',
    'mwaadag' : 'mwaa-094459-243-demo',
    'mwaa-sg':'sg-0c4c3a8b6ff9f1f4d',
    'mwaa-vpc-id':'vpc-037ca384596917690',
    'redshiftclustername':'mwaa-redshift-cluster',
    'redshiftdb':'mwaa',
    'redshiftusername':'awsuser'
    }

app = cdk.App()

mwaa_demo_vpc = MwaaCdkStackVPC(
    scope=app,
    id="mwaa-demo-vpc",
    env=env_EU,
    mwaa_props=mwaa_props
)

mwaa_demo_env_dev = MwaaCdkStackDevEnv(
    scope=app,
    id="mwaa-demo-dev-environment",
    vpc=mwaa_demo_vpc.vpc,
    env=env_EU,
    mwaa_props=mwaa_props
)

mwaa_demo_redshift = MwaaRedshiftStack(
    scope=app,
    id="mwaa-demo-redshift",
    vpc=mwaa_demo_vpc.vpc,
    env=env_EU,
    mwaa_props=mwaa_props
)

app.synth()
