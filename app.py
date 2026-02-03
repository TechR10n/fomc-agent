#!/usr/bin/env python3
"""CDK entry point for FOMC data pipeline."""

import aws_cdk as cdk

from infra.config import get_env_config
from infra.stacks.storage_stack import FomcStorageStack
from infra.stacks.compute_stack import FomcComputeStack
from infra.stacks.messaging_stack import FomcMessagingStack
from infra.stacks.site_stack import FomcSiteStack

app = cdk.App()
config = get_env_config()

env = cdk.Environment(account=config["account"], region=config["region"])

storage = FomcStorageStack(app, "FomcStorageStack", env=env)
compute = FomcComputeStack(app, "FomcComputeStack", storage=storage, env=env)
messaging = FomcMessagingStack(app, "FomcMessagingStack", storage=storage, env=env)
site = FomcSiteStack(app, "FomcSiteStack", env=env)

app.synth()
