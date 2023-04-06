from aws_cdk import (
    aws_s3 as s3,
    aws_s3_deployment as s3deploy,
    Tags,
    Stack,
    CfnOutput
)
import boto3

from constructs import Construct

class MwaaCdkStackUtils(Stack):

    def __init__(self, scope: Construct, id: str, mwaa_props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        tags = {
            'env': f"{mwaa_props['mwaa_env']}-dev",
            'service': 'MWAA Apache AirFlow'
        }
        
        dags_bucket = s3.Bucket(
                self,
                "mwaa-dags",
                bucket_name=f"{mwaa_props['dagss3location']}-dev",
                versioned=True,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL
            )

        for tag in tags:
                Tags.of(dags_bucket).add(tag, tags[tag])

        s3deploy.BucketDeployment(self, "DeployDAG",
            sources=[s3deploy.Source.asset("./dags")],
            destination_bucket=dags_bucket,
            destination_key_prefix="dags",
            prune=False,
            retain_on_delete=False
            )
    
        s3deploy.BucketDeployment(self, "DeployRequirements",
                sources=[s3deploy.Source.asset("./requirements")],
                destination_bucket=dags_bucket,
                destination_key_prefix="requirements",
                prune=False,
                retain_on_delete=False
            )

        dags_bucket_arn = dags_bucket.bucket_arn

        CfnOutput(
            self,
            id="S3Bucket",
            value=dags_bucket_arn,
            description="S3 Bucket Arn used by Apache Airflow EKS"
        )