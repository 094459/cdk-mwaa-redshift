from aws_cdk import (
    aws_iam as iam,
    aws_ec2 as ec2,
    aws_s3 as s3,
    aws_redshift_alpha as redshift,
    Stack,
    CfnOutput,
    RemovalPolicy
)
from constructs import Construct

class MwaaRedshiftStack(Stack):

    def __init__(self, scope: Construct, id: str, vpc, mwaa_props, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # create s3 bucket that redshift will use. if this bucket exists
        # this cdk app will fail, so ensure this has not been created yet

        redshift_bucket = s3.Bucket(
            self,
            "mwaa-redshift import",
            bucket_name=f"{mwaa_props['redshifts3location'].lower()}",
            versioned=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL
        )

        redshift_bucket_arn = redshift_bucket.bucket_arn

        # get arn of dags bucket - not sure if this is needed so may remove
        
        dags_bucket = s3.Bucket.from_bucket_name(self, "mwaa-dag-bucket", f"{mwaa_props['mwaadag'].lower()}")
        dags_bucket_arn = dags_bucket.bucket_arn

        # create redshift secret and redshift user

        # create redshift iam role/policy that we will attach to the RedShift cluster
        # that has the right level of access to a specific S3 bucket
        # you can further lockdown this policy by just specifying s3 actions.

        mwaa_redshift_policy_document = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "s3:*"
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=[
                        f"{redshift_bucket_arn}/*",
                        f"{redshift_bucket_arn}",
                        f"{dags_bucket_arn}/*",
                        f"{dags_bucket_arn}",
                        ]
                )
            ]
        )

        mwaa_redshift_service_role = iam.Role(
            self,
            "mwaa-redshift-service-role2nd",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
            inline_policies={"mwaaRedshiftPolicyDocument": mwaa_redshift_policy_document}
        )

        mwaa_redshift_service_role_arn = mwaa_redshift_service_role.role_arn

        # Setup Security Group

        default_redshift_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "MWAARedshiftSG",
            security_group_id=vpc.vpc_default_security_group
            )

        default_redshift_security_group.add_ingress_rule(
            peer=default_redshift_security_group,
            connection=ec2.Port.tcp(5439)
            )

        # Modify MWAA security group to enable Redshift access

        mwaa_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            "SG",
            mwaa_props['mwaa-sg']
            #mutable=False
            )
        mwaa_security_group.add_ingress_rule(ec2.Peer.any_ipv4(), ec2.Port.tcp(5439), "allow redshift access")
        

        # create subnet groups - one for RedShift and one for the VPE we will create
        # the VPE subnet will take in parameters we provide that are the subnet-ids
        # of the VPC where MWAA is deployed

        redshift_cluster_subnet_group = redshift.ClusterSubnetGroup(
            self,
            "RedshiftCSG",
            vpc = vpc,
            vpc_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            description="Redshift Cluster Subnet Group"
        )

        ## get all the subnet ids from the MWAA VPC

        #mwaavpc = ec2.Vpc.from_lookup(
        #    self,
        #    "MWAA VPC",
            # if you have deployed in a different VPC then use this instead
            # vpc_id=props['mwaa-vpc-id']
        #    vpc_id=vpc
        #)

        vpe_redshift_cluster_subnet_group = redshift.ClusterSubnetGroup(
            self,
            "MWAAVPERedshiftCSG",
            #vpc = mwaavpc,
            vpc = vpc,
            description="MWAA VPE Redshift Cluster Subnet Group"
        )

        redshiftclustername = f"{mwaa_props['redshiftclustername'].lower()}"

        cluster = redshift.Cluster(
            self,
            "MWAARedshiftCluster",
             master_user=redshift.Login(
                master_username=mwaa_props['redshiftusername']
            ),
            vpc = vpc,
            security_groups=[default_redshift_security_group],
            node_type=redshift.NodeType.RA3_4XLARGE,
            number_of_nodes=2,
            cluster_name=redshiftclustername,
            default_database_name=mwaa_props['redshiftdb'],
            removal_policy=RemovalPolicy.DESTROY,
            roles=[mwaa_redshift_service_role],
            publicly_accessible=False,
            subnet_group=redshift_cluster_subnet_group
        )

        redshift_secret_arn = cluster.secret.secret_arn
        

        # Display some useful output

        CfnOutput(
            self,
            id="RedshiftSecretARN :",
            value=redshift_secret_arn,
            description="This is the Redshift Secret ARN"
        )

        CfnOutput(
            self,
            id="RedshiftIAMARN :",
            value=mwaa_redshift_service_role_arn,
            description="This is the Redshift IAM ARN"
        )

        CfnOutput(
            self,
            id="RedshiftClusterEndpoint :",
            value=cluster.cluster_endpoint.hostname,
            description="This is the Redshift Cluster Endpoint"
        )
        CfnOutput(
            self,
            id="MWAAVPCESG :",
            value=vpe_redshift_cluster_subnet_group.cluster_subnet_group_name,
            description="This is the VPE Subnet Group to use when creating the VPC Endpoint"
        )
        CfnOutput(
            self,
            id="redshiftvpcendpointcli",
            value="aws redshift create-endpoint-access --cluster-identifier "+redshiftclustername+" --resource-owner "+self.account+ " --endpoint-name mwaa-redshift-endpoint --subnet-group-name "+vpe_redshift_cluster_subnet_group.cluster_subnet_group_name+" --vpc-security-group-ids "+mwaa_props['mwaa-sg'],
            description="Use this command to create your vpce"
        )        
