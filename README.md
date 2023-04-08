### Amazon Worflows for Apache Workflow and Amazon Redshift

This repo contains example code to go with [this blog post](https://dev.to/aws/working-with-managed-workflows-for-apache-airflow-mwaa-and-amazon-redshift-40nd)


### How to deploy

1. Edit the app.py file to suit your AWS account and update the mwaa_props values to your own (make sure to set a unique S3 bucket)
2. Deploy the CDK stacks using the following command:

```
cdk deploy mwaa-demo-utils
cdk deploy mwaa-demo-vpc
cdk deploy mwaa-demo-dev-environment
cdk deploy mwaa-demo-redshift
```

The deployment will take around 25-30 minutes to complete. You will end up with a MWAA environment running 2.4.3 and with updated Amazon Providers (see below), and an Amazon Redshift cluster. Amazon S3 buckets will be created, and the contents of the repo DAGS folder will be uploaded.

To use the sample Redshift data, please follow the instructions here - https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-create-sample-db.html

**Updated Providers**

Use the updated constraints file (deploy to DAGS folder) and the requirements.txt file which will configurare MWAA to run the Amazon Provider 7.3.0 package.

**To uninstall**

To remove all the resources deployed, use the following

```
cdk destroy mwaa-demo-redshift
cdk destroy mwaa-demo-dev-environment
cdk destroy mwaa-demo-vpc
```

You will need to manually delete the S3 buckets.