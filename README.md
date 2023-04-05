### How to deploy

1. Edit the app.py file to suit your AWS account and update the mwaa_props values to your own (make sure to set a unique S3 bucket)
2. Deploy the CDK stacks using the following command:

```
cdk deploy mwaa-demo-vpc
cdk deploy mwaa-demo-dev-environment
```

Update the app.py to set the "mwaa-sg" and "mwaa-vpc-id" values. I will fix this soon, just did not want to tie these too closely together for this quick demo

```
cdk deploy mwaa-demo-redshift
```

The deployment will take around 25-30 minutes to complete.

**Updated Providers**

Use the updated constraints file (deploy to DAGS folder) and the requirements.txt file which will configurare MWAA to run the Amazon Provider 7.3.0 package.

**To uninstall**

You will need to manually delete the S3 bucket that was created for your DAGs, and then use CDK to uninstall

```
cdk destroy mwaa-demo-redshift
cdk destroy mwaa-demo-dev-environment
cdk destroy mwaa-demo-vpc
```