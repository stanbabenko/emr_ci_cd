"""
Copy paste the following code in your Lambda function. Make sure to change the following key parameters for the API as per your account

-Name (Name of Spark cluster)
-LogUri (S3 bucket to store EMR logs)
-Ec2SubnetId (The subnet to launch the cluster into)
-JobFlowRole (Service role for EC2)
-ServiceRole (Service role for Amazon EMR)

The following parameters are additional parameters for the Spark job itself. Change the bucket name and prefix for the Spark job (located at the bottom).

-s3://your-bucket-name/prefix/lambda-emr/SparkProfitCalc.jar (Spark jar file)
-s3://your-bucket-name/prefix/fake_sales_data.csv (Input data file in S3)
-s3://your-bucket-name/prefix/outputs/report_1/ (Output location in S3)
"""
import json
import boto3


client = boto3.client('emr')


def lambda_handler(event, context):
     response = client.run_job_flow(
        Name= 'SDN-DATA-GATHER',
        LogUri= 's3://emr-src/data/sdn/data_gather_logs/',
        ReleaseLabel= 'emr-6.0.0',
        Instances={
            'MasterInstanceType': 'm5.xlarge',
            'SlaveInstanceType': 'm5.large',
            'InstanceCount': 1,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-0417107eeda42e3ea'
        },
        Applications = [ {'Name': 'Spark'}, {'Name': 'Zeppelin'}, {'Name': 'Ganglia'} ],
        Configurations = [
            { 'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'}
            }
        ],
        VisibleToAllUsers=True,
        JobFlowRole = 'defaultEMRStack-EMREC2InstanceProfile-eEzN0mKaQfyW',
        ServiceRole = 'defaultEMRStack-EMRRole-3LJMSJOJWZ51',
        Steps=
        [
                {
                    "Name": "Setup Python Configuration",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                         "bash",
                         "-c",
                         "aws s3 cp s3://emr-src/configs/sdn-pull/setup_python.sh .; chmod +x setup_python.sh; ./setup_python.sh; rm setup_python.sh"
                     ]
                }
            },
            {
                'Name': 'Execute app.py',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit','s3://emr-src/configs/sdn-pull/app.py'
                    ]
                }
            }
        ],
    )