import boto3
from datetime import datetime

def lambda_handler(event, context):
    sagemaker_client = boto3.client('sagemaker')

    # Generate a timestamp in the format YYYYMMDD_HHMMSS
    timestamp = datetime.now().strftime('%Y%m%d-%H%M%S')

    # Create a unique Processing Job Name
    processing_job_name = f'scheduled-news-analyzer-{timestamp}'

    response = sagemaker_client.create_processing_job(
        ProcessingJobName=processing_job_name,
        ProcessingResources={
            'ClusterConfig': {
                'InstanceCount': 1,
                'InstanceType': 'ml.t3.medium',
                'VolumeSizeInGB': 2,
            }
        },
        AppSpecification={
            'ImageUri': '418272765834.dkr.ecr.us-east-1.amazonaws.com/news_analyzer_repo:latest'
        },
        RoleArn='arn:aws:iam::418272765834:role/service-role/AmazonSageMaker-ExecutionRole-20241203T212932',
        ProcessingInputs=[
            {
                'InputName': 'input',
                'S3Input': {
                    'S3Uri': 's3://news-scraped-data/csv/',
                    'LocalPath': '/opt/ml/processing/input',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File',
                }
            },
        ],
        ProcessingOutputConfig={
            'Outputs': [
                {
                    'OutputName': 'output',
                    'S3Output': {
                        'S3Uri': 's3://sentiment-analyzed-data/csv/',
                        'LocalPath': '/opt/ml/processing/output',
                        'S3UploadMode': 'EndOfJob',
                    }
                },
            ]
        },
    )

    return response
