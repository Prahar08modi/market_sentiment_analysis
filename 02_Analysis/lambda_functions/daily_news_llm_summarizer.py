import boto3
import pandas as pd
import json
from io import StringIO
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    print(event)
    print(context)

    # Extract bucket name and object key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Read the CSV file from S3
    try:
        csv_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        csv_content = csv_obj['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Error reading CSV from S3: {e}")
        return {'statusCode': 500, 'body': 'Error reading CSV from S3'}
    
    # Check for required columns
    required_columns = {'timestamp', 'text_for_analysis', 'sentiment'}
    if not required_columns.issubset(df.columns):
        missing_cols = required_columns - set(df.columns)
        print(f"Missing columns in CSV: {missing_cols}")
        return {'statusCode': 400, 'body': f'Missing columns: {missing_cols}'}
    
    # Format the data for the LLM prompt
    combined_text = ""
    for _, row in df.iterrows():
        combined_text += f"DateTime: {row['timestamp']}\n"
        combined_text += f"Text: {row['text_for_analysis']}\n"
        combined_text += f"Sentiment: {row['sentiment']}\n\n"
    
    # Define the prompt
    prompt = f"""
        You are a great stock market analyzer and you are tasked with generating comprehensive daily summary of the market emails for our subscribed users from the following daily sentiment analysis results:\n\n{combined_text}\n

        I want you to output in a specific format only and do not use here's the summary..., etc.\n
        
        Output:
        
        Dear Subscriber,\n\n
        Here is your daily summary of market sentiments and key events:\n\n
        <Put Your Summary Here>\n
        Best regards,\nYour Market Insights Team"""
    
    # Initialize Bedrock client
    bedrock_client = boto3.client('bedrock-runtime', region_name='us-east-1')
    
    # Prepare the request payload
    native_request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1024,
        "temperature": 0.7,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
    }
    
    model_id = "us.anthropic.claude-3-5-sonnet-20241022-v2:0"
    request = json.dumps(native_request)
    
    # Invoke the Claude 3.5 Sonnet model
    try:
        response = bedrock_client.invoke_model(modelId=model_id, body=request)
        model_response = json.loads(response["body"].read())
        summary = model_response["content"][0]["text"]
    except (ClientError, Exception) as e:
        print(f"Error invoking Claude 3.5 Sonnet model: {e}")
        return {'statusCode': 500, 'body': 'Error invoking Claude 3.5 Sonnet model'}
    
    # Define the output bucket and key
    output_bucket = 'llm-summary'
    output_key = f"txt/{object_key.split('/')[-1].replace('.csv', '.txt')}"
    
    # Save the summary to the output S3 bucket
    try:
        s3_client.put_object(Bucket=output_bucket, Key=output_key, Body=summary)
    except Exception as e:
        print(f"Error saving summary to S3: {e}")
        return {'statusCode': 500, 'body': 'Error saving summary to S3'}
    
    # Send the summary via SNS email
    sns_client = boto3.client('sns')
    topic_arn = 'arn:aws:sns:us-east-1:418272765834:dailySummaryEmail'
    
    try:
        sns_client.publish(
            TopicArn=topic_arn,
            Subject='Your Daily Market Sentiment Summary',
            Message=summary
        )
    except Exception as e:
        print(f"Error sending email via SNS: {e}")
        return {'statusCode': 500, 'body': 'Error sending email via SNS'}
    
    return {'statusCode': 200, 'body': 'daily summary generated, saved, and emailed successfully'}