# MIT License
# Copy it, but attribute the author: David Horvath - systematicguy.com
# Linking to the repo is also fine.

import os
import json
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
from aws_lambda_powertools.logging import Logger


JOB_INPUT_BUCKET = os.getenv('JOB_INPUT_BUCKET')

# Supported media formats for Amazon Transcribe
SUPPORTED_FORMATS = ['mp3', 'mp4', 'wav', 'flac', 'ogg', 'amr', 'webm', 'm4a']

s3 = boto3.client('s3')

TIMEZONE = os.getenv('TIMEZONE', 'UTC')
timezone_preference = ZoneInfo(TIMEZONE)


logger = Logger()


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    # Extract bucket name and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    event_datetime_str = event['Records'][0]['eventTime']

    print(f"{event_datetime_str=}")
    event_datetime = datetime.fromisoformat(event_datetime_str)

    file_extension = Path(key).suffix.lower().strip('.')
    print(f"{file_extension=}")

    if file_extension not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported file format: {file_extension}, supported formats: {SUPPORTED_FORMATS}")

    # Generate new key with suffix based on upload time in the desired timezone as per TZ
    datetime_suffix = event_datetime.astimezone(timezone_preference).strftime('%Y-%m-%d-%H-%M-%S')
    # TODO sanitize the key to avoid any special characters
    new_s3_key = f"{Path(key).stem}-uploaded-{datetime_suffix}.{file_extension}"

    # Copy the file to the audio bucket with the new key
    s3.copy_object(
        Bucket=JOB_INPUT_BUCKET,
        CopySource={'Bucket': bucket, 'Key': key},
        Key=new_s3_key
    )

    # Delete the original file from the upload bucket
    s3.delete_object(Bucket=bucket, Key=key)

    # Prepare the response payload for Step Function
    response = {
        "TranscriptionJobName": new_s3_key,
        "Media": {
            "MediaFileUri": f"s3://{JOB_INPUT_BUCKET}/{new_s3_key}",
        },
        "MediaFormat": file_extension,
        "OutputKey": f"{new_s3_key}-transcription.json"
    }

    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }
