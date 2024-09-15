# MIT License
# Copy it, but attribute the author: David Horvath - systematicguy.com
# Linking to the repo is also fine.

import os
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
import re
import hashlib

import boto3
from unidecode import unidecode
from aws_lambda_powertools.logging import Logger


JOB_INPUT_BUCKET = os.getenv('JOB_INPUT_BUCKET')

# Supported media formats for Amazon Transcribe
SUPPORTED_FORMATS = ['mp3', 'mp4', 'wav', 'flac', 'ogg', 'amr', 'webm', 'm4a']
FOLDER_SEPARATOR = "__"
# TODO aac

s3 = boto3.client('s3')

TIMEZONE = os.getenv('TIMEZONE', 'UTC')
timezone_preference = ZoneInfo(TIMEZONE)

logger = Logger()

invalid_chars = re.compile(r"[^0-9a-zA-Z._-]+")  # + will match multiple adjacent invalid characters


def sanitize_path(path: str) -> str:
    ''' Sanitize object key keeping folder structure intact '''

    latin_path = unidecode(path)

    # make sure each part of the path is sanitized
    # replace adjacent invalid characters to exactly one '_'
    sanitized_path = "/".join(invalid_chars.sub('_', part) for part in Path(latin_path).parts)
    return sanitized_path


def hash8chars(s: str) -> str:
    return str(int(hashlib.sha256(s.encode('utf-8')).hexdigest(), 16) % 10**8)


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    # Extract bucket name and object key from the event
    bucket = event['detail']['bucket']['name']
    uploaded_key = event['detail']['object']['key']

    event_datetime_str = event['time']
    event_datetime = datetime.fromisoformat(event_datetime_str)

    sanitized_key = sanitize_path(uploaded_key)
    file_extension = Path(sanitized_key).suffix.lower().strip('.')

    logger.info({
        "event_datetime_str": event_datetime_str,
        "uploaded_key": uploaded_key,
        "sanitized_key": sanitized_key,
        "file_extension": file_extension,
    })

    if file_extension not in SUPPORTED_FORMATS:
        raise ValueError(f"Unsupported file format: {file_extension}, supported formats: {SUPPORTED_FORMATS}")

    # Generate new key with suffix based on upload time in the desired timezone as per TZ
    datetime_suffix = event_datetime.astimezone(timezone_preference).strftime('%Y-%m-%d_%H-%M-%S')

    sanitized_path = Path(sanitized_key)
    new_s3_key = f"{sanitized_path}.uploaded-{datetime_suffix}.{file_extension}"  # this is more readable and searchable

    # Copy the file to the audio bucket with the new key
    s3.copy_object(
        Bucket=JOB_INPUT_BUCKET,
        CopySource={'Bucket': bucket, 'Key': uploaded_key},
        Key=new_s3_key
    )

    # Delete the original file from the upload bucket
    s3.delete_object(Bucket=bucket, Key=uploaded_key)

    # https://docs.aws.amazon.com/transcribe/latest/APIReference/API_StartTranscriptionJob.html#transcribe-StartTranscriptionJob-request-TranscriptionJobName
    transcription_job_name = new_s3_key.replace("/", FOLDER_SEPARATOR)
    # make sure transcription job name is no longer than 200 characters, converting the last 8 characters to hash
    if len(transcription_job_name) > 180:
        transcription_job_name = f"{transcription_job_name[:180]}-{hash8chars(transcription_job_name)}"

    # Prepare the response payload for Step Function
    response = {
        "TranscriptionJobName": transcription_job_name,
        "Media": {
            "MediaFileUri": f"s3://{JOB_INPUT_BUCKET}/{new_s3_key}",
        },
        "MediaFormat": file_extension,
        "OutputKey": f"{new_s3_key}.transcription.json"
    }

    logger.info({"response": response})
    return response
