# MIT License
# Copy it, but attribute the author: David Horvath - systematicguy.com
# Linking to the repo is also fine.

import os
import json
from datetime import timedelta

import boto3
from aws_lambda_powertools.logging import Logger

s3 = boto3.client('s3')

logger = Logger()

DESTINATION_BUCKET = os.environ.get('DESTINATION_BUCKET')


# Helper function to format time to SRT format (hh:mm:ss,ms)
def format_time(seconds):
    ms = int((seconds - int(seconds)) * 1000)
    td = timedelta(seconds=int(seconds))
    return str(td) + f',{ms:03d}'


# Function to generate SRT content from audio segments
def generate_srt(audio_segments):
    srt_content = []
    for index, segment in enumerate(audio_segments):
        start_time = format_time(float(segment['start_time']))
        end_time = format_time(float(segment['end_time']))
        transcript = segment['transcript']

        srt_content.append(f"{index + 1}")
        srt_content.append(f"{start_time} --> {end_time}")
        srt_content.append(transcript)
        srt_content.append("")  # SRT entries are separated by a blank line

    return "\n".join(srt_content)


@logger.inject_lambda_context(log_event=True)
def handler(event, context):
    https_s3_uri = event['jobStatus']['TranscriptionJob']['Transcript']['TranscriptFileUri']

    source_bucket_and_key = https_s3_uri.split('amazonaws.com/')[1]
    source_bucket, source_key = source_bucket_and_key.split('/', 1)
    destination_key = source_key.replace('.json', '.srt')

    logger.info({
        'https_s3_uri': https_s3_uri,
        'source_bucket': source_bucket,
        'source_key': source_key,
        'destination_key': destination_key,
    })

    # Get the JSON file from S3
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    json_content = json.loads(response['Body'].read().decode('utf-8'))

    # Extract audio segments
    audio_segments = json_content['results']['audio_segments']

    # Generate SRT content
    srt_content = generate_srt(audio_segments)

    # Upload the SRT file to the destination bucket
    s3.put_object(
        Bucket=DESTINATION_BUCKET,
        Key=destination_key,
        Body=srt_content,
        ContentType='text/srt'
    )
    return {}
