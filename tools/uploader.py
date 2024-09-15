# MIT License
# Copy it, but attribute the author: David Horvath - systematicguy.com
# Linking to the repo is also fine.

from pathlib import Path
import re
import os
import json

import boto3
from unidecode import unidecode


invalid_chars = re.compile(r"[^0-9a-zA-Z._-]+")  # + will match multiple adjacent invalid characters

DRY_RUN = True

LOCAL_FOLDER = os.getenv('LOCAL_FOLDER')
UPLOAD_BUCKET = os.getenv('UPLOAD_BUCKET')
OTHER_BUCKET = os.getenv('OTHER_BUCKET')


s3_client = boto3.client('s3')
s3_paginator = s3_client.get_paginator('list_objects_v2')

SUPPORTED_FORMATS = ['mp3', 'mp4', 'wav', 'flac', 'ogg', 'amr', 'webm', 'm4a']


def sanitize_path(path: str) -> str:
    ''' Sanitize object key keeping folder structure intact '''

    latin_path = unidecode(path)

    # make sure each part of the path is sanitized
    # replace adjacent invalid characters to exactly one '_'
    sanitized_path = "/".join(invalid_chars.sub('_', part) for part in Path(latin_path).parts)
    return sanitized_path


def list_dir(bucket_name, *, prefix, exclude_zips=False):
    files = []
    pages = s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in pages:
        for obj in page['Contents']:
            files.append(obj['Key'])

    if exclude_zips:
        files = [f for f in files if not f.lower().endswith(".zip")]

    return files


def strip_upload_suffix(files: list[str]) -> list[str]:
    pattern = r'^(.*?)(?=\.uploaded-\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})'
    result = []
    for path in files:
        match = re.match(pattern, path)
        result.append(match.group(0) if match else path)
    return result


def upload_folder_to_s3(local_folder, bucket_name, s3_base_path=''):
    """
    Recursively uploads a local folder to an S3 bucket, maintaining the folder structure.

    :param local_folder: The local folder path to upload.
    :param bucket_name: The name of the S3 bucket.
    :param s3_base_path: The base path in the S3 bucket (optional).
    """

    other_keys = list_dir(OTHER_BUCKET, prefix='')
    other_keys = strip_upload_suffix(other_keys)
    other_keys = set(other_keys)

    # Convert the local folder to a Path object
    local_folder = Path(local_folder)

    num_already_uploaded = 0
    num_processed = 0
    skipped = []

    # Walk through all files in the specified local folder recursively
    for file_path in local_folder.rglob('*'):
        if file_path.is_file():
            num_processed += 1
            print("")
            print(f"# {num_processed}: Processing {file_path}")

            # Construct the S3 key, preserving folder structure
            relative_path = file_path.relative_to(local_folder.parent)
            uploaded_key = (Path(s3_base_path) / relative_path).as_posix()  # Convert path to POSIX format for S3

            sanitized_key = sanitize_path(uploaded_key)
            if sanitized_key in other_keys:
                print(f" Skipping {file_path} as {sanitized_key} already exists in {OTHER_BUCKET}")
                num_already_uploaded += 1
                continue

            if Path(uploaded_key).suffix.lower().strip('.') not in SUPPORTED_FORMATS:
                print(f" Skipping {file_path} as {uploaded_key} has an unsupported file extension")
                skipped.append(file_path)
                continue

            try:
                # Upload the file to S3
                size = file_path.stat().st_size
                print(f" Uploading src {file_path} (size: {size})")
                print(f" Uploading dst s3://{bucket_name}/{uploaded_key} ...")
                if not DRY_RUN:
                    s3_client.upload_file(str(file_path), bucket_name, uploaded_key)

            except Exception as e:
                print(f" Error uploading {file_path}: {e}")

    print("----------------------------------")
    print("skipped:")
    print(json.dumps(skipped, indent=2, default=str))
    print(f"Processed {num_processed} files, {num_already_uploaded} already uploaded, {len(skipped)} skipped")

upload_folder_to_s3(LOCAL_FOLDER, UPLOAD_BUCKET)
