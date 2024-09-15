# MIT License
# Copy it, but attribute the author: David Horvath - systematicguy.com
# Linking to the repo is also fine.

import os
import io
import zipfile
from datetime import datetime

import boto3


BUCKET = os.getenv("BUCKET")
EXCLUDE_ZIPS = True

s3 = boto3.client("s3")
s3_paginator = s3.get_paginator('list_objects_v2')

# inspiration taken from https://stackoverflow.com/a/76552544/429162


def list_dir(bucket_name, *, prefix, exclude_zips=False):
    files = []
    pages = s3_paginator.paginate(Bucket=bucket_name, Prefix=prefix)
    for page in pages:
        for obj in page['Contents']:
            files.append(obj['Key'])

    if exclude_zips:
        files = [f for f in files if not f.lower().endswith(".zip")]

    return files


def zip_folder_into_buffer(bucket_name, *, prefix):
    files = list_dir(bucket_name, prefix=prefix, exclude_zips=EXCLUDE_ZIPS)
    zip_buffer = io.BytesIO()
    for key in files:
        print(f"Adding file {key}")
        object_key = f"{prefix}{key}"
        print(f"{object_key=}")

        with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED, False) as zipper:
            infile_object = s3.get_object(Bucket=bucket_name, Key=object_key)
            infile_content = infile_object['Body'].read()
            zipper.writestr(key, infile_content)

    return zip_buffer


def handler(event, context):
    zip_buffer = zip_folder_into_buffer(BUCKET, prefix="")
    zip_filename = f"all_{datetime.now().strftime('%Y-%m-%d_%H-%M-%SZ')}.zip"
    s3.put_object(Bucket=BUCKET, Key=zip_filename, Body=zip_buffer.getvalue())