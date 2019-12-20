#!/usr/bin/env python
# encoding: utf-8

import boto3

# initialize access to the bucket
bucket_name = "richcontext"
bucket = boto3.resource("s3").Bucket(bucket_name)

# list the keys for files within our pseudo-directory
prefix = "corpus_docs/"
    
for obj in bucket.objects.filter(Prefix=prefix):
    print(obj.key)

# example of how to download one file
key = "corpus_docs/pdf/ff659e8fd9a4b45a65e0.pdf"
local_file = "ff659e8fd9a4b45a65e0.pdf"

bucket.download_file(key, local_file)


