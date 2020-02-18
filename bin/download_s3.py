#!/usr/bin/env python
# encoding: utf-8

import boto3

# initialize access to the storage grid bucket
bucket_name = "richcontext"
bucket = boto3.resource("s3").Bucket(bucket_name)

# list up to N keys for files within our pseudo-directory
prefix = "corpus_docs"
limit = 10
    
for obj in bucket.objects.filter(Prefix=prefix):
    if limit < 1:
        break
    else:
        print(obj.key)
        limit -= 1

# show an example of how to download a specific file
local_file = "001966ac583b67a965cf.json"
key = prefix + "/pub/json/" + local_file

bucket.download_file(key, local_file)

