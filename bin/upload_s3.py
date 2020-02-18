#!/usr/bin/env python
# encoding: utf-8

from git import Repo
from pathlib import Path
from tqdm import tqdm
import boto3
import datetime
import json
import os
import sys

BUCKET_NAME = "richcontext"


def access_bucket (handle):
    """
    initialize access to the bucket
    """
    bucket = handle.Bucket(BUCKET_NAME)
    return bucket


def upload_file (handle, local_path, grid_path):
    """
    upload a local file to the bucket
    """
    handle.meta.client.upload_file(local_path, BUCKET_NAME, grid_path)


def list_uploaded_files (bucket, prefix, kind):
    """
    list the files of a particular kind which have already been
    uploaded to the bucket
    """
    done = set([])
    extension = f".{kind}"

    for obj in bucket.objects.filter(Prefix=prefix + "/pub/" + kind):
        if obj.key.endswith(extension):
            uuid = obj.key.split("/")[3].split(extension)[0]
            done.add(uuid)
    
    return done


def iter_needed_files (dir_path, kind, done):
    """
    iterator for the local files of a particular kind which 
    haven't been uploaded yet
    """
    for file_name in tqdm(list(dir_path.glob(f"*.{kind}")), ascii=True, desc=f"{kind} files"):
        uuid = file_name.stem

        if uuid not in done:
            yield uuid


def upload_needed_files (handle, bucket, prefix, dir_path, kind, iter):
    """
    upload the needed local files of a particular kind
    """
    extension = f".{kind}"
    count = 0

    for uuid in iter:
        file_name = uuid + extension
        local_path = dir_path / file_name
        grid_path = prefix + "/pub/" + kind + "/"

        #print("uploading {} to {}".format(local_path, grid_path))

        upload_file(handle, local_path.as_posix(), grid_path + file_name)
        count += 1

    return count
    

def manage_upload (handle, bucket, prefix, pub_dir, kind):
    """
    manage the upload for a particular kind of file
    """
    dir_path = pub_dir / kind
    done = list_uploaded_files(bucket, prefix, kind)
    iter = iter_needed_files(dir_path, kind, done)  
    count = upload_needed_files(handle, bucket, prefix, dir_path, kind, iter)

    return len(done), count


def write_manifest (handle, prefix, manifest_data, file_name="MANIFEST.txt"):
    """
    summarize details about the upload to a `MANIFEST.txt` 
    file in the bucket
    """
    with open(file_name, "w") as f:
        for key, val in manifest_data.items():
            f.write("{}: {}\n".format(key, str(val)))

    grid_path = prefix + "/" + file_name
    upload_file(handle, file_name, grid_path)


def main ():
    # locate the Git tag info
    git_path = Path.cwd().as_posix()
    repo = Repo(git_path)
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    # set up the manifest
    manifest_data = {}
    manifest_data["date"] = datetime.date.today().strftime("%Y-%m-%d")
    manifest_data["release"] = tags[-1]

    # connect to the storage grid bucket
    handle = boto3.resource("s3")
    bucket = access_bucket(handle)
    prefix = "corpus_docs"

    # set up the local paths
    pub_dir = Path.cwd() / "resources/pub"
    
    # which PDF files do we need to upload?
    count, prev_count = manage_upload(handle, bucket, prefix, pub_dir, "pdf")
    manifest_data["uploaded_pdf"] = count + prev_count

    # which JSON files do we need to upload?
    count, prev_count = manage_upload(handle, bucket, prefix, pub_dir, "json")
    manifest_data["uploaded_json"] = count + prev_count

    # which TXT files do we need to upload?
    count, prev_count = manage_upload(handle, bucket, prefix, pub_dir, "txt")
    manifest_data["uploaded_txt"] = count + prev_count

    # write upload details to manifest
    write_manifest(handle, prefix, manifest_data)


if __name__ == "__main__":
    main()
