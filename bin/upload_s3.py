#!/usr/bin/env python
# # encoding: utf-8

from git import Repo
import boto3
import datetime
import json
import os
import sys


def access_bucket (handle):
    """
    initialize access to the bucket
    """
    bucket_name = "richcontext"
    bucket = handle.Bucket(bucket_name)
    return bucket


def format_file_path (file_path):
    if file_path.endswith("/"):
        return file_path
    elif not file_path.endswith("/"):
        return file_path + "/"


def gen_files (pdf_file_path, json_file_path, bucket):
    pdfs = [p for p in os.listdir(pdf_file_path) if p.endswith(".pdf")]
    jsons = [j.split(".pdf.json")[0] for j in os.listdir(json_file_path) if j.endswith(".json")]

    # list docs that we have locally but not in bucket
    prefix = "corpus_docs"
    existing_json = [obj.key.split("/")[2].split(".json")[0] for obj in bucket.objects.filter(Prefix=prefix + "/json") if obj.key.endswith(".json")]
    existing_pdfs = [obj.key.split("corpus_docs/pdf/")[1] for obj in bucket.objects.filter(Prefix=prefix + "/pdf") if obj.key.endswith(".pdf")]
    
    new_json = [j for j in jsons if j not in existing_json]
    new_pdf = [j for j in pdfs if j not in existing_pdfs]
    
    print("there are {} JSON files in the bucket, uploading {} more".format(len(existing_json), len(new_json)))
    print("there are {} PDF files in the bucket, uploading {} more".format(len(existing_pdfs), len(new_pdf)))
    return new_json, new_pdf


def upload_file (handle, local_path, s3_path):
    handle.meta.client.upload_file(local_path, "richcontext", s3_path)


def upload_pdf_files (handle, json_file_path, new_json):
    for n in new_json:
        local_json_path = json_file_path + n + ".pdf.json"
        s3_json_path = "corpus_docs/json/" + n.split(".pdf.json")[0] + ".json"
        upload_file(handle, local_json_path, s3_json_path)
        print("uploading {} to {}".format(local_json_path, s3_json_path))

    print("uploaded {} files to corpus_docs/json/ in the bucket".format(len(new_json)))
    

def upload_pdf_files (handle, pdf_file_path, new_pdf):
    for n in new_pdf:
        local_pdf_path = pdf_file_path + n
        s3_pdf_path = "corpus_docs/pdf/" + n
        upload_file(handle, local_pdf_path, s3_pdf_path)
        print("uploading {} to {}".format(local_pdf_path, s3_pdf_path))

    print("Uploaded {} files to corpus_docs/pdf/ in the bucket".format(len(new_pdf)))


def write_manifest (new_json, new_pdf, git_path="."):
    """
    summarize upload details to MANIFEST.txt
    """
    #git_path = pdf_file_path[0:pdf_file_path.find("rclc")+4]

    repo = Repo(git_path)
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    filename = "MANIFEST.txt"
    manifest_data = {}

    manifest_data["date"] = datetime.date.today().strftime("%Y-%m-%d")
    manifest_data["uploaded"] = len(new_json) + len(new_pdf)
    manifest_data["release"] = tags[-1]

    with open(filename, "w") as f:
        for key, val in manifest_data.items():
            f.write("{}: {}\n".format(key, str(val)))

    s3_path = "corpus_docs/" + filename
    upload_file(handle, filename, s3_path)


if __name__ == "__main__":
    # connect to bucket
    handle = boto3.resource("s3")
    bucket = access_bucket(handle)

    # retrieve and format file paths
    pdf_file_path = "resources/pub/pdf/"
    #input("Enter path where your PDF files are stored locally: ")

    json_file_path = "resources/pub/json/"
    #input("Enter path where your JSON files are stored locally: ")
    
    pdf_file_path = format_file_path(pdf_file_path)
    json_file_path = format_file_path(json_file_path)

    # upload files
    new_json, new_pdf = gen_files(pdf_file_path, json_file_path, bucket)
    upload_pdf_files(handle, pdf_file_path, new_pdf)
    upload_pdf_files(handle, json_file_path, new_json)

    # write upload details to manifest
    write_manifest(new_json, new_pdf)
