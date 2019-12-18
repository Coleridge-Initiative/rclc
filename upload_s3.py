#!/usr/bin/env python
# # encoding: utf-8
import boto3
import os 
import datetime
import json
from git import Repo

# initialize access to the bucket
def access_bucket():
    bucket_name = "richcontext"
    bucket = boto3.resource("s3").Bucket(bucket_name)
    return bucket

def format_file_path(file_path):
    if file_path.endswith("/"):
        return file_path
    elif not file_path.endswith("/"):
        return file_path + "/"

def gen_files(pdf_file_path,json_file_path,bucket):
    
    jsons = [j.split(".pdf.json")[0] for j in os.listdir(json_file_path) if j.endswith(".json")]
    pdfs = [p for p in os.listdir(pdf_file_path) if p.endswith(".pdf")]

    # list docs that we have locally but not in bucket
    prefix = "corpus_docs"
    existing_json = [obj.key.split("/")[2].split(".json")[0] for obj in bucket.objects.filter(Prefix=prefix + "/json") if obj.key.endswith(".json")]
    existing_pdfs = [obj.key.split("corpus_docs/pdf/")[1] for obj in bucket.objects.filter(Prefix=prefix + "/pdf") if obj.key.endswith(".pdf")]
    
    new_json = [j for j in jsons if j not in existing_json]
    new_pdf = [j for j in pdfs if j not in existing_pdfs]
    
    print("There are {} json files in the bucket, uploading {} more".format(len(existing_json),len(new_json)))
    print("There are {} pdf files in the bucket, uploading {} more".format(len(existing_pdfs),len(new_pdf)))
    return new_json, new_pdf

def write_file(local_path,s3_path):
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(local_path, 'richcontext', s3_path)

def write_jsons(json_file_path, new_json):
    for n in new_json:
        local_json_path = json_file_path + n + ".pdf.json"
        s3_json_path = "corpus_docs/json/" + n.split(".pdf.json")[0] + ".json"
        write_file(local_path = local_json_path,s3_path = s3_json_path)
        print("uploading {} to {}".format(local_json_path,s3_json_path))
    print("Uploaded {} files to corpus_docs/json/ in the bucket".format(len(new_json)))
    

def write_pdfs(pdf_file_path, new_pdf):
    for n in new_pdf:
        local_pdf_path = pdf_file_path + n
        s3_pdf_path = "corpus_docs/pdf/" + n
        write_file(local_path = local_pdf_path,s3_path = s3_pdf_path)
        print("uploading {} to {}".format(local_pdf_path,s3_pdf_path))
    print("Uploaded {} files to corpus_docs/pdf/ in the bucket".format(len(new_pdf)))

def write_manifest(new_json,new_pdf,pdf_file_path):
    # Write to manifest.txt
    git_path = pdf_file_path[0:pdf_file_path.find("rclc")+4]
    repo = Repo(git_path)

    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    manifest_data = {}
    manifest_data["date"] = datetime.date.today().strftime("%Y%m%d")
    manifest_data["num_files_uploaded"] = len(new_json)+len(new_pdf)
    manifest_data["release"] = tags[-1]
    fo = open("MANIFEST.txt", "w")
    for k, v in manifest_data.items():
        fo.write(str(k) + ': '+ str(v) + '\n')
    fo.close()


if __name__ == "__main__":
    
    # Retrieve and format file paths
    pdf_file_path = input("Enter file path where your pdfs are stored locally: ")
    json_file_path = input("Enter file path where your jsons are stored locally: ")
    
    pdf_file_path = format_file_path(pdf_file_path)
    json_file_path = format_file_path(json_file_path)
    # Connect to bucket
    bucket = access_bucket()
    
    # Upload files
    new_json, new_pdf =  gen_files(pdf_file_path = pdf_file_path,json_file_path = json_file_path, bucket = bucket)
    write_jsons(json_file_path = json_file_path, new_json = new_json)
    write_pdfs(pdf_file_path = pdf_file_path, new_pdf = new_pdf)
    
    # Write upload details to manifest
    write_manifest(new_json = new_json, new_pdf = new_pdf,pdf_file_path=pdf_file_path)




