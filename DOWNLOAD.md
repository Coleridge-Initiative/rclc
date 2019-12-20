## Installation

To install the Python library dependencies:

```
pip install -r requirements.txt
```


## Download Parsed PDFs

The `bin/download_s3.py` script provides example code for downloading
PDF files (open access publications) and JSON files (extracted text)
from the public S3 bucket.


## Collecting Open Access PDFs

**For those on the NYU-CI team who update the corpus:**

Download the corpus PDFs and other resource files:

```
python bin/download_resources.py
```

The PDF files get stored in the `resources/pub/pdf` subdirectory.


## Extract text from PDFs

We use `science-parse` to extract text from research publications.
Download the latest `science-parse-cli-assembly-*.jar` from the
<https://github.com/allenai/science-parse/releases>
and copy that JAR file into the `lib/` subdirectory.

Then run the `science-parse` CLI to extract text from the PDF files,
and be sure to use the correct version number for the JAR that you
downloaded:

```
mkdir -p resources/pub/json
SPJAR=lib/science-parse-cli-assembly-2.0.3.jar
java -jar $SPJAR -o ./resources/pub/json ./resources/pub/pdf
```

That command will download multiple resources from the Allan AI public
datastore, which may take several minutes.


## Upload PDF and JSON files

**For those on the NYU-CI team who update the corpus:**

Upload the PDF files (open access publications) and JSON files
(extracted text) to the public S3 bucket:

```
python bin/upload_s3.py
```


## S3 Bucket Specs

View the public AWS S3 Bucket `richcontext` online:

 - <https://richcontext.s3.us-east-2.amazonaws.com/>
 - <https://s3.console.aws.amazon.com/s3/buckets/richcontext/corpus_docs/>

The directory structure of the public S3 bucket is similar to the
directory structure used for resources in this repo:

- richcontext
  - corpus_docs
    - pdf
    - json
