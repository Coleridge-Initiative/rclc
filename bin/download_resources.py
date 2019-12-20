#!/usr/bin/env python
# encoding: utf-8

from bs4 import BeautifulSoup  # type: ignore
from pathlib import Path
from pdfminer.pdfdocument import PDFDocument  # type: ignore
from pdfminer.pdfpage import PDFTextExtractionNotAllowed  # type: ignore
from pdfminer.pdfparser import PDFParser, PDFSyntaxError  # type: ignore
from requests_html import HTMLSession  # type: ignore
from tqdm import tqdm  # type: ignore
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse
import argparse
import csv
import json
import logging  # type: ignore
import ray  # type: ignore
import requests
import sys
import time
import traceback

DEFAULT_LOGGER_FILE = None
DEFAULT_CORPUS_FILE = "corpus.jsonld"
DEFAULT_TODO_FILE = "todo.tsv"
DEFAULT_TODO_LIST = None
DEFAULT_OUTPUT_RESOURCE = "resources/"
DEFAULT_FORCE_DOWNLOAD = False

MAX_DOWNLOAD_TRIAL = 3

PUB_PDF_PATH = "pub/pdf/"
DAT_PAGE_PATH = "dat/"

LOGGER = None  # type: ignore


@ray.remote
class Worker (object):
    def __init__ (self):
        self.logger = LOGGER

    def train (self):
        self.logger.warning("print from inside worker")


def setup_logger (args) -> None:
    """ logging is optional: to debug, set the logging level
    """
    global LOGGER
    level = logging.WARNING

    if args.logger:
        logging.basicConfig(filename=args.logger, filemode="w", level=level)
    else:
        logging.basicConfig(stream=sys.stdout, level=level)

    LOGGER = logging.getLogger("RichContext")


def load_corpus (filename: str) -> dict:
    """ Load the corpus file (in JSON-LD format)
    """
    global LOGGER
    corpus = None

    with open(filename, "r") as f:
        jld_corpus = json.load(f)
        corpus = jld_corpus["@graph"]

    LOGGER.warning(f"number of records in the corpus: {len(corpus)}")
    return corpus


def generate_todo (flag: str, todo_pub: list, todo_dat: list) -> None:
    """ Generate a TODO file for downloads
    """
    if not flag:
        with open(DEFAULT_TODO_FILE, "wt") as f:
            writer = csv.writer(f, delimiter="\t")

            for t in todo_pub:
                writer.writerow(t)

            for t in todo_dat:
                writer.writerow(t)


def load_todo (filename: str) -> List[Any]:
    """ load a TSV file for the list of files to be downloaded
    """
    todo = []

    with open(filename, "r") as f:
        reader = csv.reader(f, delimiter="\t")

        for row in reader:
            todo.append(row)


def enum_pub_resources (corpus: dict, output_path: Path, force_download: bool) -> Tuple[Path, List[List[Any]]]:
    """ Enumerate all publications PDF files from the corpus data to be 
        downloaded, if not downloaded yet.  We use the entity id as filename.
        All downloaded files are stored under `output_path` folder.
        Input:
            - corpus: corpus file containing a list of publications.
            - output_path: path to store downloaded resources.
            - force_download: always download resources.
    """
    global LOGGER
    pub_path = output_path / PUB_PDF_PATH

    if not pub_path.exists():
        pub_path.mkdir(parents=True)

    pubs = [e for e in corpus if e["@type"] == "ResearchPublication"]

    if force_download:
        downloaded_pubs_id = set([])
    else:
        downloaded_pubs = list(pub_path.glob("*.pdf"))
        downloaded_pubs_id = set([f.stem for f in downloaded_pubs])

    todo = []

    for entity in pubs:
        e_id = urlparse(entity["@id"]).fragment.split("-")[1]
        downloaded_before = e_id in downloaded_pubs_id

        if force_download or not downloaded_before:
            if isinstance(entity["openAccess"], list):
                ## this only happens in the error case where
                ## publications are duplicated in the corpus
                res_url = entity["openAccess"][0]["@value"]
                LOGGER.warning("duplicate: {} {}".format(e_id, entity["openAccess"]))
            else:
                res_url = entity["openAccess"]["@value"]

            todo.append(["pdf", e_id, res_url, pub_path])

    return pub_path, todo


def enum_dat_resources (corpus: dict, output_path: Path, force_download: bool) -> Tuple[Path, List[List[Any]]]:
    """ Enumerate all dataset "foaf:page" files from corpus data to be 
        downloaded, if not downloaded yet. Uses the entity id as filename.
        All downloaded files are stored under `output_path` folder.
        Input:
            - corpus: corpus file containing a list of datasets.
            - output_path: path to store downloaded resources.
            - force_download: always download resources.
    """
    global LOGGER
    dat_path = output_path / DAT_PAGE_PATH

    if not dat_path.exists():
        dat_path.mkdir(parents=True)

    dats = [e for e in corpus if e["@type"] == "Dataset"]

    if force_download:
        downloaded_dat_id = set([])
    else:
        downloaded_datasets = list(dat_path.glob("*.*"))
        downloaded_dat_id = set([f.stem for f in downloaded_datasets])

    todo = []

    for entity in dats:
        e_id = urlparse(entity["@id"]).fragment.split("-")[1]
        downloaded_before = e_id in downloaded_dat_id

        if force_download or not downloaded_before:
            res_url = entity["foaf:page"]["@value"]

            if res_url.startswith("http://example.com"):
                # ignore these placeholder URLs
                continue
            else:
                todo.append(["unknown", e_id, res_url, dat_path])

    return dat_path, todo


def is_valid_pdf_file (filename: str) -> bool:
    global LOGGER

    try:
        with open(filename, "rb") as f:
            parser = PDFParser(f)
            document = PDFDocument(parser, "")

            if not document.is_extractable:
                raise PDFTextExtractionNotAllowed(filename)

            return True
    except:
        LOGGER.debug(traceback.format_exc())
        LOGGER.debug(f"not valid PDF file: {filename}")
        return False


@ray.remote
def _download (res_type: str, e_id: str, url: str, output_path: Path) -> Tuple[bool, str]:
    """ Download a resource for the corpus and store it in a file
    """
    global LOGGER

    if res_type not in ["pdf", "html", "unknown"]:
        raise ValueError(f"Invalid resource type: {res_type}")

    headers = requests.utils.default_headers()
    headers["User-Agent"] = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36"

    trial = 0
    status = None

    while trial < MAX_DOWNLOAD_TRIAL:
        try:
            parsed_url = urlparse(url)

            if parsed_url.netloc == "www.sciencedirect.com":
                """ special case: sciencedirect.com auto generates PDF
                    download link in an intermediate page
                """
                try:
                    session = HTMLSession()
                    r0 = session.get(url)
                    res = session.get(list(r0.html.absolute_links)[0])
                except:
                    LOGGER.debug(traceback.format_exc())
                    LOGGER.debug(list(r0.html.absolute_links))

                    status = f"session.get failed: {e_id} {url}"
                    LOGGER.warning(status)

                    return False, status

            elif parsed_url.netloc.endswith("onlinelibrary.wiley.com"):
                """ special case: wiley.com auto embeds to render PDF
                """
                r0 = requests.get(url)
                soup = BeautifulSoup(r0.content, "html5lib")

                if soup.find("embed") is None:
                    status = f"no embedded PDF: {e_id} {url}"
                    LOGGER.warning(status)

                    trial += 1
                    continue

                src = soup.find("embed")["src"]
                res = requests.get(parsed_url.scheme + "://" + parsed_url.netloc + src)
            else:
                res = requests.get(url, headers=headers, timeout=(10, 20))

            if res_type == "unknown":
                content_type = res.headers["content-type"]
                res_type = "html" if "text/html" in content_type else "pdf"

            out_file = output_path / (e_id + "." + res_type)
            out_file.write_bytes(res.content)

            if res_type == "pdf":
                filename = out_file.resolve().as_posix()

                try:
                    if is_valid_pdf_file(filename):
                        LOGGER.debug("writing: {filename}")

                    if not is_valid_pdf_file(filename):
                        out_file.unlink()
                        trial += 1
                        continue
                except:
                    LOGGER.debug(traceback.format_exc())

                    status = f"{e_id} {url} not valid PDF: {filename}"
                    LOGGER.warning(status)

                    return False, status

            return True, status

        except KeyboardInterrupt:
            pass

        except requests.exceptions.RequestException as err:
            status = f"{e_id} {url} request exception {err}"
            LOGGER.warning(status)

            time.sleep(1)
            trial += 1

    if trial == MAX_DOWNLOAD_TRIAL:
        LOGGER.debug(f"aborted {e_id} {url} after {MAX_DOWNLOAD_TRIAL} attempts")

    return False, status


def download_resource_files (todo: list) -> None:
    """ Download all resource files on the TODO list.
        We use the entity id as filename.
        Input:
            - todo: list of URLs to download
    """
    global LOGGER

    try:
        for _type, e_id, res_url, path in tqdm(todo, ascii=True, desc="download files"):
            obj_id = _download.remote(_type, e_id, res_url, Path(path))
            success, status = ray.get(obj_id)

            if not success:
                LOGGER.warning(f"failed: {e_id} {res_url}")

                if status:
                    LOGGER.warning(status)

            time.sleep(0.1)

    except KeyboardInterrupt:
        # this function may run for a long while and is much more
        # likely to get interrupted
        pass


def get_resources_stats (corpus: dict, pub_path: Path, dat_path: Path) -> None:
    global LOGGER

    pubs = [e for e in corpus if e["@type"] == "ResearchPublication"]
    missing_pub = set()

    downloaded_pubs = list(pub_path.glob("*.pdf"))
    downloaded_pubs_id = set([f.stem for f in downloaded_pubs])

    for entity in pubs:
        e_id = urlparse(entity["@id"]).fragment.split("-")[1]

        if e_id not in downloaded_pubs_id:
            missing_pub.add(e_id)

    LOGGER.warning(f"number of research publications: {len(pubs)}")
    LOGGER.warning(f"successfully downloaded {len(pubs) - len(missing_pub)} PDF files")
    LOGGER.debug(f"missing publication resources: {missing_pub}")

    dats = [e for e in corpus if e["@type"] == "Dataset"]
    missing_dat_res = set()

    downloaded_dats = list(dat_path.glob("*.*"))
    downloaded_dat_id = set([f.stem for f in downloaded_dats])

    for entity in dats:
        e_id = urlparse(entity["@id"]).fragment.split("-")[1]

        if e_id not in downloaded_dat_id:
            missing_dat_res.add(e_id)

    LOGGER.warning(f"number of datasets: {len(dats)}")
    LOGGER.warning(f"successfully downloaded {len(dats) - len(missing_dat_res)} resource files")
    LOGGER.debug(f"missing dataset resources: {missing_dat_res}")


def main (args) -> None:
    # load and parse the corpus
    setup_logger(args)
    corpus = load_corpus(args.input)

    # enumerate the resource files to download
    output_path = Path(args.output_dir)
    pub_path, todo_pub = enum_pub_resources(corpus, output_path, args.force)
    dat_path, todo_dat = enum_dat_resources(corpus, output_path, args.force)

    # manage the TODO file for downloads
    generate_todo(args.todo, todo_pub, todo_dat)

    if not args.todo:
        todo = todo_pub + todo_dat
    else:
        todo = load_todo(args.todo)

    # NB: when connecting to an existing cluster, instead use
    # ray.init(address=<cluster-address>)
    ray.init()

    # run the downloads and report
    download_resource_files(todo)
    get_resources_stats(corpus, pub_path, dat_path)


if __name__ == "__main__":
    # parse the command line arguments, if any
    parser = argparse.ArgumentParser(
        description="download publication PDFs and dataset foaf pages for the rclc corpus"
        )

    parser.add_argument(
        "--logger",
        type=str,
        default=DEFAULT_LOGGER_FILE,
        help="logger file"
        )

    parser.add_argument(
        "--input",
        type=str,
        default=DEFAULT_CORPUS_FILE,
        help="rclc corpus file"
        )

    parser.add_argument(
        "--todo",
        type=str,
        default=DEFAULT_TODO_LIST,
        help="download todo file"
        )

    parser.add_argument(
        "--output_dir",
        type=str,
        default=DEFAULT_OUTPUT_RESOURCE,
        help="path to store downloaded resources"
        )

    parser.add_argument(
        "--force",
        type=bool,
        default=DEFAULT_FORCE_DOWNLOAD,
        help="always download resources"
        )

    main(parser.parse_args())
