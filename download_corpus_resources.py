import argparse
import json
import requests
import time

from bs4 import BeautifulSoup  # type: ignore
from pathlib import Path
from pdfminer.pdfdocument import PDFDocument  # type: ignore
from pdfminer.pdfpage import PDFTextExtractionNotAllowed  # type: ignore
from pdfminer.pdfparser import PDFParser, PDFSyntaxError  # type: ignore
from requests_html import HTMLSession  # type: ignore
from tqdm import tqdm  # type: ignore
from urllib.parse import urlparse

DEFAULT_CORPUS_FILE = 'corpus.jsonld'
DEFAULT_OUTPUT_RESOURCE = 'resources/'

MAX_DOWNLOAD_TRIAL = 3
PUB_PDF_PATH = 'pubs/pdf/'
DATASET_PAGE_PATH = 'dataset/'


def load_corpus(filename: str) -> dict:
    """ Load corpus file (in jsonld format)
    """
    with open(filename, 'r') as f:
        corpus = json.load(f)
        return corpus['@graph']


def is_valid_pdf_file(filename: str) -> bool:
    try:
        with open(filename, 'rb') as f:
            parser = PDFParser(f)
            document = PDFDocument(parser, '')
            if not document.is_extractable:
                raise PDFTextExtractionNotAllowed(filename)
            return True
    except PDFSyntaxError as err:
        print(err)
        print(f'{filename} is not a valid pdf file.')
        return False


def _download(uri: str, res_type: str,
              output_path: Path, e_id: str) -> bool:
    """ download a resource and store in a file
    """
    if res_type not in ['pdf', 'html', 'unknown']:
        raise ValueError(f'Invalid resource type: {res_type}')
    trial = 0
    headers = requests.utils.default_headers()
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36'

    while trial < MAX_DOWNLOAD_TRIAL:
        try:
            parsed_uri = urlparse(uri)
            if parsed_uri.netloc == 'www.sciencedirect.com':
                """ Special case: sciencedirect.com auto generates pdf
                    download link in an intermediate page
                """
                session = HTMLSession()
                r0 = session.get(uri)
                res = session.get(list(r0.html.absolute_links)[0])
            elif parsed_uri.netloc.endswith('onlinelibrary.wiley.com'):
                """ Special case: wiley auto generates embed pdf to render pdf
                """
                r0 = requests.get(uri)
                soup = BeautifulSoup(r0.content, 'html5lib')
                if soup.find('embed') is None:
                    print(f'Unexpected response for: {uri}')
                    trial += 1
                    continue
                src = soup.find('embed')['src']
                res = requests.get(parsed_uri.scheme + '://' +
                                   parsed_uri.netloc + src)
            else:
                res = requests.get(uri, headers=headers, timeout=(10, 20))
            if res_type == 'unknown':
                content_type = res.headers["content-type"]
                res_type = 'html' if 'text/html' in content_type else 'pdf'
            out_file = output_path / (e_id + '.' + res_type)
            out_file.write_bytes(res.content)
            if res_type == 'pdf':
                if not is_valid_pdf_file(out_file.resolve().as_posix()):
                    out_file.unlink()
                    trial += 1
                    continue
            return True
        except requests.exceptions.RequestException as err:
            print(err)
            time.sleep(5)
            trial += 1

    if trial == MAX_DOWNLOAD_TRIAL:
        print(f'Failed downloading {uri} after {MAX_DOWNLOAD_TRIAL} attempts.')
    return False


def download_pub_resources(corpus: dict,
                           output_path: Path,
                           force_download: bool = False) -> None:
    """ Download all publications pdf files from corpus data (if not downloaded
        yet). We use the entity id as filename.
        All downloaded files are stored under `output_path` folder.
        Input:
            - corpus: corpus file containing a list of publications.
            - output_path: path to store downloaded resources
            - force_download: Always download resources if True. Default:
                              false.
    """
    pub_pdf_full_path = output_path / PUB_PDF_PATH
    if not pub_pdf_full_path.exists():
        pub_pdf_full_path.mkdir(parents=True)
    pubs = [e for e in corpus if e['@type'] == 'ResearchPublication']
    downloaded_pubs = list(pub_pdf_full_path.glob('*.pdf'))
    downloaded_pubs_id = set([f.stem for f in downloaded_pubs])

    for entity in tqdm(pubs, ascii=True, desc='Fetch resources'):
        _id = urlparse(entity['@id']).fragment.split('-')[1]
        _type = entity['@type']
        downloaded_before = _id in downloaded_pubs_id
        res_uri = entity['openAccess']['@value']
        if force_download or not downloaded_before:
            if not _download(res_uri, 'pdf',
                             pub_pdf_full_path, _id):
                print(f'Failed to download {res_uri}')
            time.sleep(0.5)


def download_dataset_resources(corpus: dict,
                               output_path: Path,
                               force_download: bool = False) -> None:
    """ Download all dataset 'foaf:page' files from corpus data (if not
        downloaded yet). We use the entity id as filename.
        All downloaded files are stored under `output_path` folder.
        Input:
            - corpus: corpus file containing a list of datasets.
            - output_path: path to store downloaded resources
            - force_download: Always download resources if True. Default:
                              false.
    """
    dataset_page_full_path = output_path / DATASET_PAGE_PATH
    if not dataset_page_full_path.exists():
        dataset_page_full_path.mkdir(parents=True)
    datasets = [e for e in corpus if e['@type'] == 'Dataset']
    downloaded_datasets = list(dataset_page_full_path.glob('*.*'))
    downloaded_datasets_id = set([f.stem for f in downloaded_datasets])

    for entity in tqdm(datasets, ascii=True, desc='Fetch resources'):
        _id = urlparse(entity['@id']).fragment.split('-')[1]
        _type = entity['@type']
        downloaded_before = _id in downloaded_datasets_id
        res_uri = entity['foaf:page']['@value']
        if force_download or not downloaded_before:
            if not _download(res_uri, 'unknown',
                             dataset_page_full_path, _id):
                print(f'Failed to download {res_uri}')
            time.sleep(0.5)


def get_resources_stats(corpus: dict, output_dir: str) -> None:
    print(f'Number of records in the corpus: {len(corpus)}')

    output_path = Path(output_dir)
    pubs = [e for e in corpus if e['@type'] == 'ResearchPublication']
    pub_pdf_full_path = output_path / PUB_PDF_PATH
    downloaded_pubs = list(pub_pdf_full_path.glob('*.pdf'))
    downloaded_pubs_id = set([f.stem for f in downloaded_pubs])
    missing_pub = set()
    for entity in pubs:
        _id = urlparse(entity['@id']).fragment.split('-')[1]
        if _id not in downloaded_pubs_id:
            missing_pub.add(_id)
    print(f'Number of research publications: {len(pubs)}')
    print(f'Successfully downloaded {len(pubs) - len(missing_pub)} pdf files.')
    print(f'Missing publication resources: {missing_pub}')

    datasets = [e for e in corpus if e['@type'] == 'Dataset']
    dataset_page_full_path = output_path / DATASET_PAGE_PATH
    downloaded_datasets = list(dataset_page_full_path.glob('*.*'))
    downloaded_datasets_id = set([f.stem for f in downloaded_datasets])
    missing_dataset_res = set()
    for entity in datasets:
        _id = urlparse(entity['@id']).fragment.split('-')[1]
        if _id not in downloaded_datasets_id:
            missing_dataset_res.add(_id)
    print(f'Number of datasets: {len(datasets)}')
    print(f'Successfully downloaded {len(datasets) - len(missing_dataset_res)} resource files.')
    print(f'Missing dataset resources: {missing_dataset_res}')


def download_resources(corpus: dict, output_dir: str) -> None:
    output_path = Path(output_dir)
    download_pub_resources(corpus, output_path)
    download_dataset_resources(corpus, output_path)


def main(args):
    corpus = load_corpus(args.input)
    download_resources(corpus, args.output_dir)
    get_resources_stats(corpus, args.output_dir)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Download publication \
                                     open access and dataset foaf page \
                                     from rclc corpus')
    parser.add_argument('--input', type=str,
                        default=DEFAULT_CORPUS_FILE,
                        help='rclc corpus file')
    parser.add_argument('--output_dir', type=str,
                        default=DEFAULT_OUTPUT_RESOURCE,
                        help='path to store downloaded resources')
    args = parser.parse_args()
    main(args)
