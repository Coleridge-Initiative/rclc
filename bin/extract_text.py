#!/usr/bin/env python
# encoding: utf-8

from pathlib import Path
from tqdm import tqdm
import codecs
import os
import pdfx
import ray
import sys
import time
import traceback


def extract_text (file_path):
    """                                                                                                                      
    parse text from PDF                                                                                                      
    """
    text = None
    page_count = 0

    try:
        pdf_meta = pdfx.PDFx(file_path)
        meta = pdf_meta.get_metadata()
        page_count = meta["Pages"]

        # split into sections                                                                                                   
        buf = []
        grafs = []

        for line in pdf_meta.get_text().split("\n"):
            line = line.strip()
            buf.append(line)

            if len(line) < 1:
                section = " ".join(buf).strip().replace("- ", "") + "\n"
                grafs.append(section)
                buf = []

        text = "\n".join(grafs)
    except:
        print(f"ERROR parsing {pdf_file}")
        traceback.print_exc()
    finally:
        return text, page_count


def enum_pdfs (pdf_dir, txt_dir):
    """
    enumerate all of the non-zero downloaded PDF files
    """
    uuid_set = set([])

    for pdf_file in list(pdf_dir.glob("*.pdf")):
        if os.path.getsize(pdf_file) > 0:
            uuid_set.add(pdf_file.stem)

    # filter out PDF files that have already been converted to text
    for txt_file in list(txt_dir.glob("*.txt")):
        if txt_file.stem in uuid_set and os.path.getsize(txt_file) > 0:
            uuid_set.remove(txt_file.stem)

    for uuid in uuid_set:
        yield uuid


@ray.remote
def convert_pdf (pdf_dir, txt_dir, uuid):
    t0 = time.time()
    pdf_file = pdf_dir / f"{uuid}.pdf"
    txt_file = txt_dir / f"{uuid}.txt"

    text, page_count = extract_text(pdf_file.as_posix())

    if text and len(text) > 0:
        with codecs.open(txt_file, "wb", encoding="utf8") as f:
            f.write(text)

        timing = time.time() - t0
        print("\n{} {:.3f} s".format(uuid, timing))


def main ():
    pdf_dir = Path.cwd() / "resources/pub/pdf"
    txt_dir = Path.cwd() / "resources/pub/txt"
    task_ids = []

    ray.init()

    for uuid in tqdm(enum_pdfs(pdf_dir, txt_dir), ascii=True, desc="convert pdf"):
        task_ids.append(convert_pdf.remote(pdf_dir, txt_dir, uuid))

    ray.get(task_ids)

    
if __name__ == "__main__":
    main()
