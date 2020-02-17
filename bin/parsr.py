#!/usr/bin/env python
# encoding: utf-8

from parsed_json_interpreter import mkdir
from parsr_client import ParserClient
from pathlib import Path
import codecs
import json
import os
import sys
import traceback


def Convert (base_path=".", force=False):
    config_path = Path(base_path) / "bin/sampleConfig.json"

    pub_dir = Path(base_path) / "resources/pub"

    json_dir = pub_dir / "json"
    mkdir(json_dir)

    txt_dir = pub_dir  / "txt"
    mkdir(txt_dir)

    pdf_dir = pub_dir / "pdf"

    for pdf_file in list(pdf_dir.glob("*.pdf")):
        json_file = pdf_file.stem + ".json"
        json_path = json_dir / json_file

        if json_path.exists() and not force:
            # ignore the PDFs that were already parsed
            continue

        # send document to Parsr server for processing 
        try:
            print(f"parsing {pdf_file}")

            job = parsr.send_document(
                file=pdf_file.as_posix(),
                config=config_path.as_posix(),
                wait_till_finished=True,
                save_request_id=True,
            )
        
            # output the full results in JSON
            with codecs.open(json_path, "wb", encoding="utf8") as f:
                json.dump(parsr.get_json(), f, indent=2, ensure_ascii=False)
            
            # output the raw text
            txt_file = pdf_file.stem + ".txt"
            txt_path = txt_dir / txt_file

            with codecs.open(text_path, "wb", encoding="utf8") as f:
                f.write(parsr.get_text())

        except:
            traceback.print_exc()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: parsr.py host:port")
        sys.exit(-1)

    server = sys.argv[1]
    path = os.path.dirname(os.path.dirname(__file__))

    print(f"using Parsr server {server}")
    print(f"save to path {path}")

    parsr = ParserClient(server)
    Convert(path)
