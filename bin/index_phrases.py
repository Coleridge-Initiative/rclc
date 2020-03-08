#!/usr/bin/env python
# encoding: utf-8

from collections import OrderedDict
from pathlib import Path
from tqdm import tqdm
import codecs
import json
import pytextrank
import ray
import spacy
import sys


@ray.remote
def extract_phrases (txt_file, dir_path, nlp):
    tr_path = dir_path / "tr"

    with codecs.open(txt_file, "r", encoding="utf8") as f:
        text = f.read()
        doc = nlp(text)
        view = OrderedDict()

        for phrase in doc._.phrases[:20]:
            view[phrase.text] = { "count": phrase.count, "rank_score": phrase.rank }

            file_name = txt_file.stem + ".json"
            tr_file = tr_path / file_name

            with codecs.open(tr_file, "wb", encoding="utf8") as f:
                json.dump(view, f, indent=4, ensure_ascii=False)


def main ():
    nlp = spacy.load("en_core_web_sm")
    tr = pytextrank.TextRank(logger=None)
    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)

    dir_path = Path("resources/pub")
    txt_path = dir_path / "txt"

    task_ids = []
    ray.init()

    for txt_file in tqdm(list(txt_path.glob(f"*txt")), ascii=True, desc=f"extracted text files"):
        id = extract_phrases.remote(txt_file, dir_path, nlp)
        task_ids.append(id)

    ray.get(task_ids)


if __name__ == "__main__":
    main()
