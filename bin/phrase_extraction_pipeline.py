#!/usr/bin/env python
# encoding: utf-8

from parsed_json_interpreter import ParsedJsonInterpreter, mkdir
from pathlib import Path
import codecs
import json
import os
import pytextrank
import spacy
import sys


def setup (base_path=".", testing=False):
    """
    add PyTextRank into the spaCy pipeline, then set up the input
    directory path for test vs. production env
    """
    nlp = spacy.load("en_core_web_sm")
    tr = pytextrank.TextRank(logger=None)

    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)

    if testing:
        resource_path = Path(base_path) / "example/pub"
    else:
        resource_path = Path(base_path) / "resources/pub"

    return nlp, resource_path


def extract_phrases (nlp, resource_path, limit_keyphrase=15, verbose=True):
    """
    run PyTextRank on Parsr output to extract and rank key phrases
    """
    json_dir = resource_path / "json"

    if verbose:
        print(f"scanning input directory: {json_dir}")

    for parse_file in list(json_dir.glob("*.json")):
        if verbose:
            print(f"loading {parse_file}")

        with codecs.open(parse_file, "r", encoding="utf8") as f:
            parsr_object = json.load(f)
       
        # parse the JSON and convert it to text, divided by sections,
        # then extract the title of each section
        parsr_interpreter = ParsedJsonInterpreter(parsr_object)
        sections, titles = parsr_interpreter.GetSectionalText(parsr_object)
        
        # run TextRank and collect the ranked keyphrases
        results = []

        for i, section in enumerate(sections):
            doc = nlp(section)
            phrases = {}
            final = {}

            for phrase in doc._.phrases[:limit_keyphrase]:
                phrases[phrase.text] = {"count": phrase.count, "rank_score": phrase.rank}
         
            final["section_title"] = titles[i]
            final["text_rank"] = phrases
            results.append(final)

            if verbose:
                print("section: {}".format(final["section_title"]))

        # output the ranked results to JSON 

        tr_path = resource_path / "tr"
        mkdir(tr_path)

        tr_file = parse_file.stem + ".json"
        output_path = tr_path / tr_file

        with codecs.open(output_path, "wb", encoding="utf8") as f:
            json.dump(results, f, indent=4, ensure_ascii=False)

        if verbose:
            print(f"completed: {output_path}")

                      
if __name__ == "__main__":
    nlp, resource_path = setup(testing=True)
    extract_phrases(nlp, resource_path)
