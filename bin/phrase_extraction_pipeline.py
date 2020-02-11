#!/usr/bin/env python
# encoding: utf-8

from parsed_json_interpreter import ParsedJsonInterpreter, mkdir
from pathlib import Path
import codecs
import json
import logging
import os
import pytextrank
import spacy
import sys


def Setup ():
    """
    add PyTextRank into the spaCy pipeline
    """
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logger = logging.getLogger("PyTR")
    
    nlp = spacy.load("en_core_web_sm")
    tr = pytextrank.TextRank(logger=None)
    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)


def PhraseExtraction (resource_path):
    """
    run PyTextRank on Parsr output to extract and rank key phrases
    """
    json_dir = resource_path / "json"

    for parse_file in list(json_dir.glob("*.json")):
        with codecs.open(parse_file, "r", encoding="utf8") as f:
            object = json.load(f)
       
        # Parse the JSON and convert it to Txt, devided by sections, and extract the title of the section
        parsr_interpreter = ParsedJsonInterpreter(object)
        sections, titles = parsr_interpreter.GetSectionalText(object)
        
        # Now run textrank and save the output 
        Output = []

        for i, section in enumerate(sections):
            Dict = {}
            Final = {}
        
            doc = nlp(section)

            for phrase in doc._.phrases[:15]:
                Dict[phrase.text] = {"count": phrase.count, "rank_score": phrase.rank}
         
            Final["section_title"] = titles[i]
            Final["text_rank"] = Dict
            
            Output.append(Final)
            print("\n----------\n")
            
        print("Job Done!")
        
        # output the rank results to JSON 
        tr_path = resource_path / "tr"
        mkdir(tr_path)

        tr_file = parse_file.stem + ".json"
        output_path = tr_path / tr_file

        with codecs.open(output_path, "wb", encoding="utf8") as f:
            json.dump(Output, f, indent=4, ensure_ascii=False)

                      
if __name__ == "__main__":
    Setup()

    # can point to different directories:
    #resource_path = Path(".") / "example/pub"		# test environment
    resource_path = Path(".") / "resources/pub"		# production use

    PhraseExtraction(resource_path)
