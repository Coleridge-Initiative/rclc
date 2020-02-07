#!/usr/bin/env python
# coding: utf-8

# import libraries
from parsed_json_interpreter import ParsedJsonInterpreter
import pytextrank
import networkx as nx
import operator
import json
import math
import logging
import sys
import spacy
import warnings
warnings.filterwarnings("ignore")

def Setup()
    nlp = spacy.load("en_core_web_sm")
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    logger = logging.getLogger("PyTR")
    
    # Add PyTextRank into the spaCy pipeline
    tr = pytextrank.TextRank(logger=None)
    nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)



def PhraseExtraction(current_path)
    
    RootDir = current_path + 'resource/pubs'
    json_Dir = RootDir + '/json'
    for lists in os.listdir(json_Dir): 
        json_path = json_Dir + '/' + lists
        with open(json_path, 'r', encoding='utf-8') as jsonfile:
            object = json.load(jsonfile)
       
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
                Dict[phrase.text] = {'count': phrase.count, 'rank_score': phrase.rank}
         
            Final['section_title'] = titles[i]
            Final['text_rank'] = Dict
            
            Output.append(Final)
            print('\n----------\n')
            
        print('Job Done!')
        
        outputDir = RootDir + '/TextRankJSON'
        mkdir(outputDir)
        output_path = outputDir + '/' + lists[:-9] + '.json'
        # Output the rank result to JSON 
        with open(output_path, 'w', encoding="utf-8") as outputfile:
            json.dump(Output, outputfile, indent=4, ensure_ascii=False

                      
def mkdir(path):
    # Check if the folder is already exist, if not, create a new one.
    folder = os.path.exists(path)
 
    if not folder:                   
        os.makedirs(path)
                      
                      
if __name__ == "__main__":
    
    Setup()
    
    current_path = os.path.dirname(os.path.dirname(__file__))
    PhraseExtraction(current_path)
    
    