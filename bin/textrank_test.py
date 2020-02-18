#!/usr/bin/env python
# encoding: utf-8


from phrase_extraction_pipeline import setup, extract_phrases
from parsed_json_interpreter import ParsedJsonInterpreter
from pathlib import Path
import pytextrank
import codecs
import json
import spacy
import unittest


class TestVerifyTextRank (unittest.TestCase):
    EXAMPLE_TITLE = [
        "trajGANs: Using generative adversarial networks for geo-privacy protection of trajectory data (Vision paper) ",
        "1 Introduction and motivation ",
        "2 Trajectory types and data generation scenarios ",
        "3 The trajGANs framework ",
        "5 Conclusions and Discussion ",
        "References "
    ]


    EXAMPLE_TEXTRANK = [
        ["generative adversarial networks", "real data", "xi hanzhou chen1"],
        ["real data", "data", "trajectory data"],
        ["place- based trajectories", "synthetic trajectories", "human trajectories"],
        ["place- based trajectories", "synthetic trajectory samples", "synthetic trajectories"],
        ["real data", "original data", "pre-calculated statistical metrics"],
        ["generative adversarial networks", "deep convolutional generative adversarial networks", "s."]
    ]


    def setUp (self):
        '''run the example file'''
        nlp, resource_path = setup(testing=True)
        extract_phrases(nlp, resource_path, limit_keyphrase=3, verbose=False)

        tr_path = resource_path / "tr" 
        tr_file = tr_path / "PE_Example.json"

        with codecs.open(tr_file, "r", encoding="utf8") as f:
            self.example_file = json.load(f)


    def test_key_phrases (self):
        for i, section in enumerate(self.example_file):
            for j, textrank in enumerate(section["text_rank"]):
                self.assertTrue(textrank == self.EXAMPLE_TEXTRANK[i][j])


    def test_section_titles (self):
        for i, section in enumerate(self.example_file):
            self.assertTrue(section["section_title"] == self.EXAMPLE_TITLE[i])


if __name__ == "__main__":
    unittest.main()


