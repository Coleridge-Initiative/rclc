#!/usr/bin/env python
# encoding: utf-8

import json
import networkx as nx
import os
import rdflib
import sys
import tempfile


######################################################################
## NetworkX

LABEL = {}

def get_item_index (item):
    global LABEL
    item = str(item)

    if item not in LABEL:
        index = len(LABEL)
        LABEL[item] = index
    else:
        index = LABEL[item]

    return index


def make_nxgraph (graph):
    g = nx.Graph()

    for s, p, o in graph:
        s_idx = get_item_index(s)
        o_idx = get_item_index(o)

        print(s_idx, str(s))
        print(o_idx, str(o))

        g.graph[s_idx] = str(s)
        g.graph[o_idx] = str(o)

        e = (s_idx, o_idx)
        g.add_edge(*e)

        g[s_idx][o_idx]["label"] = str(p)

    print(g.graph)


def wrap_token (token):
    if token.startswith("http"):
        return "<{}>".format(token)
    else:
        return "\"{}\"".format(token)


PREAMBLE = """
@base <https://github.com/Coleridge-Initiative/adrf-onto/wiki/Vocabulary> .

@prefix cito:	<http://purl.org/spar/cito/> .
@prefix dct:	<http://purl.org/dc/terms/> .
@prefix foaf:	<http://xmlns.com/foaf/0.1/> .
@prefix rdf:	<http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix xsd:	<http://www.w3.org/2001/XMLSchema#> .
"""


if __name__ == "__main__":
    # load the graph
    filename = sys.argv[1]
    graph = rdflib.Graph().parse(filename, format="n3")

    # enumerate all of the relations
    term = "dataset-11a95bfc951f7d23206a"
    out_triples = set([])

    for s, p, o in graph:
        if s.endswith(term):
            out_triples.add((s, p, o,))

        elif o.endswith(term):
            out_triples.add((s, p, o,))

    ## write to in-memory file
    f = tempfile.NamedTemporaryFile(delete=False)
    f.write(PREAMBLE.encode("utf-8"))

    for s, p, o in out_triples:
        line = "{} {} {} .\n".format(wrap_token(s), wrap_token(p), wrap_token(o))
        f.write(line.encode("utf-8"))

    f.close()

    # serialize the graph as JSON-LD
    with open("vocab.json", "r") as v:
        context = json.load(v)

    graph = rdflib.Graph().parse(f.name, format="n3")
    os.unlink(f.name)

    buf = graph.serialize(format="json-ld", context=context, indent=None)
    print(buf)


