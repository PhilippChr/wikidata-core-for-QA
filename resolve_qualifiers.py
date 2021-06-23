import csv
import json
import re

#####################################################
# CONSTANTS                                         #
#####################################################
DUMP_SPECIFICATION = "wikidata_clean"
PATH_TO_INPUT_FILE = "dumps/" + DUMP_SPECIFICATION + ".nt"
PATH_TO_UNIQUE_PREDICATES_DUMP = "tmp_dumps//" + DUMP_SPECIFICATION + "_unique_predicates.csv"
PATH_TO_QUALIFIER_DUMP = "tmp_dumps//" + DUMP_SPECIFICATION + "_qualifiers_resolved.csv"
PATH_TO_OUTPUT_FILE = "dumps/" + DUMP_SPECIFICATION + "_old.csv"

TYPE_PATTERN = re.compile("Q[0-9]+\-[0-9]+")

#####################################################
# FUNCTIONS                                         #
#####################################################
def normalize_object(kg_object):
    if "http://www.wikidata.org" in kg_object:
        return kg_object.rsplit("/", 1)[1]
    if '"^^' in kg_object:
        kg_object = kg_object.rsplit('"^^', 1)[0]
        kg_object += '"'
    if '"@en' in kg_object:
        kg_object = kg_object.replace('"@en', "")
        kg_object += '"'
    o = kg_object
    return o


def normalize_wikidata_url(kg_item):
    return kg_item.rsplit("/", 1)[1]


def create_unique_predicates():
    predicate_nodes = dict()
    type_nodes = dict()
    intermediate_nodes = dict()
    with open(PATH_TO_INPUT_FILE, "r") as fp_in:
        with open(PATH_TO_UNIQUE_PREDICATES_DUMP, "w") as fp_out:
            line = fp_in.readline()
            rows = ""
            count = -1
            while line:
                currentLine = line
                line = fp_in.readline()
                s, p, o = currentLine.replace(">", "").replace("<", "").split(" ", 2)
                # remove " .\n" at end of each triple from object
                o = o[:-3]
                # remove prefix-url from kg item
                s, p, o = normalize_wikidata_url(s), normalize_wikidata_url(p), normalize_object(o)
                o = o.strip()
                # create unique type nodes
                if p == "P31":
                    if not type_nodes.get(o):
                        type_nodes[o] = 1
                        type_index = 0
                    else:
                        type_index = type_nodes[o]
                        type_nodes[o] += 1
                    o = o + "-" + str(type_index)
                # create unique predicate nodes
                if not predicate_nodes.get(p):
                    predicate_nodes[p] = 1
                    predicate_index = 0
                else:
                    predicate_index = predicate_nodes[p]
                    predicate_nodes[p] += 1

                p = p + "-" + str(predicate_index)
                # check for statements
                if "-" in o:
                    if o.startswith(s) or o.startswith("q" + s[1:]):
                        # store in dict dummy node and unique predicate, remove this line from dump
                        intermediate_nodes[o] = p
                        continue
                    if o.startswith("p" + s[1:]):
                        continue

                count += 1
                rows += str(s) + "," + str(p) + "," + str(o) + "\n"
                if count == 1000:
                    count = 0
                    fp_out.write(rows)
                    rows = ""

            fp_out.write(rows)
            with open("tmp_dumps/qualifier_intermediate_nodes.json", "w") as json_file:
                json.dump(intermediate_nodes, json_file)
    return


def resolve_qualifiers():
    triples = dict()
    with open("tmp_dumps/qualifier_intermediate_nodes.json", "r") as json_file:
        intermediate_nodes = json.load(json_file)
    with open(PATH_TO_UNIQUE_PREDICATES_DUMP, "r") as fp_in:
        with open(PATH_TO_QUALIFIER_DUMP, "w") as fp_out:
            line = fp_in.readline()
            rows = ""
            count = -1
            while line:
                currentLine = line
                line = fp_in.readline()
                s, p, o = currentLine.split(",", 2)
                o = o.strip()
                # check for statement
                if "-" in s:
                    # look up corresponding predicate in dictionary, if not contained in dict: 00 will be returned
                    p_val = intermediate_nodes.get(s, "00")
                    # actually a lot of dummy nodes as subject are apparently not as objects in dump due to earlier pruning (see csv.log for a list of them)
                    if p_val == "00":
                        continue
                    # if qualifier predicate and predicate from dict are the same: create the corresponding triple from this
                    if p.split("-")[0] == p_val.split("-")[0]:
                        s = s.split("-")[0]
                        if s.startswith("q"):
                            s = "Q" + s[1:]
                        p = p_val
                        # store in new dictionary subject, object, predicate (later used to prune direct triples)
                        if not s in triples.keys():
                            triples[s] = dict()
                        triples[s][o] = p_val
                    # if not: add predicate as subject of this triple
                    else:
                        s = p_val

                count += 1
                rows += str(s) + "," + str(p) + "," + str(o) + "\n"
                if count == 1000:
                    count = 0
                    fp_out.write(rows)
                    rows = ""

            fp_out.write(rows)
            with open("tmp_dumps/qualifier_triples.json", "w") as json_file:
                json.dump(triples, json_file)
    return


def prune_duplicate_lines():
    continue_flag = False
    with open("tmp_dumps/qualifier_triples.json", "r") as json_file:
        triples = json.load(json_file)
    with open(PATH_TO_QUALIFIER_DUMP, "r") as fp_in:
        with open(PATH_TO_OUTPUT_FILE, "w") as fp_out:
            line = fp_in.readline()
            rows = ""
            count = -1
            while line:
                currentLine = line
                line = fp_in.readline()
                s, p, o = currentLine.split(",", 2)
                o = o.strip()
                if s in triples.keys():
                    for o_s in triples[s].keys():
                        if o == o_s:
                            # get stored predicate
                            p_s = triples[s][o_s]
                            # if same: we are at qualifier predicate, otherwise: direct predicate which can be pruned
                            # if p == p_s:
                            # continue_flag = True
                            if not p == p_s and p.split("-")[0] == p_s.split("-")[0]:
                                continue_flag = True
                        elif p.split("-")[0] == "P31":
                            if o_s.split("-")[0] == o.split("-")[0]:
                                p_s = triples[s][o_s]
                                if not p_s == p:
                                    if p.split("-")[0] == p_s.split("-")[0]:
                                        continue_flag = True
                    if continue_flag:
                        continue_flag = False
                        continue
                count += 1
                rows += str(s) + "," + str(p) + "," + str(o) + "\n"
                if count == 1000:
                    count = 0
                    fp_out.write(rows)
                    rows = ""
            fp_out.write(rows)
    return


create_unique_predicates()
resolve_qualifiers()
prune_duplicate_lines()
print("done")
