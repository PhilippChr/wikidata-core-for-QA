import concurrent.futures
import glob
import json
import multiprocessing as mp
import os
import pickle
import re
import sys
import threading
import time
from multiprocessing import Process
from threading import Thread

#####################################################
# SETTINGS                                          #
#####################################################
FILES = glob.glob(os.getcwd() + "/tmp_dumps/wd*")
PATH_TO_EXT_ID_PREDICATES = "dicts/identifier_predicates.pickle"
PATH_TO_GEO_PREDICATES = "dicts/geo_predicates.pickle"
PATH_TO_OUTPUT_FILE = "dumps/wikidata_clean.nt"

#####################################################
# CONSTANTS                                         #
#####################################################
LABELS_PATTERN = re.compile('".*"@((?!en)[a-z][a-z])')
ENGLISH_LABELS_PATTERN = re.compile('".*"@en')
URI_PATTERN = re.compile("[A-z]*://[A-z.-/#]+.*")
LABELS = {}
DESCRIPTIONS = {}
write_lock = threading.Lock()

with open(PATH_TO_GEO_PREDICATES, "rb") as geo_file:
    GEO_PREDS = pickle.load(geo_file)

with open(PATH_TO_EXT_ID_PREDICATES, "rb") as identifiers:
    EXT_IDS = pickle.load(identifiers)

#####################################################
# FUNCTIONS                                         #
#####################################################
def prune_triples(file, worker_id):
    """
    Stepwise filter out triples.
    In this version, you can select (comment) whatever filters you like.
    Note, that the runtime is significantly slower than the runtim
    of 'prune_triples_all'.
    Return: None
    """
    buf_triples_count = 0
    buf_triples = ""
    labels = {}
    aliases = {}
    descriptions = {}
    wikipedia_mappings = {}
    inverse_wikipedia_mappings = {}
    with open(file, "r") as fp:
        line = fp.readline()
        while line:
            currentLine = line
            # note that o is not only the object, but the object + " .", the line ending
            s, p, o = currentLine.split(" ", 2)
            line = fp.readline()
            """ 
            Extract english labels (+aliases) and descriptions.
            This needs to be done before filtering predicates,
            to ensure that predicate labels are extracted.
            Further, wikipedia mappings need to be extracted before
            skipping lines with non wikidata subjects.
            """
            # extract wikipedia mappings
            extract_wikipedia_mappings(s, p, o, wikipedia_mappings, inverse_wikipedia_mappings)
            # filter triples without a wikidata id as subject
            if filter_non_wikidata_id_subjects(s):
                continue
            # extract labels
            extract_english_labels(s, p, o, labels)
            # extract aliases
            extract_english_aliases(s, p, o, aliases)
            # extract descriptions
            extract_english_descriptions(s, p, o, descriptions)

            """ 
            Prune triples
            """
            if filter_predicates_as_subjects(s):
                continue
            if filter_schema_predicates(p):
                continue
            if filter_values(s, o):
                continue
            if filter_labels(o):
                continue
            if filter_references(s, o):
                continue
            if filter_uri_objects(o):
                continue
            if filter_non_english_labels(o):
                continue
            if filter_external_id_predicates(p):
                continue
            if filter_unknown_values(s, o):
                continue
            if filter_geo_predicates(p):
                continue
            if filter_other_objects(o):
                continue
            # if triple was not filtered out, include it into output-buffer
            buf_triples_count += 1
            buf_triples += currentLine
            # store triples, if buffer exceeded
            if buf_triples_count > 1000000:
                write_lock.acquire()
                try:
                    with open(PATH_TO_OUTPUT_FILE, "a") as output:
                        output.write(buf_triples)
                except Exception as e:
                    print(e)
                finally:
                    write_lock.release()
                    buf_triples_count = 0
                    buf_triples = ""
        # store remaining triples in buffer
        write_lock.acquire()
        try:
            with open(PATH_TO_OUTPUT_FILE, "a") as output:
                output.write(buf_triples)
        except Exception as e:
            print(e)
        finally:
            write_lock.release()

        # store labels dict for worker
        for k in labels:
            labels[k] = list(labels[k])
        with open("dicts/labels_" + str(worker_id) + ".json", "w") as outfile:
            outfile.write(json.dumps(labels, separators=(",", ":")))
        # store aliases dict for worker
        for k in aliases:
            aliases[k] = list(aliases[k])
        with open("dicts/aliases_" + str(worker_id) + ".json", "w") as outfile:
            outfile.write(json.dumps(aliases, separators=(",", ":")))
        # store description dict for worker
        with open("dicts/descriptions_" + str(worker_id) + ".json", "w") as outfile:
            outfile.write(json.dumps(descriptions, separators=(",", ":")))
        # store wikipedia_mappings dict for worker
        with open("dicts/wikipedia_mappings_" + str(worker_id) + ".json", "w") as outfile:
            outfile.write(json.dumps(wikipedia_mappings, separators=(",", ":")))
        # store inverse_wikipedia_mappings dict for worker
        with open("dicts/inverse_wikipedia_mappings_" + str(worker_id) + ".json", "w") as outfile:
            outfile.write(json.dumps(inverse_wikipedia_mappings, separators=(",", ":")))


#####################################################
# Dict Extraction                                   #
#####################################################
def extract_english_labels(s, p, o, labels):
    if re.match(ENGLISH_LABELS_PATTERN, o):
        if p == "<http://schema.org/name>" or p.endswith("altLabel"):
            s = s.rsplit("/", 1)[1][:-1]
            o = o.replace('"', "").split("@")[0]
            if labels.get(s):
                labels[s].add(o)
            else:
                labels[s] = set()
                labels[s].add(o)


def extract_english_aliases(s, p, o, aliases):
    # this method needs to be placed after the filter_non_wikidata_subjects function
    if "abel" in p and re.match(ENGLISH_LABELS_PATTERN, o):
        s = s.rsplit("/", 1)[1][:-1]
        o = o.replace('"', "").split("@")[0]
        if aliases.get(s):
            aliases[s].add(o)
        else:
            aliases[s] = set()
            aliases[s].add(o)


def extract_english_descriptions(s, p, o, descriptions):
    if p == "<http://schema.org/description>" and re.match(ENGLISH_LABELS_PATTERN, o):
        s = s.rsplit("/", 1)[1][:-1]
        o = o.replace('"', "").split("@")[0]
        descriptions[s] = o


def extract_wikipedia_mappings(s, p, o, wikipedia, inverse_wikipedia_mappings):
    if p != "<http://schema.org/about>":
        return
    if s.startswith("<https://en.wikipedia.org/wiki/") and o.startswith("<http://www.wikidata.org/entity/"):
        wikipedia_name = s.replace("<https://en.wikipedia.org/wiki/", "")[:-1]
        wikidata_id = o.rsplit("/", 1)[1][:-4]
        wikipedia[wikidata_id] = wikipedia_name
        inverse_wikipedia_mappings[wikipedia_name] = wikidata_id


##########################################################
# Filters                                                #
# more fine-grained than required for easier adjustment  #
##########################################################
def filter_non_wikidata_subjects(s):
    if not "<http://www.wikidata.org/entity/" in s:
        return True
    return False


def filter_non_wikidata_id_subjects(s):
    if "<http://www.wikidata.org/entity/" in s:
        return False
    if "<http://www.wikidata.org/entity/statement/" in s:
        return False
    return True


def filter_schema_predicates(p):
    if "<http://www.w3.org" in p:
        return True
    if "<http://wikiba.se" in p:
        return True
    if "<http://schema.org" in p:
        return True
    return False


def filter_external_id_predicates(p):
    if p.rsplit("/", 1)[1][:-1] in EXT_IDS:
        return True
    return False


def filter_uri_objects(o):
    if o.startswith("<http://www.wikidata.org"):
        return False
    if o.startswith("<http"):
        return True
    if re.match(URI_PATTERN, o):
        return True
    return False


def filter_non_english_labels(o):
    if re.match(ENGLISH_LABELS_PATTERN, o):
        return False
    if re.match(LABELS_PATTERN, o):
        return True
    return False


def filter_predicates_as_subjects(s):
    if "<http://www.wikidata.org/entity/P" in s:
        return True
    if "<http://www.wikidata.org/entity/p" in s:
        return True
    if "<http://www.wikidata.org/entity/statement/P" in s:
        return True
    if "<http://www.wikidata.org/entity/statement/p" in s:
        return True
    return False


def filter_values(s, o):
    if "<http://www.wikidata.org/value" in o:
        return True
    return False


def filter_references(s, o):
    if "<http://www.wikidata.org/reference" in o:
        return True
    return False


def filter_unknown_values(s, o):
    if "_:genid" in o:
        return True
    return False


def filter_geo_predicates(p):
    if p.rsplit("/", 1)[1][:-1] in GEO_PREDS:
        return True
    return False


def filter_other_objects(o):
    if (
        not o[0] == '"'
        and not "<http://www.wikidata.org/entity/Q" in o
        and not "<http://www.wikidata.org/entity/statement/Q" in o
    ):
        return True
    return False


def filter_labels(o):
    if re.match(LABELS_PATTERN, o):
        return True
    return False


#####################################################
# MAIN                                              #
#####################################################
if __name__ == "__main__":
    workers = len(FILES)
    start_time = time.time()
    processes = []
    #############################################################
    # Start processes for different partitions of the dump      #
    #############################################################
    for i in range(workers):
        print(FILES[i])
        p = Process(
            target=prune_triples,
            args=(
                FILES[i],
                i,
            ),
        )
        processes.append(p)
        p.start()
    #########################
    # Join all threads      #
    #########################
    for i in range(workers):
        processes[i].join()
    end = time.time()
    ###############################################
    # Merge extracted dicts from all workers      #
    ###############################################
    labels = {}
    aliases = {}
    descriptions = {}
    wikipedia_mappings = {}
    inverse_wikipedia_mappings = {}
    for i in range(workers):
        # load label dict for worker
        with open("dicts/labels_" + str(i) + ".json", "r") as file:
            tmp_labels = json.load(file)
            for k in tmp_labels:
                if labels.get(k):
                    labels[k] += tmp_labels[k]
                else:
                    labels[k] = tmp_labels[k]
        # load aliases dict for worker
        with open("dicts/aliases_" + str(i) + ".json", "r") as file:
            tmp_aliases = json.load(file)
            for k in tmp_aliases:
                if aliases.get(k):
                    aliases[k] += tmp_aliases[k]
                else:
                    aliases[k] = tmp_aliases[k]
        # load description dict for worker
        with open("dicts/descriptions_" + str(i) + ".json", "r") as file:
            tmp_description = json.load(file)
            descriptions.update(tmp_description)
        # load label dict for worker
        with open("dicts/wikipedia_mappings_" + str(i) + ".json", "r") as file:
            tmp_wikipedia_mappings = json.load(file)
            wikipedia_mappings.update(tmp_wikipedia_mappings)
        # load description dict for worker
        with open("dicts/inverse_wikipedia_mappings_" + str(i) + ".json", "r") as file:
            tmp_inverse_wikipedia_mappings = json.load(file)
            inverse_wikipedia_mappings.update(tmp_inverse_wikipedia_mappings)
        # remove worker-dicts
        os.remove("dicts/labels_" + str(i) + ".json")
        os.remove("dicts/aliases_" + str(i) + ".json")
        os.remove("dicts/descriptions_" + str(i) + ".json")
        os.remove("dicts/wikipedia_mappings_" + str(i) + ".json")
        os.remove("dicts/inverse_wikipedia_mappings_" + str(i) + ".json")
    #########################
    # Store all dicts       #
    #########################
    # store labels dict
    with open("dicts/labels_dict.json", "w") as outfile:
        outfile.write(json.dumps(labels, separators=(",", ":")))
    # store aliases dict
    with open("dicts/aliases_dict.json", "w") as outfile:
        outfile.write(json.dumps(aliases, separators=(",", ":")))
    # store description dict
    with open("dicts/descriptions_dict.json", "w") as outfile:
        outfile.write(json.dumps(descriptions, separators=(",", ":")))
    # store wikipedia_mappings dict
    with open("dicts/wikipedia_mappings.json", "w") as outfile:
        outfile.write(json.dumps(wikipedia_mappings, separators=(",", ":")))
    # store inverse_wikipedia_mappings dict
    with open("dicts/inverse_wikipedia_mappings.json", "w") as outfile:
        outfile.write(json.dumps(inverse_wikipedia_mappings, separators=(",", ":")))

    print("Time(filter_wikidata): " + str(end - start_time))
