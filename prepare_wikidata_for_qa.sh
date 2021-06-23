#!/bin/bash 
if [ "$#" -ne 2 ]; then
    echo "Error: Illegal number of parameters. Usage: ./prepare_wikidata_for_qa.sh <wikidata_dump_path> <number_of_workers>"
    exit 1
fi
WIKIDATA_DUMP_PATH=$1
NUMBER_OF_TMP_DUMPS=$2
# create needed directories
mkdir tmp_dumps
mkdir dicts
mkdir dumps
# compute tmp_dump_size
WIKIDATA_SIZE=$(stat -c%s "$WIKIDATA_DUMP_PATH")
# wikidata size of <= 13000000000 triples assumed
TMP_DUMP_LINES=$((13000000000/NUMBER_OF_TMP_DUMPS)) 
# extract identifier_predicates
python3 extract_special_predicates.py $WIKIDATA_DUMP_PATH
split -l $TMP_DUMP_LINES $WIKIDATA_DUMP_PATH tmp_dumps/wd
# filter wikidata and store result in nt-format
python3 filter_wikidata.py
# resolve qualifiers and store result in csv-format
python3 resolve_qualifiers.py