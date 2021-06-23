# Wikidata Core for Question Answering

This is a joint project (with Magdalena Kaiser) to prepare a n-triples Wikidata (https://www.wikidata.org/) dumps for QA access.
Triples that are not required (for most QA tasks) are filtered, and intermediate nodes (known as CVTs)
are resolved, by making predicates unique. The project is organized as a pipeline, so individual steps
can be skipped or modified.
Single filters (e.g. a filter pruning URLs from the dump) can be deactivated in filter_wikidata.py. 
Further, there is the possibility to specify additional filters (e.g. for removing qualifier information, removing facts with specific entities,...).
The latest n-triples Wikidata dump can be downloaded at https://dumps.wikimedia.org/wikidatawiki/entities/.

Run: 
 ```shell
 bash prepare_wikidata_for_qa.sh <wikidata_dump_path> <number_of_workers>
 ```
wikidata_dump_path: specifies the path to the n-triples Wikidata dump.  
number_of_workers: specifies the number of processes (or workers) that are run in parallel.

 
## Duration (using 13 workers)
* split (2 TB dump) 			=> ~ 8h  
* extract special predicates 	=> ~ 5h
* filter_wikidata 				=> ~ 7h
* resolve_qualifiers			=> ~ 11h

# License
This project by Magdalena Kaiser and Philipp Christmann is licensed under MIT license.