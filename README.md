Project to prepare a n-triples wikidata dump for QA access

Run: bash prepare_wikidata_for_qa.sh <wikidata_dump_path> <number_of_workers>
* split (2 TB dump) 			=> ~ 8h  
* extract special predicates 	=> ~ 5h
* filter_wikidata 				=> ~ 7h
* resolve_qualifiers			=> ~ 11h

Single filters (e.g. a filter pruning URLs from the dump) can be deactivated in filter_wikidata.py. Further, there is the possibility to specify additional filters (e.g. for removing qualifier information, removing facts with specific entities,...).
