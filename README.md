# Wikidata Core for Question Answering

This is a joint project with Magdalena Kaiser (https://github.com/magkai) to prepare an n-triples Wikidata (https://www.wikidata.org/) dump for QA access.
The project is organized as a pipeline, so individual steps can be skipped or modified.

The original Wikidata dumps contain a large set of facts that are not
needed for common question answering use cases. For example,
Wikidata contains labels and descriptions in several languages, it
provides meta-information for internal concepts (e.g. for predicates),
and references for facts, URLs, images, or geographical coordinates.
Furthermore, identifiers for external sites such as Spotify ID, IMDb
ID, or Facebook ID are stored.
Such facts are pruned in an initial step.
Single filters (e.g. a filter pruning URLs from the dump) can be deactivated in filter_wikidata.py. 
Further, there is the possibility to specify additional filters (e.g. for removing all qualifier information, removing facts with specific entities,...).


Next, intermediate nodes (known as CVTs) used for storing contextual information for a fact (**reification**) are resolved by making each appearance of a predicate in a fact unique.
Specifically, a fact 
```
[s, p, o, qp, qo]
```
consisting of a triple and a qualifier-predicate and qualifier-object,
is represented as two triples:
```
s, p-0, o
p-0, qp-0, qo
```
`p-0` and `qp-0` are fact-specific predicates, that are only used as such for representing this fact.
When p appears in another fact, the counter would be incremented, and it would be stored as `p-1`.


The latest n-triples Wikidata dump can be downloaded at https://dumps.wikimedia.org/wikidatawiki/entities/.

## Code usage
Run: 
 ```shell
 bash prepare_wikidata_for_qa.sh <wikidata_dump_path> <number_of_workers>
 ```
wikidata_dump_path: specifies the path to the n-triples Wikidata dump.  
number_of_workers: specifies the number of processes (or workers) that are run in parallel.

## Downloads
Our filtered dumps (in csv format) are available here:
Version | Link  | 
| ---- | ----- | 
| n-triples Wikidata dump April 2020 | [wikidata_filtered_2020](http://qa.mpi-inf.mpg.de/conquer/static/wikidata_clean.zip) | 
| n-triples Wikidata dump January 2022 | [wikidata_filtered_2022](http://qa.mpi-inf.mpg.de/conquer/static/wikidata_clean_2022.zip)|


## Duration (using 13 workers)
* split (2 TB dump) 			=> ~ 8h  
* extract special predicates 	=> ~ 5h
* filter_wikidata 				=> ~ 7h
* resolve_qualifiers			=> ~ 11h

# License
This project by [Magdalena Kaiser](https://people.mpi-inf.mpg.de/~mkaiser/) and [Philipp Christmann](https://people.mpi-inf.mpg.de/~pchristm/) is licensed under MIT license.
