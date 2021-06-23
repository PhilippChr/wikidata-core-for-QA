'''
Extract identifier predicates and geo predicates.
Such predicates (and corresponding facts) are removed from the dump.
'''

import pickle
import sys

#####################################################
# CONSTANTS											#
#####################################################
PATH_TO_EXT_ID_PREDICATES="dicts/identifier_predicates.pickle"
PATH_TO_GEO_PREDICATES="dicts/geo_predicates.pickle"

#####################################################
# FUNCTIONS											#
#####################################################
def extract_special_predicates(file, ext_id_predicates, geo_predicates):
	'''
	Extract special (unnecessary) predicates from the wikidata dump.
	'''
	with open(file, "r") as fp:
		line = fp.readline()
		while line:
			currentLine = line
			s,p,o = currentLine.split(" ", 2)
			line = fp.readline()
			# Remove " .\n", the line ending from o
			if len(o) < 35:
				continue
			o = o[:-3]
			# Extract external identifier predicates
			if p == "<http://wikiba.se/ontology#propertyType>" and o == "<http://wikiba.se/ontology#ExternalId>":
				ext_id_pred = s.rsplit("/", 1)[1].replace(">", "")
				if not ext_id_pred in ext_id_predicates:
					ext_id_predicates.append(ext_id_pred)
			# Extract geo predicates (for geo shapes)
			if p == "<http://wikiba.se/ontology#propertyType>" and o == "<http://wikiba.se/ontology#GeoShape>":
				geo_pred = s.rsplit("/", 1)[1].replace(">", "")
				if not geo_pred in geo_predicates:
					geo_predicates.append(geo_pred)
			# Extract geo predicates (for geo coordinates)
			if p == "<http://wikiba.se/ontology#propertyType>" and o == "<http://wikiba.se/ontology#GlobeCoordinate>":
				geo_pred = s.rsplit("/", 1)[1].replace(">", "")
				if not geo_pred in geo_predicates:
					geo_predicates.append(geo_pred)

def store_special_predicates(special_predicates, path):
	'''
	Store the special predicates on the given path.
	Return: None
	'''
	with open(path, 'wb') as output:
		pickle.dump(special_predicates, output, protocol=pickle.HIGHEST_PROTOCOL)

#####################################################
# MAIN												#
#####################################################
if __name__ == '__main__':
	if len(sys.argv) != 2:
		print('Usage: extract_identifier_predicates.py <wikidata_dump_path>')
		sys.exit()
	WIKIDATA_DUMP_PATH = sys.argv[1]
	ext_id_predicates = list()
	geo_predicates = list()
	extract_special_predicates(WIKIDATA_DUMP_PATH, ext_id_predicates, geo_predicates)
	store_special_predicates(ext_id_predicates, PATH_TO_EXT_ID_PREDICATES)
	store_special_predicates(geo_predicates, PATH_TO_GEO_PREDICATES)