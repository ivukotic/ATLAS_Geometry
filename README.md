# ATLAS_Geometry
Code for indexing ATLAS experiment Geometry in Elasticsearch

Last Neo4j database is zipped in n.zip.
If pass is needed and it is not the default neo4j/neo4j then try with "rufo"

To get fully flattened geometry in json format (starting from the sqlite) do:
use: gm2json.py -i geometry_atlas_20Apr17.db


logvol point to shape and material (also sometimes have tag "name")
PhysVol and FullPhysVol are the only ones that can branch. PhysVol and FullPhysVol are the same thing the only difference is how Athena caches info on them.
