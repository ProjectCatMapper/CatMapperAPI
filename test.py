from CM import *
driver = getDriver("sociomap")
re.compile(r"^\s*[^=&&]+?\s*==\s*[^=&&]+?(?:\s*&&\s*[^=&&]+?\s*==\s*[^=&&]+?)*\s*$")

keys = getQuery*("MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)")