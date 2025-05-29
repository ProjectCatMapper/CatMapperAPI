from CM import *

driver = getDriver("sociomap")

query = """
match (c:CATEGORY {CMID: $cmid})<-[r:USES {Key: $key}]-(d:DATASET {CMID: $datasetID})
return c.CMName, r.Key, d.CMID
"""

result = getQuery(query, driver, cmid = "SM250177", key = "EC: 8049", )
result
result = getQuery(query, driver, params = {"cmid": "SM250177", "key": "EC: 8049"}, datasetID= "SD11")