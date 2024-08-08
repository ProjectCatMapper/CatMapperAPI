import CM
property = "religion"
relationship = "RELIGION_OF"
CMID = None
driver = CM.getDriver("SocioMap")

if property in ["country","district"]:
    qProp = "collect(distinct r.country) + collect(distinct r.district)"
    qProp2 = ""
else:
    qProp = f"collect(distinct r.{property})"
    if CMID is None:
      qProp2 = f"not r.{property} is null"  
    else:
      qProp2 = f"and not r.{property} is null"

if CMID is not None:
    qFiltera = "unwind $cmid as cmid"
    qFilterb = "a.CMID = cmid "
else: 
    qFiltera = ""
    qFilterb = ""

if qProp2 == "" and qFiltera == "":
    qWhere = ""
else:
    qWhere = "where"

query1 = f"""
{qFiltera}
match (a:CATEGORY)<-[r:USES]-(:DATASET)
{qWhere}
{qFilterb}
{qProp2}
with a, apoc.coll.toSet(apoc.coll.flatten({qProp},true)) as cmids 
match (n:CATEGORY) where n.CMID in cmids
merge (a)<-[:{relationship}]-(n)
return count(*) as count
"""

if qFiltera == "":
    qWhere = ""
else:
    qWhere = "where"

query2 = f"""
{qFiltera}
match (n)-[rel:{relationship}]->(a:CATEGORY)<-[r:USES]-(:DATASET)
{qWhere} 
{qFilterb}
with a, apoc.coll.toSet(apoc.coll.flatten({qProp},true)) as current, 
apoc.coll.toSet(apoc.coll.flatten(collect(distinct n.CMID),true)) as exists, collect(distinct rel) as rels, collect(n) as nodes
with a, rels, nodes, [i in exists where not i in current] as extra
unwind nodes as n unwind rels as rel with n,  rel, extra where n.CMID in extra and id(startNode(rel)) = id(n)
delete rel
"""

query3 = f"""
{qFiltera}
match (d:DATASET)-[rU:USES]->(a:CATEGORY)<-[rs:{relationship}]-(b:CATEGORY)
{qWhere}
{qFilterb}
unwind rU.{property} as prop
with d,a,b,rU,rs,prop
where b.CMID = prop
with distinct b, rs,a, collect(distinct d.shortName + " Key: " + rU.Key) as refKey
set rs.referenceKey = refKey
"""
data = CM.getQuery(query = query1, driver = driver, params = {'cmid': CMID}, type = "list")
CM.getQuery(query = query2, driver = driver, params = {'cmid': CMID})
CM.getQuery(query = query3, driver = driver, params = {'cmid': CMID})