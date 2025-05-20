import os
import pandas as pd
from CM import *
database = "SocioMap"
driver = getDriver(database)
mail = None
labels = getQuery(
    "match (l:LABEL) where not l.relationship is null return distinct l.groupLabel as group, l.relationship as relationship", driver)

labels.append({'group': 'ETHNICITY', 'relationship': ''})

groups = list(set(item['group'] for item in labels))

results = []
contains = []
for label in labels:
    relationship = label['relationship']
    group = label['group']
    if group == "ETHNICITY":
        matchContains = getQuery(
            f"MATCH (p:CATEGORY)-[:CONTAINS]->(c:{group}) WHERE NOT 'GENERIC' IN labels(p) WITH p, c,[x IN labels(p) WHERE x IN {groups}] AS parentLabels,[y IN labels(c) WHERE y IN {groups}] AS childLabels UNWIND parentLabels AS parentLabel UNWIND childLabels AS childLabel WITH p, c, parentLabel, childLabel WHERE parentLabel <> '{group}' AND childLabel = '{group}' RETURN DISTINCT p.CMID AS parentCMID, p.CMName AS parentCMName, parentLabel + '->' + childLabel AS domains, c.CMID AS childCMID, c.CMName AS childCMName, 'CONTAINS' AS relationship", driver)
        contains.append(matchContains)
    else:
        matches = getQuery(f"match (p:CATEGORY)-[:{relationship}]->(c:{group})<-[r:USES]-(d:DATASET) where not '{group}' in labels(p) unwind keys(r) as property with p.CMID as parentCMID, p.CMName as parentCMName, c.CMID as childCMID, c.CNName as childCMName, r.Key as Key, d.datasetID as datasetID, d.shortName as shortName, '{relationship}' as relationship, apoc.text.join([i in labels(p) where not i in ['CATEGORY']],'; ') as domains, property, r[property] as value where parentCMID = value or parentCMID in value return distinct parentCMID, parentCMName, childCMID, childCMName, Key, datasetID, shortName, relationship, domains, property, value", driver)
        results.append(matches)

    matchContains = getQuery(
        f"MATCH (p:CATEGORY)-[:CONTAINS]->(c:CATEGORY) WHERE NOT 'GENERIC' IN labels(p) WITH p, c, [x IN labels(p) WHERE x IN {groups}] AS parentLabels, [y IN labels(c) WHERE y IN {groups}] AS childLabels UNWIND parentLabels AS parentLabel UNWIND childLabels AS childLabel WITH p, c, parentLabel, childLabel WHERE NOT parentLabel = $group AND childLabel = $group RETURN DISTINCT p.CMID AS parentCMID, p.CMName AS parentCMName, parentLabel + '->' + childLabel AS domains, c.CMID AS childCMID, c.CMName AS childCMName, 'CONTAINS' AS relationship", driver, params={'group': group})
    contains.append(matchContains)

results = pd.concat([pd.DataFrame(item) for item in results])
contains = pd.concat([pd.DataFrame(item) for item in contains])
results = pd.concat([results, contains])
results = results.drop_duplicates()

if results and isinstance(results, list) and len(results) > 0:
    if mail is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix="_bad_relationship_labels.xlsx", dir="/tmpapi") as tmpfile:
            fp1 = tmpfile.name
            results.to_excel(fp1, index=False)
        sendEmail(mail, subject="Bad Relationship Label", recipients=[
            "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

    # return {"bad_relationship_labels_count": len(results), "bad_relationship_labels": results.to_dict(orient="records")}
