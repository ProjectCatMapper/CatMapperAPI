from CM.utils import *
import pandas as pd

driver = getDriver('Sociomap')
ncontains = 2
category_label = "ETHNICITY"
dataset_choices = ["SD21", "SD14"]

if len(dataset_choices) == 1:
    print("Please select more than one dataset")

qContains = ""
qResult = ""
qWhere = ""
if ncontains > 1:
    for i in range(2, ncontains + 1):
        print(i)
        qContains = qContains + f"optional match (c)<-[:CONTAINS*..{i}]-(p{i}:CATEGORY) " 
        qResult = qResult + f", p{i}.CMID as parent{i} "
        qWhere = qWhere + f"AND not 'GENERIC' in labels(p{i}) "

query = f"""
UNWIND $datasets AS dataset
MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{category_label}) 
optional match (c)<-[:CONTAINS]-(p:CATEGORY) 
{qContains}
WHERE not "GENERIC" in labels(p) 
{qWhere}
RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID,
    apoc.text.join(apoc.coll.toSet(r.Name), '; ') AS Name, p.CMID as parent
    {qResult}
"""

merged = getQuery(query, driver = driver,params = {'datasets': dataset_choices})

merged = pd.DataFrame(merged)

cols = [col for col in merged.columns if col.startswith('parent')]

merged_long = merged.melt(id_vars=["datasetID", "Key", "Name"], 
                  value_vars=["CMID"] + cols, 
                  var_name="equivalence", value_name="CMIDs")

merged_long = merged_long.dropna(subset=['CMIDs'])

merged_df = merged_long.pivot_table(
                    index=['CMIDs', 'equivalence'],
                    columns='datasetID',
                    values=['Key', 'Name'],
                    aggfunc=lambda x: '; '.join(filter(None, set(x)))
                ).reset_index()

merged_df.columns = [
    col[0] if col[1] == "" else '_'.join(col).strip() 
    if isinstance(col, tuple) else col 
    for col in merged_df.columns
]

merged_df = merged_df.sort_values(by=["equivalence"])

merged_df.to_excel("merged_df.xlsx")

cols = ["Name_" + ds for ds in dataset_choices]
cols2 = ["Key_" + ds for ds in dataset_choices]

for col in cols:
    merged_df[col] = merged_df[col].str.split("; ") 
    merged_df = merged_df.explode(col, ignore_index=True).copy()
for col in cols2:
    merged_df[col] = merged_df[col].str.split("; ") 
    merged_df = merged_df.explode(col, ignore_index=True).copy()
for col in cols2:
    merged_df = merged_df[merged_df[col].notna()]    
for col in cols2:
    merged_df = merged_df.drop_duplicates(subset=col, keep="first")

merged_df.columns

merged_df

query2 = """
unwind $CMID as cmid 
MATCH (d:DATASET)-[r:USES]->(c:CATEGORY {CMID: cmid}) 
return distinct c.CMID as CMID, c.CMName as CMName
"""

names = getQuery(query2, driver = driver, params = {'CMID': merged_df['CMIDs'].unique().tolist()})

names = pd.DataFrame(names)

merged_df.rename(columns = {'CMIDs':'CMID'}, inplace = True)
merged_df = pd.merge(merged_df, names, how='left', on='CMID')

merged_df


# need to show all the matches for each equivalence
# LCA CMID is the CMID that is the lowest common ancestor of all the CMIDs in the equivalence group
# Number of ties is nTies
# show CMID and CMName for each Key


from CM.utils import *
database = "SocioMap"

driver = getDriver(database)
labels = getQuery("match (l:LABEL) where not l.relationship is null return distinct l.groupLabel as group, l.relationship as relationship", driver)

results = []
contains = []
for label in labels:
    relationship = label['relationship']
    group = label['group']
    matches = getQuery(f"match (p:CATEGORY)-[:{relationship}]->(c:CATEGORY)<-[r:USES]-(d:DATASET) where not '{group}' in labels(p) unwind keys(r) as property with p.CMID as parentCMID, p.CMName as parentCMName, c.CMID as childCMID, c.CNName as childCMName, r.Key as Key, d.datasetID as datasetID, d.shortName as shortName, '{relationship}' as relationship, apoc.text.join([i in labels(p) where not i in ['CATEGORY']],'; ') as domains, property, r[property] as value where parentCMID = value or parentCMID in value return distinct parentCMID, parentCMName, childCMID, childCMName, Key, datasetID, shortName, relationship, domains, property, value", driver)
    results.append(matches)
    matchContains = getQuery(f"match (p:CATEGORY)-[:CONTAINS]->(c:{group})<-[r:USES]-(d:DATASET) where not '{group}' in labels(p) and (p.CMID in r.parent or p.CMID = r.parent) return distinct p.CMID as parentCMID, p.CMName as parentCMName, c.CMID as childCMID, c.CMName as childCMName, r.Key as Key, d.CMID as datasetID, d.shortName as shortName, 'CONTAINS' as relationship, apoc.text.join([i in labels(p) where i in $groups],',') + "->" + '{group}' as domains, 'parent' as property, r.parent as value", driver, params = {'groups': list(pd.DataFrame(labels)['group'].values)})
    contains.append(matchContains)

results = pd.concat([pd.DataFrame(item) for item in results])
contains = pd.concat([pd.DataFrame(item) for item in contains])
results = pd.concat([results, contains])

if len(results) > 0:
    results = pd.DataFrame(results)

    if mail is not None:
        if results:
            fp1 = "tmp/missing_dataset.xlsx"
            results.to_excel(fp1, index = False)
            results = results.to_dict(orient="records")
            sendEmail(mail, subject = "Bad Relationship Label", recipients = ["admin@catmapper.org"], body = "See attached", sender = os.getenv("mail_default"), attachments = [fp1])         