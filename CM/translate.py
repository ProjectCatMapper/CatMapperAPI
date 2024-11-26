import pandas as pd
from .utils import *

def translate(
        database,
        property,
        domain,
        key,
        term,
        country, 
        context ,
        dataset,
        yearStart, 
        yearEnd,
        query,
        table,
        uniqueRows):
    
    if query is not None:
        if query.lower() != 'true':
            query = 'false'

    if domain in ["ANY DOMAIN", "GENERIC"]:
        domain = "CATEGORY"
    if domain == "AREA":
        domain = "DISTRICT"
    if not key is None:
        if str.lower(key) != 'true':
            key = None
    driver = getDriver(database)

    # format data
    # add rowid, 
    # table = [{'Name':'test1',"key": 1}, {'Name':'test1',"key": 2}, {'Name':'test2',"key": 3}]
    df = pd.DataFrame(table)
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    df['CMuniqueRowID'] = df.index
    rows = pd.DataFrame({'term': df[term],'CMuniqueRowID': df["CMuniqueRowID"]})
    if isinstance(country,str) and country in df.columns:
        rows['country'] = df[country]
    if isinstance(context,str) and context in df.columns:
        rows['context'] = df[context]
    if isinstance(dataset,str) and dataset in df.columns:
        rows['dataset'] = df[dataset]
    if isinstance(yearStart,str) and yearStart is not None:
        rows['yearStart'] = yearStart
    if isinstance(yearEnd,str) and yearEnd is not None:
        rows['yearEnd'] = yearEnd
    rows.dropna(subset=['term'], inplace=True)
    rows = rows[rows['term'] != '']
    columns_to_group_by = rows.columns.difference(['CMuniqueRowID']).tolist()
    if not uniqueRows:
        rows = rows.groupby(columns_to_group_by)['CMuniqueRowID'].apply(list).reset_index()
    rows['CMuniqueCategoryID'] = rows.index

    rows = rows.to_dict('records')

    # Define the Cypher query

    qLoad = "unwind $rows as row with row call {"

    if property == "Key":
        qStart = f"""
    with row call db.index.fulltext.queryRelationships('keys','"' + tolower(row.term) +'"') yield relationship
    with row, endnode(relationship) as a, relationship.Key as matching, case when row.term contains ":" then row.term else ": " + row.term end as term
    where '{domain}' in labels(a) and matching ends with term 
    AND ( (matching CONTAINS ";" AND row.term CONTAINS ";") OR NOT matching CONTAINS ";" )
    with row, a, matching, 0 as score
    """
    elif property in ["glottocode","ISO","CMID"]:
        if property == "CMID":
            indx = "CMIDindex"
        else:
            indx = property

        qStart = f"""
    with row call db.index.fulltext.queryNodes('{indx}','"' + toupper(row.term) +'"') yield node
    with row, node as a, toupper(node['{property}']) as matching, toupper(row.term) as term
    where matching = term
    with row, a call apoc.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{{a:a}}) yield value
    with row, value.node as a, value.matching as matching, 0 as score
    """

    elif property == "Name":

        if domain != "DATASET":
            qStart = f"""
    with row call {{ with row 
    call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
    union with row 
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
    union with row 
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
    with row, node as a
    with row, a, custom.matchingDist(a.names, row.term) as matching
    with row, a, matching.matching as matching, toInteger(matching.score) as score
    """
        else:
            qStart = f"""
    with row call {{ with row 
    call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
    union with row 
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
    union with row 
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
    with row, node as a
    with row, a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], row.term) as matching
    with row, a, matching.matching as matching, toInteger(matching.score) as score
    """

    else:
        qStart = f""" 
    with row call apoc.cypher.run('match (a:{domain}) 
    where not a.{property} is null and tolower(a.{property}) = tolower(\"' + row.term + '\") 
    return a, a.{property} as matching',{{}}) yield value 
    with row, value.a as a, value.matching as matching, 0 as score
    """

    # filter by domain

    qDomain = f" where '{domain}' in labels(a) with row, a, matching, score "

    # filter by country
    if 'country' in rows[0]:
        qCountryFilter = """
    where (a)<-[:DISTRICT_OF]-(:ADM0 {CMID: row.country})
    with row, a, matching, score
    """
    else:
        qCountryFilter = " "

    # filter by context
    if 'context' in rows[0]:
        qContext = """
    where (a)<-[]-({CMID: row.context})
    with row, a, matching, score
    """
    else:
        qContext = " "

    # filter by dataset
    if 'dataset' in rows[0]:
        if property == "Key":
            qDataset = """
        match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
        where r.Key ends with row.term
        with row, a, matching, score
        """
        else: 
            qDataset = """
        match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
        with row, a, matching, score
        """
    else:
        qDataset = "with row, a, matching, score"

    # filter by year
    if 'yearStart' in rows[0] and 'yearEnd' in rows[0]:
        if domain == "DATASET":
            qYear = """
    call {with row, a with row, a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
    else a.ApplicableYears end as yearMatch, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
    with a, years, apoc.convert.toIntList(apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) as yearMatch 
    where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}
    with node as a, matching, score
    """
        else:
            qYear = f"""
    call {{with row, a with row, a, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years 
    match (a)<-[r:USES]-(:DATASET) unwind r.yearStart as yearStart 
    unwind r.yearEnd as yearEnd with years, a, r, apoc.coll.toSet(collect(yearStart) + collect(yearEnd)) as yearMatch 
    where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}}
    with row, node as a, matching, score order by score desc
    """   
    else: 
        qYear = " "

    # limit results
    qLimit = """
    with row, collect(a{a, matching, score}) as nodes, collect(score) as scores
    with row, nodes, apoc.coll.min(scores) as minScore
    unwind nodes as node
    with row, node.a as a, node.matching as matching, node.score as score, minScore
    where score = minScore
    with row, a, matching, score
    return distinct a, matching, score}
    with row, a, matching, score
    """

    # get country
    qCountry = """
    optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
    with row, a, matching, collect(c.CMName) as country, score
    """

    # get key
    if key and 'dataset' in rows[0]:
        qKey = """
        optional match (a)<-[r:USES]-(:DATASET {CMID: row.dataset})
        with row, a, matching, country, score, r.Key as Key
        """
    else: 
        qKey = "with row, a, matching, country, score, '' as Key"

    # return results
    qReturn = """
    return distinct row.CMuniqueCategoryID as CMuniqueCategoryID, row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, custom.getLabel(a) as label, 
    matching, score as matchingDistance, country, apoc.text.join(collect(Key),'; ') as Key order by matchingDistance
    """
    cypher_query = qLoad + qStart + qDomain + qCountryFilter + qContext + qDataset + qYear + qLimit + qCountry + qKey + qReturn
    if query == "true":
        qResult = getQuery(cypher_query, driver, params = {'rows': rows})
        return [{"query": cypher_query.replace("\n"," "),"params":qResult,"rows":rows}]
    else:
        data = getQuery(cypher_query, driver, params = {'rows': rows})

    if not data:
        data = df
        data[f'matchType_{term}'] = "None"
        return data

    data = pd.DataFrame(data)
    data = data.replace("", pd.NA)
    data = data.dropna(axis='columns', how='all')
  
    # return data
  
    # add matching type
    data = addMatchResults(df = data)
    data = data.drop('CMuniqueCategoryID', axis=1).copy()
    new_column_names = {col: f"{col}_{term}" for col in data.columns if col != 'CMuniqueRowID'}
    data = data.rename(columns=new_column_names)
    data = data.drop(f"term_{term}", axis=1)

    data = data.explode('CMuniqueRowID')
    df = df.explode('CMuniqueRowID')
    data['CMuniqueRowID'] = data['CMuniqueRowID'].astype(int)
    df['CMuniqueRowID'] = df['CMuniqueRowID'].astype(int)

    data = pd.merge(df, data, on="CMuniqueRowID", how='outer')
    data[f'matchType_{term}'] = data[f'matchType_{term}'].fillna('none')
    data.fillna('', inplace=True)
    dtypes = data.dtypes.to_dict()
    list_cols = []
    for col_name, typ in dtypes.items():
        if typ == 'object' and isinstance(data[col_name].iloc[0], list):
            list_cols.append(col_name)

    for col in list_cols:
        data[col] = data[col].apply(lambda x: '|'.join(map(str, x)))

    data = data.astype(str)

    colOrder = [
        term,
        f"CMID_{term}",
        f"CMName_{term}",
        f"matching_{term}",
        f"matchingDistance_{term}",
        f"label_{term}",
        f"country_{term}",
        f"Key_{term}",
        f"matchType_{term}",
        "CMuniqueRowID"
    ]


    for col in data.columns:
        if col not in colOrder:
            colOrder.append(col)


    finalColOrder = [col for col in colOrder if col in data.columns]

    data = data[finalColOrder]
    
    return data

def addMatchResults(df):
    try:
        # Initialize matchType column with None
        df['matchType'] = None

        # Count occurrences of each CMID within each CMuniqueCategoryID
        cmid_counts_per_category = df.groupby(['CMuniqueCategoryID', 'CMID']).size()

        # Count occurrences of CMuniqueRowID within each CMuniqueCategoryID
        cmunique_counts = df.groupby('CMuniqueCategoryID')['CMuniqueRowID'].transform('size')

        # Helper to assign match types
        def determine_match_type(row):
            cmid = row['CMID']
            cmunique = row['CMuniqueRowID']
            matching_distance = row['matchingDistance']
            category_id = row['CMuniqueCategoryID']
            
            # Check the count of the current CMID within the specific CMuniqueCategoryID
            cmid_count_in_category = cmid_counts_per_category.get((category_id, cmid), 0)
            cmunique_count = cmunique_counts.get(category_id, 0)
            
            # Determine match type based on conditions
            if matching_distance == 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'exact match'
            elif matching_distance > 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'fuzzy match'
            elif cmid_count_in_category > 1:
                return 'many-to-one'
            elif cmunique_count > 1:
                return 'one-to-many'
            return None

        # Apply the function to each row
        df['matchType'] = df.apply(determine_match_type, axis=1)
        
        return df

    except Exception as e:
        print(f"Error returning match statistics: {e}")
        return e

