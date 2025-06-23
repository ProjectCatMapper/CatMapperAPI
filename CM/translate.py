import pandas as pd
from .utils import *

def search(
        database,
        term,
        property,
        domain,
        yearStart,
        yearEnd,
        context,
        country,
        limit,
        query):

    if domain == "ANY DOMAIN":
        domain = "CATEGORY"
    if domain == "AREA":
        domain = "DISTRICT"

    driver = getDriver(database)

    if term:
        term = term.strip()

    if term == "":
        term = None

    if property == "":
        property = None

    if domain == "":
        domain = None

    if domain is None:
        domain = "CATEGORY"

    # need to add check to make sure property is valid and domain is valid

    if context is not None:
        if context == "null" or context == "":
            context = None
        else:
            if re.search("^SM|^SD|^AD|^AM", context) is None:
                raise Exception("context must be a valid CMID")

    if country is not None:
        if country == "null" or country == "":
            country = None
        else:
            if re.search("^SM|^SD|^AD|^AM", country) is None:
                raise Exception("country must be a valid CMID")

    if yearStart == "null" or yearStart == "":
        yearStart = None

    if yearEnd == "null" or yearEnd == "":
        yearEnd = None

    try:
        if yearStart is not None:
            yearStart = int(yearStart)
    except ValueError:
        raise Exception("yearStart must be an integer")

    try:
        if yearEnd is not None:
            yearEnd = int(yearEnd)
    except ValueError:
        raise Exception("yearEnd must be an integer")

    if yearEnd is None and yearStart is not None:
        raise Exception("must specify yearEnd property")

    if yearStart is None and yearEnd is not None:
        raise Exception("must specify yearStart property")

    try:
        if limit is not None:
            limit = int(limit)
    except ValueError:
        raise Exception("limit must be an integer")

    if limit is None:
        limit = 10000

    if property is None and term is not None:
        raise Exception("Must specify a property (e.g., Name, CMID, or Key)")

    # Define the Cypher query

    # if no term specified
    if term is None:
        qStart = f"match (a:{domain}) with a, '' as matching, 0 as score"
    elif property == "Key":
        qStart = f"""
    call db.index.fulltext.queryRelationships('keys','"' + custom.escapeText($term) + '"') yield relationship
    with endnode(relationship) as a, relationship.Key as matching, case when $term contains ":" then $term else ": " + $term end as term
    where '{domain}' in labels(a) and matching ends with term
    with a, matching, 0 as score
    """

    elif property == "Name":
        if domain != "DATASET":
            qStart = f"""
    call {{with $term as term
    call db.index.fulltext.queryNodes('{domain}', '"' + term +'"') yield node return node
    union with $term as term
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term)) yield node return node
    union with $term as term
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term) + '~') yield node return node}}
    with node as a
    with a, a.names as nameList
    with a, nameList,  [i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText($term))] as scores
    with a, nameList, scores, apoc.coll.min(scores) as score
    with a, nameList[apoc.coll.indexOf(scores,score)] as matching, score
    """

        else:
            qStart = f"""
    call {{with $term as term
    call db.index.fulltext.queryNodes('{domain}', '"' + term +'"') yield node return node
    union with $term as term
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term))  yield node return node
    union with $term as term
    call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term) + '~') yield node return node}}
    with node as a
    with a, [a.CMName, a.shortName, a.DatasetCitation] as nameList
    with a, nameList, [i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText($term))] as scores
    with a, nameList, scores, apoc.coll.min(scores) as score
    with a, nameList[apoc.coll.indexOf(scores,score)] as matching, score
    """
    elif property == "CMID":
        qStart = """
    match (a) where a.CMID = $term
    call apoc.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{a:a}) yield value
    with value.node as a, value.matching as matching, 0 as score
    """

    else:
        qStart = f"""
    match (a) where tolower(a.{property}) = tolower($term)
    with a, a.{property} as matching, 0 as score
    """

    # filter by domain

    qDomain = f" where '{domain}' in labels(a) "

    qUnique = """
    with a, collect(matching) as matchingL, 
    collect(score) as scores call {with matchingL, 
    scores unwind matchingL as matching 
    unwind scores as score return distinct matching, score order by score limit 1}
    with a, matching, score
    """

    # filter by country
    if country is not None:
        qCountryFilter = """
    where (a)<-[:DISTRICT_OF]-(:ADM0 {CMID: $country})
    with a, matching, score
    """
    else:
        country = ""
        qCountryFilter = " "

    # filter by context
    if context is not None:
        qContext = """
    where (a)<--({CMID: $context})
    with a, matching, score
    """
    else:
        context = ""
        qContext = " "

        # filter by year
    if yearStart is not None:
        if domain == "DATASET":
            qYear = f"""
    call {{with a where not a.ApplicableYears is null with a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
    else a.ApplicableYears end as yearMatch, range(toInteger('{yearStart}'),toInteger('{yearEnd}')) as years
    with a, years, [i in apoc.coll.toSet(apoc.coll.flatten(collect(coalesce(yearMatch,"")),true))) | toInteger(i)] as yearMatch 
    where not isEmpty([i in yearMatch where toInteger(i) in years]) return a as node}}
    with node as a, matching, score
    """
        else:
            qYear = f"""
    call {{ with a with a, range(toInteger('{yearStart}'),toInteger('{yearEnd}')) as inputYears 
    match (a)<-[r:USES]-(:DATASET) where r.yearStart is not null and not isEmpty(r.yearStart) with a, inputYears, range(apoc.coll.min([i in apoc.coll.flatten(collect(r.yearStart),true) | 
    toInteger(i)]), apoc.coll.max(custom.getYear(collect(r.yearEnd)))) as years where not isEmpty([i in inputYears where i in years]) return a as node}}
    with node as a, matching, score order by score desc
    """
    else:
        qYear = " "

    # limit results
    qLimit = f"with distinct a, matching, score order by score limit {limit} "

    # get country
    qCountry = """
    optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
    with a, matching, apoc.coll.toSet(collect(c.CMName)) as country, score
    """

    # return results
    qReturn = """
    return distinct a.CMID as CMID, a.CMName as CMName, 
    custom.getLabel(a) as domain, matching, score as matchingDistance, 
    country order by matchingDistance
    """

    cypher_query = qStart + qDomain + qUnique + qCountryFilter + \
        qContext + qYear + qLimit + qCountry + qReturn

    if query != 'true':
        data = getQuery(cypher_query, driver, params={
                        "term": term, "context": context, "country": country, "yearStart": yearStart, "yearEnd": yearEnd})

        return data
    else:
        return ({"query": cypher_query, "parameters": [{"term": term, "context": context, "country": country, "domain": domain, "yearStart": yearStart, "yearEnd": yearEnd}]})


def translate(
        database,
        property,
        domain,
        key,
        term,
        country,
        context,
        dataset,
        yearStart,
        yearEnd,
        query,
        table,
        uniqueRows):
    """
    database: Name or identifier of the target database; used to initialize a connection driver.
    property: The property or attribute to match against in the graph database (e.g., 'Name', 'Key', 'glottocode').
    domain: The graph node label/domain to limit the query scope (e.g., 'LANGUAGE', 'DATASET', 'AREA'); may be normalized internally.
    key: Optional flag (string 'true' or 'false') to include associated 'Key' values from dataset relationships in the results.
    term: The column name in the input table containing the text values to search for in the database.
    country: Optional column name from the table used to filter matches by country (ADM0 level in the graph).
    context: Optional column name from the table to filter matches by a contextual node (e.g., higher-level grouping).
    dataset: Optional column name from the table specifying dataset CMIDs used to filter matches by dataset usage.
    yearStart: Optional string or column name indicating the lower bound of the date range for filtering dataset applicability.
    yearEnd: Optional string or column name indicating the upper bound of the date range for filtering dataset applicability.
    query: Optional string ('true' or 'false') that determines whether to return the Cypher query instead of executing it.
    table: List of dictionaries representing input records to be matched; typically contains a 'term' field and other metadata.
    uniqueRows: Boolean flag that determines whether identical input rows should be grouped together or processed individually.
    """

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
    rows = pd.DataFrame(
        {'term': df[term], 'CMuniqueRowID': df["CMuniqueRowID"]})
    if isinstance(country, str) and country in df.columns:
        rows['country'] = df[country]
    if isinstance(context, str) and context in df.columns:
        rows['context'] = df[context]
    if isinstance(dataset, str) and dataset in df.columns:
        rows['dataset'] = df[dataset]
    if isinstance(yearStart, str) and yearStart is not None:
        rows['yearStart'] = yearStart
    if isinstance(yearEnd, str) and yearEnd is not None:
        rows['yearEnd'] = yearEnd
    rows.dropna(subset=['term'], inplace=True)
    rows = rows[rows['term'] != '']
    columns_to_group_by = rows.columns.difference(['CMuniqueRowID']).tolist()
    if not uniqueRows:
        rows = rows.groupby(columns_to_group_by)[
            'CMuniqueRowID'].apply(list).reset_index()
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
    elif property in ["glottocode", "ISO", "CMID"]:
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
    with row, a, a.names as nameList
    with row, a, nameList, [i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText(row.term))] as scores
    with row, a, nameList, scores, apoc.coll.min(scores) as score
    with row, a, nameList[apoc.coll.indexOf(scores,score)] as matching, score
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
    with row, a, [a.CMName, a.shortName, a.DatasetCitation] as nameList
    with row, a, nameList,  [i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText(row.term))] as scores
    with row, a, nameList, scores, apoc.coll.min(scores) as score
    with row, a, nameList[apoc.coll.indexOf(scores,score)] as matching, score
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
    cypher_query = qLoad + qStart + qDomain + qCountryFilter + \
        qContext + qDataset + qYear + qLimit + qCountry + qKey + qReturn
    if query == "true":
        qResult = getQuery(cypher_query, driver, params={'rows': rows})
        return [{"query": cypher_query.replace("\n", " "), "params": qResult, "rows": rows}]
    else:
        data = getQuery(cypher_query, driver, params={'rows': rows})

    if not data:
        data = df
        data[f'matchType_{term}'] = "None"
        return data

    data = pd.DataFrame(data)
    data = data.replace("", pd.NA)
    data = data.dropna(axis='columns', how='all')

    # return data

    # add matching type
    data = addMatchResults(df=data)
    data = data.drop('CMuniqueCategoryID', axis=1).copy()
    new_column_names = {
        col: f"{col}_{term}" for col in data.columns if col != 'CMuniqueRowID'}
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
        cmid_counts_per_category = df.groupby(
            ['CMuniqueCategoryID', 'CMID']).size()

        # Count occurrences of CMuniqueRowID within each CMuniqueCategoryID
        cmunique_counts = df.groupby('CMuniqueCategoryID')[
            'CMuniqueRowID'].count().to_dict()

        # Helper to assign match types
        def determine_match_type(row):
            cmid = row['CMID']
            cmunique = row['CMuniqueRowID']
            matching_distance = row['matchingDistance']
            category_id = row['CMuniqueCategoryID']

            # Check the count of the current CMID within the specific CMuniqueCategoryID
            cmid_count_in_category = cmid_counts_per_category.get(
                (category_id, cmid), 0)
            cmunique_count = cmunique_counts.get(category_id, 0)

            # Determine match type based on conditions
            if matching_distance == 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'exact match'
            elif cmid_count_in_category > 1:
                return 'many-to-one'
            elif cmunique_count > 1:
                return 'one-to-many'
            elif matching_distance > 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'fuzzy match'
            return None

        # Apply the function to each row
        df['matchType'] = df.apply(determine_match_type, axis=1)

        return df

    except Exception as e:
        print(f"Error returning match statistics: {e}")
        return e
