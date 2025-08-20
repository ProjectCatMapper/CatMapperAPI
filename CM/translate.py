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
        query,
        dataset):
    
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
        domain = "ALL NODES"

    if domain == "ALL NODES":
        domain = "ALLNODES"

    # need to add check to make sure property is valid and domain is valid

    if context is not None:
        if context == "null" or context == "":
            context = None
        else:
            if re.search("^SM|^SD|^AD|^AM", context) is None:
                raise Exception("context must be a valid CMID")
            
    if dataset is not None:
        if dataset == "null" or dataset == "":
            dataset = None
        else:
            if re.search("^SD|^AD", dataset) is None:
                raise Exception("dataset must be a valid CMID")

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
        if domain == "ALLNODES":
            qStart = f"match (a) with a, '' as matching, 0 as score"
        else:
            qStart = f"match (a:{domain}) with a, '' as matching, 0 as score"

    elif property == "Key":
        qStart = f"""
    call db.index.fulltext.queryRelationships('keys','"' + custom.escapeText($term) + '"') yield relationship
    with endnode(relationship) as a, relationship.Key as matching, case when $term contains ":" then $term else ": " + $term end as term
    where '{domain}' in labels(a) and matching ends with term
    with a, matching, 0 as score
    """

    elif property == "Name":
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
    
        
    #     if domain != "DATASET":
    #         qStart = f"""
    # call {{with $term as term
    # call db.index.fulltext.queryNodes('{domain}', '"' + term +'"') yield node return node
    # union with $term as term
    # call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term)) yield node return node
    # union with $term as term
    # call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term) + '~') yield node return node}}
    # with node as a
    # with a, a.names as nameList
    # with a, nameList,  [i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText($term))] as scores
    # with a, nameList, scores, apoc.coll.min(scores) as score
    # with a, nameList[apoc.coll.indexOf(scores,score)] as matching, score
    # """

    #     else:
    #         qStart = f"""
    # call {{with $term as term
    # call db.index.fulltext.queryNodes('{domain}', '"' + term +'"') yield node return node
    # union with $term as term
    # call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term))  yield node return node
    # union with $term as term
    # call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term) + '~') yield node return node}}
    # with node as a
    # with a, [a.CMName, a.shortName, a.DatasetCitation] as nameList
    # with a, nameList, [i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText($term))] as scores
    # with a, nameList, scores, apoc.coll.min(scores) as score
    # with a, nameList[apoc.coll.indexOf(scores,score)] as matching, score
    # """

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
    if domain != "ALLNODES":
        qDomain = f" where '{domain}' in labels(a) "
    else:
        qDomain = " where any(label in labels(a) WHERE label IN ['CATEGORY', 'DATASET']) "

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

    # filter by dataset
    if dataset is not None:
        qDataset = """
    where (a)<-[:USES]-({CMID: $dataset})
    with a, matching, score
    """
    else:
        dataset = ""
        qDataset = " "

        # filter by year
    if yearStart is not None:
        if domain == "DATASET":
             qYear = f"""
                    call {{ with a with a, toInteger('{yearStart}') AS inputYearStart,toInteger('{yearEnd}') AS inputYearEnd 
                    match (a:DATASET) where a.recordStart IS NOT NULL AND a.recordStart <> ''
                    AND a.recordEnd IS NOT NULL AND a.recordEnd <> '' 
                    WITH a, toInteger(a.recordStart) AS rStart,toInteger(a.recordEnd) AS rEnd,inputYearStart, inputYearEnd
                    WHERE rStart >= inputYearStart AND rEnd <= inputYearEnd return a as node}}
                    with node as a, matching, score order by score desc
                    """
        elif domain == "ALLNODES":
            qYear = " "
        else:
            qYear = f"""
    call {{ with a with a, toInteger('{yearStart}') AS inputYearStart,toInteger('{yearEnd}') AS inputYearEnd 
    match (a)<-[r:USES]-(:DATASET) where r.recordStart IS NOT NULL AND r.recordStart <> ''
    AND r.recordEnd IS NOT NULL AND r.recordEnd <> '' 
    WITH a, toInteger(r.recordStart) AS rStart,toInteger(r.recordEnd) AS rEnd,inputYearStart, inputYearEnd
    WHERE rStart >= inputYearStart AND rEnd <= inputYearEnd return a as node}}
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
        qContext + qDataset + qYear + qLimit + qCountry + qReturn
        
    if query != 'true':
        data = getQuery(cypher_query, driver, params={
                        "term": term, "context": context, "dataset": dataset, "country": country, "yearStart": yearStart, "yearEnd": yearEnd})

        return data
    else:
        return ({"query": cypher_query, "parameters": [{"term": term, "context": context,"dataset": dataset, "country": country, "domain": domain, "yearStart": yearStart, "yearEnd": yearEnd}]})


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
    df[term] = df[term].astype(str).str.replace('~', '', regex=False)
    df[term] = df[term].astype(str).str.replace('"', '', regex=False)
    # adding the index into the dataset (as an uniqueID) to preserve original order and later joining
    df['CMuniqueRowID'] = df.index
    # creates new dataframe with term and unique row ID
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
    # drops nulls because you dont want to search for null values
    rows.dropna(subset=['term'], inplace=True)
    rows = rows[rows['term'] != '']
    # chooses all column names except for unique row ID to groupby
    columns_to_group_by = rows.columns.difference(['CMuniqueRowID']).tolist()
    # aggregates "rows" dataframe by columns to group by
    # it stores all the CMuniqueRowID's for that group(row) in a list
    rows = rows.groupby(columns_to_group_by)[
            'CMuniqueRowID'].apply(list).reset_index()
    # now adds new uniqueID for each row from the potentially new "rows" 
    # CMUniqueCategoryID clumps rows that share the same term, country, contect, dataset, yearStart and yearEnd.
    rows['CMuniqueCategoryID'] = rows.index

    rows = rows.to_dict('records')

    # Define the Cypher query

    qLoad = "unwind $rows as row with row call {"

    if property == "Key":
        # Only finds exact matches for keys, which can include cases where an inputted 173 matches with ID: 173.  
        # Additional code handles semicolons for composite keys.
        # Does not require exact case matches.
        qStart = f"""
    with row call db.index.fulltext.queryRelationships('keys','"' + tolower(row.term) +'"') yield relationship
    with row, endnode(relationship) as a, relationship.Key as matching,
    row.term AS term
    WITH row,a,matching,term,
        CASE
            WHEN term CONTAINS ':' AND matching = term THEN true
            WHEN NOT term CONTAINS ':' AND matching ENDS WITH ": " + term THEN true
            ELSE false
        END AS isMatch
    where '{domain}' in labels(a) and isMatch
    AND ( (matching CONTAINS ";" AND row.term CONTAINS ";") OR NOT matching CONTAINS ";" )
    with row, a, matching,0 as score
    """
    elif property in ["glottocode", "ISO", "CMID"]:
        if property == "CMID":
            indx = "CMIDindex"
        else:
            indx = property

        qStart = f"""
    with row call db.index.fulltext.queryNodes('{indx}','"' + toupper(row.term) + '"') yield node
    with row, node as a, toupper(node['{property}']) as matching, toupper(row.term) as term
    where matching = term
    with row, a call apoc.do.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{{a:a}}) yield value
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
        # when property is key only finds cases where USES tie includes the key.
        if property == "Key":
            qDataset = """
        match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
        where r.Key = matching
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
    with row, a, matching, collect(c.CMName) as CMcountry, score
    """

    # get key
    if key and 'dataset' in rows[0]:
        qKey = """
        optional match (a)<-[r:USES]-(:DATASET {CMID: row.dataset})
        with row, a, matching, CMcountry, score, r.Key as Key
        """
    else:
        qKey = "with row, a, matching, CMcountry, score, '' as Key"

    # return results
    qReturn = """
    return distinct row.CMuniqueCategoryID as CMuniqueCategoryID, row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, custom.getLabel(a) as label, 
    matching, score as matchingDistance,CMcountry, apoc.text.join(collect(Key),'; ') as Key order by matchingDistance
    """
    cypher_query = qLoad + qStart + qDomain + qCountryFilter + \
        qContext + qDataset + qYear + qLimit + qCountry + qKey + qReturn
    
    if query == "true":
        qResult = getQuery(cypher_query, driver, params={'rows': rows})
        return [{"query": cypher_query.replace("\n", " "), "params": qResult, "rows": rows}]
    else:
        # data contains any matching rows found by the propose translate query
        # any rows that are not matched are not included
        data = getQuery(cypher_query, driver, params={'rows': rows})
    
    
    if not data:
        data = df
        data[f'matchType_{term}'] = "None"
        return data

    data = pd.DataFrame(data)
    data = data.replace("", pd.NA)
    data = data.dropna(axis='columns', how='all')

    rows_df=pd.DataFrame.from_records(rows)

    # add matching type
    data = addMatchResults(df=data,original_df=rows_df)
    data = data.drop('CMuniqueCategoryID', axis=1).copy()
    new_column_names = {
        col: f"{col}_{term}" for col in data.columns if col != 'CMuniqueRowID'}
    data = data.rename(columns=new_column_names)
    data = data.drop(f"term_{term}", axis=1)

    data = data.explode('CMuniqueRowID')
    df = df.explode('CMuniqueRowID')
    data['CMuniqueRowID'] = data['CMuniqueRowID'].astype(int)
    df['CMuniqueRowID'] = df['CMuniqueRowID'].astype(int)

    # rejoins matches with original dataset
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

# this determines what type of match each matched row is and returns the df with matchtype column
# each row of the input spreadsheet is assigned a unique CMuniqueRowID 
# CMUniqueCategoryID clumps rows that share the same term, country, contect, dataset, yearStart and yearEnd unless uniquerows is clicked.
# Many-to-one and one-to-many are only identified within CMuniqueCategoryID
def addMatchResults(df,original_df):
    try:
        # Initialize matchType column with None
        df['matchType'] = None
        print(df)
        print(original_df)
        df = pd.merge(df,original_df,on="CMuniqueRowID",how="left")
        print(df)

        # Count how many rows each CMID is matched to but still preserves the length to grab the value later.
        # used for many-to-one
        groupby_list = ["country","yearStart","yearEnd","context"]
        columns_to_group_by = [col for col in groupby_list in df.columns]
        print(columns_to_group_by)

        # if no context variables are provided, all rows belong to same context
        if columns_to_group_by:
            context_grouping = df.groupby(columns_to_group_by)[
                'CMuniqueRowID'].apply(list).reset_index()
            print(context_grouping)
        else:
            df['context_grouping'] = 1

        df['cmid_counts_per_category'] = df['CMID'].map(df['CMID'].value_counts())
            
        
        # Count occurrences of CMuniqueRowID within each CMuniqueCategoryID
        # used for one-to-many
        cmunique_counts = df['CMuniqueRowID'].count().to_dict()
         
        # Helper to assign match types
        def determine_match_type(row):
            matching_distance = row['matchingDistance']
            category_id = row['CMuniqueCategoryID']

            # Check the count of the current CMID within the specific CMuniqueCategoryID
            cmid_count_in_category = row['cmid_counts_per_category']
            cmunique_count = cmunique_counts.get(category_id, 0)

            # Determine match type based on conditions
            if matching_distance == 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'exact match'
            elif cmunique_count > 1:
                return 'one-to-many'
            elif cmid_count_in_category > 1:
                return 'many-to-one'
            elif matching_distance > 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'fuzzy match'
            return None

        # Apply the function to each row
        df['matchType'] = df.apply(determine_match_type, axis=1)

        return df

    except Exception as e:
        print(f"Error returning match statistics: {e}")
        return e
