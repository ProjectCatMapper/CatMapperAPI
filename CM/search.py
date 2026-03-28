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
        query,
        dataset,
        contexts=None,
        context_mode="all",
        limit=None):

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

    if domain in {"ALL NODES", "ALLNODES"}:
        domain = "ALLNODES"
    else:
        domain = validate_domain_label(domain, driver=driver, extra_allowed={"ALLNODES"})

    if property is not None and property not in {"Name", "CMID", "Key"}:
        property = sanitize_cypher_identifier(property, "property")

    # need to add check to make sure property is valid and domain is valid

    normalized_contexts = []

    if isinstance(contexts, str):
        contexts = [value.strip() for value in contexts.split(",")]
    elif contexts is None:
        contexts = []
    elif not isinstance(contexts, list):
        contexts = [contexts]

    if context is not None and str(context).strip() not in {"", "null"}:
        contexts = [context] + contexts

    for raw_context in contexts:
        if raw_context is None:
            continue
        cleaned_context = str(raw_context).strip()
        if cleaned_context in {"", "null"}:
            continue
        if re.search("^SM|^SD|^AD|^AM", cleaned_context) is None:
            raise Exception("context must be a valid CMID")
        normalized_contexts.append(cleaned_context)

    # preserve order while deduplicating
    normalized_contexts = list(dict.fromkeys(normalized_contexts))

    context_mode = str(context_mode or "all").strip().lower()
    if context_mode not in {"all", "any"}:
        raise Exception("contextMode must be 'all' or 'any'")
            

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

    # convert yearStart to integer, return yearStart as None if not provided or error in conversion
    if yearStart is not None:
        try:
            yearStart = int(yearStart)
        except ValueError:
            yearStart = None

    if yearEnd is not None:
        try:
            yearEnd = int(yearEnd)
        except ValueError:
            yearEnd = None

    if yearEnd is None and yearStart is not None:
        raise Exception("must specify yearEnd property")

    if yearStart is None and yearEnd is not None:
        raise Exception("must specify yearStart property")
    
    if yearStart is not None and yearEnd is not None and yearStart > yearEnd:
        raise Exception("yearStart must be less than or equal to yearEnd")

    if limit is None or str(limit).strip().lower() in {"", "null"}:
        limit = 10000
    else:
        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise Exception("limit must be an integer")
        if limit <= 0:
            raise Exception("limit must be greater than 0")

    if property is None and term is not None:
        raise Exception("Must specify a property (e.g., Name, CMID, or Key)")

    # Define the Cypher query

    #qstart returns all matches disregarding filters

    # if no term specified
    if term is None:
        if domain == "ALLNODES":
            qStart = f"match (a) with a, '' as matching, 0 as score"
        else:
            qStart = f"match (a:{domain}) with a, '' as matching, 0 as score"

    elif property == "Key":
        domain2 = domain
        if domain == "ALLNODES":
            domain2 = "CATEGORY"

        qStart = f"""
                call db.index.fulltext.queryRelationships('keys','"' + custom.escapeText($term) + '"') yield relationship
                with endnode(relationship) as a, relationship.Key as matching, case when $term contains " == " then $term else " == " + $term end as term
                where '{domain2}' in labels(a) and matching ends with term
                with a, matching, 0 as score
                """
        
    elif property == "Name":

        qStart = f"""
            with replace(toLower(apoc.text.clean($term)), ' ', '') as cleanTerm
            call (cleanTerm) {{
                call db.index.fulltext.queryNodes('{domain}', '"' + cleanTerm + '"') yield node return node
                                union with cleanTerm
                call db.index.fulltext.queryNodes('{domain}', '*' + cleanTerm + '*') yield node return node
                union with cleanTerm
                call db.index.fulltext.queryNodes('{domain}', cleanTerm + '~') yield node return node
            }}
            with node as a
            with a, [i in a.names | apoc.text.distance(lower(i),lower($term))] as scores
            with a, scores, apoc.coll.min(scores) as score
            with a, a.names[apoc.coll.indexOf(scores,score)] as matching, score
            """

    elif property == "CMID":
        qStart = """
    match (a) where a.CMID = $term
    optional match (a)-[:IS]->(b)
    with a as sourceNode,
    case
      when "DELETED" in labels(a) and b is not null then b
      else a
    end as a
    with a, sourceNode.CMID as matching, 0 as score
    """

    else:
        qStart = f"""
    match (a) where tolower(toString(a.{property})) = tolower(toString($term))
    with a, a.{property} as matching, 0 as score
    """

    # filter by domain
    if domain != "ALLNODES":
        qDomain = f" where '{domain}' in labels(a) "
    else:
        qDomain = " where any(label in labels(a) WHERE label IN ['CATEGORY', 'DATASET']) "

    qUnique = """
    with a, collect(matching) as matchingL, 
    collect(score) as scores call (matchingL, scores) {with matchingL, 
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
    if normalized_contexts:
        context_predicate = "all" if context_mode == "all" else "any"
        qContext = f"""
    where {context_predicate}(ctx in $contexts WHERE (a)<-[]-({{CMID: ctx}}))
    with a, matching, score
    """
    else:
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
                    call (a) {{ with a, toInteger($yearStart) AS inputYearStart, toInteger($yearEnd) AS inputYearEnd
                    with a, inputYearStart, inputYearEnd,
                    [v IN (
                        apoc.coll.flatten([coalesce(a.recordStart, [])], true) +
                        apoc.coll.flatten([coalesce(a.recordEnd, [])], true)
                    ) WHERE v IS NOT NULL AND toInteger(toString(v)) IS NOT NULL | toInteger(toString(v))] AS years
                    WITH a, inputYearStart, inputYearEnd, years
                    WHERE size(years) > 0
                    AND apoc.coll.min(years) <= inputYearEnd AND apoc.coll.max(years) >= inputYearStart
                    return a as node}}
                    with node as a, matching, score order by score desc
                    """
        elif domain == "ALLNODES":
            qYear = " "
        else:
            qYear = f"""
    call (a) {{ with a, toInteger($yearStart) AS inputYearStart, toInteger($yearEnd) AS inputYearEnd
    match (a)<-[r:USES]-(:DATASET)
    WITH a, inputYearStart, inputYearEnd,
    apoc.coll.flatten(collect([coalesce(r.recordStart, [])]), true) +
    apoc.coll.flatten(collect([coalesce(r.recordEnd, [])]), true) AS rawYears
    WITH a, inputYearStart, inputYearEnd,
    [v IN rawYears WHERE v IS NOT NULL AND toInteger(toString(v)) IS NOT NULL | toInteger(toString(v))] AS years
    WHERE size(years) > 0
    AND apoc.coll.min(years) <= inputYearEnd AND apoc.coll.max(years) >= inputYearStart
    return a as node}}
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
        
    cypher_query_count = qStart + qDomain + qUnique + qCountryFilter + \
        qContext + qDataset + qYear + """WITH collect(DISTINCT a.CMID) AS allCMIDs
        RETURN size(allCMIDs) AS totalCount, allCMIDs AS CMID"""
    
    if query != 'true':

        data = getQuery(cypher_query, driver, params={
                        "term": term, "context": context, "contexts": normalized_contexts, "contextMode": context_mode, "dataset": dataset, "country": country, "yearStart": yearStart, "yearEnd": yearEnd})
        
        count = getQuery(cypher_query_count, driver, params={
                        "term": term, "context": context, "contexts": normalized_contexts, "contextMode": context_mode, "dataset": dataset, "country": country, "yearStart": yearStart, "yearEnd": yearEnd})

        return ({"data":data,"count":count})
    else:
        return ({"query": cypher_query, "parameters": [{"term": term, "context": context, "contexts": normalized_contexts, "contextMode": context_mode, "dataset": dataset, "country": country, "domain": domain, "yearStart": yearStart, "yearEnd": yearEnd}]})


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
        countsamename,
        uniqueRows=False,
        progress_callback=None,
        batch_size=None,
        cancel_checker=None):
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

    overwrite_warnings = []

    def emit_progress(percent, message, processed_rows=0, total_rows=0):
        if not callable(progress_callback):
            return
        progress_callback(
            percent=max(0, min(100, int(percent))),
            message=str(message),
            processedRows=int(processed_rows or 0),
            totalRows=int(total_rows or 0),
        )

    if isinstance(uniqueRows, str):
        if uniqueRows.lower() == 'true' or uniqueRows == True:
            uniqueRows = True
        else:
            uniqueRows = False

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
            
    if yearStart is not None:
        try:
            yearStart = int(yearStart)
        except ValueError:
            yearStart = None

    if yearEnd is not None:
        try:
            yearEnd = int(yearEnd)
        except ValueError:
            yearEnd = None

    if yearEnd is None and yearStart is not None:
        raise Exception("must specify yearEnd property")

    if yearStart is None and yearEnd is not None:
        raise Exception("must specify yearStart property")
    
    driver = getDriver(database)
    domain = validate_domain_label(domain, driver=driver)

    if property is None or str(property).strip() == "":
        raise ValueError("property is required")

    if property not in {"Key", "Name", "glottocode", "ISO", "CMID"}:
        property = sanitize_cypher_identifier(property, "property")

    emit_progress(10, "Processing input...")

    # format data
    # add rowid,
    # table = [{'Name':'test1',"key": 1}, {'Name':'test1',"key": 2}, {'Name':'test2',"key": 3}]
    df = pd.DataFrame(table)
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    df[term] = df[term].astype(str).str.replace('~', '', regex=False)
    df[term] = df[term].astype(str).str.replace('"', '', regex=False)
    # adding the index into the dataset (as an uniqueID) to preserve original order and later joining
    df['CMuniqueRowID'] = df.index
    # creates new dataframe with term and unique row ID
    ## Robert
    # Remove empty category rows
    df_valid = df[df[term].notna()].copy()
    df_valid = df_valid[df_valid[term] != '']
    df_valid = df_valid[df_valid[term].astype(str).str.strip() != '']
    df_valid = df_valid[df_valid[term] != 'None']

    rows = pd.DataFrame(
        {'term': df_valid[term], 'CMuniqueRowID': df_valid["CMuniqueRowID"]})

    if isinstance(country, str) and country in df.columns:
        rows['country'] = df_valid[country]
    if isinstance(context, str) and context in df.columns:
        rows['context'] = df_valid[context]
    if isinstance(dataset, str) and dataset in df.columns:
        rows['dataset'] = df_valid[dataset]
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
    total_rows = len(rows)
    emit_progress(20, "Preprocessing complete.", processed_rows=0, total_rows=total_rows)

    if total_rows == 0:
        data = df.copy()
        match_type_col = f'matchType_{term}'
        if match_type_col in data.columns:
            overwrite_warnings.append(
                f"Overwrote existing uploaded column: {match_type_col}"
            )
        data[match_type_col] = "none"
        desired_order = [col for col in data.columns.tolist()]
        emit_progress(90, "Processing 0 out of 0 rows.", processed_rows=0, total_rows=0)
        emit_progress(100, "Translation completed.", processed_rows=0, total_rows=0)
        return data, desired_order, overwrite_warnings

    # Define the Cypher query

    qLoad = "unwind $rows as row with row call (row) {"

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
            WHEN term CONTAINS ' == ' AND matching = term THEN true
            WHEN NOT term CONTAINS ' == ' AND matching ENDS WITH ' == ' + term THEN true
            ELSE false
        END AS isMatch
    where '{domain}' in labels(a) and isMatch
    AND ( (matching CONTAINS " && " AND row.term CONTAINS " && ") OR NOT matching CONTAINS " && " )
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
    optional match (a)-[:IS]->(b)
    with row, a as sourceNode,
    case
      when "DELETED" in labels(a) and b is not null then b
      else a
    end as a
    with row, a, sourceNode.CMID as matching, 0 as score
    """

    elif property == "Name":

        qStart = f"""
    with row
    with row, replace(toLower(apoc.text.clean(row.term)), ' ', '') as cleanTerm
    call (cleanTerm) {{with cleanTerm
                call db.index.fulltext.queryNodes('{domain}', '"' + cleanTerm + '"') yield node return node
                UNION
                with cleanTerm
                call db.index.fulltext.queryNodes('{domain}', '*' + cleanTerm + '*') yield node return node
                union with cleanTerm
                call db.index.fulltext.queryNodes('{domain}', cleanTerm + '~') yield node return node
                }}
            with node as a, row
            with a, row, [i in a.names | apoc.text.distance(lower(i),lower(row.term))] as scores
            with a, row, scores, apoc.coll.min(scores) as score
            with a, row, a.names[apoc.coll.indexOf(scores,score)] as matching, score
    """
   
    else:
        qStart = f""" 
    with row
    match (a:{domain})
    where a.{property} is not null and tolower(toString(a.{property})) = tolower(toString(row.term))
    with row, a, a.{property} as matching, 0 as score
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
    # Limits entries to recordStart and recordEnd that overlap with inputted yearRange
    if 'yearStart' in rows[0] and 'yearEnd' in rows[0]:
        if domain == "DATASET":
            qYear = """
    call (row, a) {with row, a,range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
    with a, years,
    [v IN apoc.coll.flatten([a.recordStart], true) WHERE v IS NOT NULL AND toInteger(toString(v)) IS NOT NULL | toInteger(toString(v))] +
    [v IN apoc.coll.flatten([a.recordEnd], true) WHERE v IS NOT NULL AND toInteger(toString(v)) IS NOT NULL | toInteger(toString(v))] as yearMatch
    where size([i in yearMatch where i in years]) > 0 return a as node}
    with node as a, matching, score
    """
                
        else:
            qYear = f"""
    call (row, a) {{with row, a, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
    match (a)<-[r:USES]-(:DATASET)
    with years, a,
    apoc.coll.flatten(collect(apoc.coll.flatten([r.recordStart], true)), true) +
    apoc.coll.flatten(collect(apoc.coll.flatten([r.recordEnd], true)), true) as rawYearMatch
    with years, a, [i in rawYearMatch WHERE i IS NOT NULL AND toInteger(toString(i)) IS NOT NULL | toInteger(toString(i))] as yearMatch
    where size([i in yearMatch where i in years]) > 0 return a as node}}
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
    return distinct row.CMuniqueCategoryID as CMuniqueCategoryID, row.CMuniqueRowID as CMuniqueRowID, row.term as term,row.country as country,row.context as context, a.CMID as CMID, a.CMName as CMName, custom.getLabel(a) as label, 
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
        if callable(progress_callback):
            try:
                effective_batch_size = int(batch_size) if batch_size is not None else 500
            except Exception:
                effective_batch_size = 500
            effective_batch_size = max(1, effective_batch_size)
            data = []

            if total_rows == 0:
                emit_progress(90, "Processing 0 out of 0 rows.", processed_rows=0, total_rows=0)
            else:
                for start_index in range(0, total_rows, effective_batch_size):
                    if callable(cancel_checker) and cancel_checker():
                        raise Exception("Translation cancelled by user request.")

                    batch_rows = rows[start_index:start_index + effective_batch_size]
                    target_rows = min(total_rows, start_index + len(batch_rows))
                    interval_percent = 20 + int((target_rows / total_rows) * 70)
                    emit_progress(
                        min(90, interval_percent),
                        f"Processing {target_rows} out of {total_rows} rows.",
                        processed_rows=start_index,
                        total_rows=total_rows,
                    )

                    batch_result = getQuery(cypher_query, driver, params={'rows': batch_rows})
                    if isinstance(batch_result, list) and batch_result:
                        data.extend(batch_result)
                emit_progress(90, f"Processing {total_rows} out of {total_rows} rows.", processed_rows=total_rows, total_rows=total_rows)
        else:
            data = getQuery(cypher_query, driver, params={'rows': rows})

    if not data:
        data = df
        match_type_col = f'matchType_{term}'
        if match_type_col in data.columns:
            overwrite_warnings.append(
                f"Overwrote existing uploaded column: {match_type_col}"
            )
        data[f'matchType_{term}'] = "None"
        desired_order = [col for col in data.columns.tolist()]
        emit_progress(100, "Translation completed.", processed_rows=total_rows, total_rows=total_rows)
        return data, desired_order, overwrite_warnings

    data = pd.DataFrame(data)
    data = data.replace("", pd.NA)
    data = data.dropna(axis='columns', how='all')

    # add matching type
    data = addMatchResults(df=data, countsamename=countsamename)
    data = data.drop('CMuniqueCategoryID', axis=1).copy()
    new_column_names = {
        col: f"{col}_{term}" for col in data.columns if col != 'CMuniqueRowID'}
    data = data.rename(columns=new_column_names)
    data = data.drop(f"term_{term}", axis=1)

    data = data.explode('CMuniqueRowID')
    df = df.explode('CMuniqueRowID')
    data['CMuniqueRowID'] = data['CMuniqueRowID'].astype(int)
    df['CMuniqueRowID'] = df['CMuniqueRowID'].astype(int)

    translated_columns = {col for col in data.columns if col != 'CMuniqueRowID'}
    conflicting_columns = sorted(translated_columns.intersection(set(df.columns)))
    if conflicting_columns:
        df = df.drop(columns=conflicting_columns)
        overwrite_warnings.append(
            "Overwrote existing uploaded columns with translated values: "
            + ", ".join(conflicting_columns)
        )

    # rejoins matches with original dataset
    data = pd.merge(df, data, on="CMuniqueRowID", how='outer')
    ## Robert
    if uniqueRows:
        """
        returns a single category row for each unique combination of term, country, context, dataset, yearStart, yearEnd
        """

        # stable identifying columns
        grouping_cols = [
            term,
            f"CMID_{term}",
            f"CMName_{term}",
            f"label_{term}",
            f"country_{term}" if f"country_{term}" in data.columns else None,
            f"dataset" if "dataset" in data.columns else None,
            f"context_{term}" if f"context_{term}" in data.columns else None,
            f"yearStart" if "yearStart" in data.columns else None,
            f"yearEnd" if "yearEnd" in data.columns else None,
        ]
        grouping_cols = [c for c in grouping_cols if c in data.columns]

        # separate scalar vs list columns
        scalar_cols = [c for c in data.columns if c not in grouping_cols and data[c].map(lambda x: isinstance(x, list)).any() is False]
        list_cols   = [c for c in data.columns if c not in grouping_cols and c not in scalar_cols]

        agg_dict = {c: "first" for c in scalar_cols}
        agg_dict.update({
            c: lambda x: list(dict.fromkeys(               # preserves order, removes duplicates
                i for sub in x for i in (sub if isinstance(sub, list) else [sub])
            ))
            for c in list_cols
        })

        data = (
            data.groupby(grouping_cols, dropna=False)
            .agg(agg_dict)
            .reset_index()
        )

    data[f'matchType_{term}'] = data[f'matchType_{term}'].fillna('none')
    data.fillna('', inplace=True)
    dtypes = data.dtypes.to_dict()
    list_cols = []
    for col_name, typ in dtypes.items():
        if typ == 'object' and isinstance(data[col_name].iloc[0], list):
            list_cols.append(col_name)

    for col in list_cols:
        data[col] = data[col].apply(lambda x: '; '.join(map(str, x)))

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

    desired_order = [col for col in data.columns.tolist()]

    emit_progress(100, "Translation completed.", processed_rows=total_rows, total_rows=total_rows)
    return data, desired_order, overwrite_warnings

# this determines what type of match each matched row is and returns the df with matchtype column
# each row of the input spreadsheet is assigned a unique CMuniqueRowID
# CMUniqueCategoryID clumps rows that share the same term, country, contect, dataset, yearStart and yearEnd unless uniquerows is clicked.
# Many-to-one and one-to-many are only identified within CMuniqueCategoryID


def addMatchResults(df, countsamename):
    try:
        # Initialize matchType column with None
        df['matchType'] = None

        # Count occurrences of CMuniqueRowID within each CMuniqueCategoryID
        # used for one-to-many
        cmunique_counts = df.groupby('CMuniqueCategoryID')[
            'CMuniqueRowID'].count().to_dict()

        # Count how many rows each CMID is matched to but still preserves the length to grab the value later.
        # used for many-to-one
        groupby_list = ["country", "yearStart", "yearEnd", "context"]
        columns_to_group_by = [
            col for col in groupby_list if col in df.columns]
        print(columns_to_group_by)

        # if no context variables are provided, all rows belong to same context
        if columns_to_group_by:
            df['context_grouping'] = df.groupby(
                columns_to_group_by).ngroup() + 1
        else:
            df['context_grouping'] = 1

        df['CMID_count_in_group'] = df.groupby('context_grouping')[
            'CMID'].transform(lambda x: x.map(x.value_counts()))

        # Helper to assign match types
        def determine_match_type(row):
            matching_distance = row['matchingDistance']
            category_id = row['CMuniqueCategoryID']

            # Check the count of the current CMID within the specific CMuniqueCategoryID
            cmid_count_in_category = row['CMID_count_in_group']
            cmunique_count = cmunique_counts.get(category_id, 0)

            # Determine match type based on conditions
            if matching_distance == 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'exact match'
            elif cmunique_count > 1:
                return 'one-to-many'
            elif cmid_count_in_category > 1:
                return 'many-to-one'
            elif countsamename and len(row['CMuniqueRowID']) > 1:
                return 'many-to-one'
            elif matching_distance > 0 and cmid_count_in_category == 1 and cmunique_count == 1:
                return 'fuzzy match'
            return None

        # Apply the function to each row
        df['matchType'] = df.apply(determine_match_type, axis=1)

        df = df.drop(['context_grouping', 'CMID_count_in_group'], axis=1)

        return df

    except Exception as e:
        print(f"Error returning match statistics: {e}")
        return e
