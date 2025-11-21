''' routines.py '''

# This is a module for automatic routines in CatMapper

from time import time
from neo4j import GraphDatabase
from CM.utils import *
from CM.email import *
from CM.USES import *
import pandas as pd
import json
import tempfile
from flask import Response, stream_with_context
from flask_mail import Mail
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

def is_valid_json(json_string):
    """
    Helper function to check if a string is valid JSON.
    """
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False


def validateJSON(database, property='parentContext', path="/mnt/storage/app/tmp/invalid_json.xlsx"):
    """
    Helper function to validate JSON properties in a Neo4j database.
    """
    try:

        driver = getDriver(database)

        query = f"match (c:CATEGORY)<-[r:USES]-(d:DATASET) where not r.{property} is null return d.CMID as datasetID, c.CMID as CMID, r.Key as Key, r.{property} as prop"

        results = getQuery(query, driver)

        results = pd.DataFrame(results)

        results = pd.DataFrame.explode(results, "prop")

        results["is_valid_json"] = results["prop"].apply(is_valid_json)

        invalid = results[results["is_valid_json"] == False]

        invalid.to_excel(path)

        invalid = invalid.to_dict(orient="records")

        return (invalid)

    except Exception as e:
        return "Unable to validate JSON properties: " + str(e)


def checkDomains(database, mail=None, return_type="data"):
    """
    Check for missing or inconsistent domain and subdomain labels 
    in a Neo4j database, and optionally export results to Excel or send via email.

    This function runs a series of Cypher queries to identify four categories of errors:
      1. Nodes labeled only as `CATEGORY` (no additional labels).
      2. Nodes missing the `CATEGORY` label that are used in `USES` relationships.
      3. Subdomains in `USES` relationships that are not present in the category's labels.
      4. Domains inferred from `LABEL` nodes that are not present in the category's labels.

    Errors are returned as either structured data or summary info. If errors exist,
    the results are also written to an Excel file in `/tmp`. If a valid `Mail` object 
    is provided, the Excel file is attached and sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, an email with the results file attached is sent to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return results as a list of dictionaries (records).
        - "info" : return a dictionary with:
            * "info": number of errors found
            * "filepath": path to the generated Excel file (or None if no file was created)

    Returns
    -------
    list of dict or dict or str
        If return_type == "data":
            A list of dictionaries, each containing:
                - query: type of error ("CATEGORY", "MissingCATEGORY", "MissingSubDomain", "MissingDomain")
                - CMID: category ID
                - CMName: category name
                - subdomain: subdomain label (if present)
                - domain: domain label (if present)
                - datasetID: dataset ID
        If return_type == "info":
            A dict with summary info and the file path to the exported Excel file.
        If an error occurs:
            A string containing the exception message.

    Side Effects
    ------------
    - Creates a temporary Excel file under `/tmp` if errors are found.
    - Sends an email with the results file attached if a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as strings.
    """
    try:
        driver = getDriver(database)
        query = """
        match (n:CATEGORY)<-[r:USES]-(d:DATASET)
        where size(labels(n)) = 1
        return "CATEGORY" as query, n.CMID as CMID, n.CMName as CMName, r.label as subdomain, '' as domain, d.CMID as datasetID
        UNION ALL
        match (n)<-[r:USES]-(d:DATASET)
        where not "CATEGORY" in labels(n)
        return "MissingCATEGORY" as query,  n.CMID as CMID, n.CMName as CMName, r.label as subdomain, '' as domain, d.CMID as datasetID
        UNION ALL
        match (n:CATEGORY)<-[r:USES]-(d:DATASET)
        where r.label is not null and apoc.meta.cypher.type(r.label) = "STRING" and not r.label in labels(n)
        return "MissingSubDomain" as query, n.CMID as CMID, n.CMName as CMName, r.label as subdomain, '' as domain, d.CMID as datasetID
        UNION ALL
        match (p:LABEL)
        WITH apoc.map.fromPairs([[p.CMName, p.groupLabel]]) AS m
        WITH collect(m) AS labelGroupMap
        match (n:CATEGORY)<-[r:USES]-(d:DATASET)
        where r.label is not null and apoc.meta.cypher.type(r.label) = "STRING"
        with n,r,d,labelGroupMap
        unwind labelGroupMap as labelGroup
        with n,r,d,labelGroup
        where keys(labelGroup)[0] = r.label
        with n,r,d,labelGroup
        where not labelGroup[r.label] in labels(n)
        return "MissingDomain" as query, n.CMID as CMID, n.CMName as CMName, r.label as subdomain, labelGroup[r.label] as domain, d.CMID as datasetID
        """
        results = getQuery(query, driver, type ="df")
        fp1 = None
        
        if isinstance(results, pd.DataFrame) and not results.empty:
            with tempfile.NamedTemporaryFile(delete=False, 
                                             prefix=f"missingDomains{database}_",
                                             suffix=f".xlsx", dir="/tmp") as tmpfile:
                fp1 = tmpfile.name
                results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Missing Domains for {database}", recipients=[
                            "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
        if return_type == "data":        
            return results.to_dict(orient="records")
        elif return_type == "info":
            return {"info": str(len(results)), "filepath": fp1}

    except Exception as e:
        return str(e)


# need to create function based off this
# with "eventType" as prop match (a)-[r]->(b) where not r[prop] is null and r[prop] = [] call apoc.cypher.doIt("with r set r." + prop + " = NULL",{r:r}) yield value return count(*)

def backup2CSV(database, mail=None):
    """
    Export key entities and relationships from a Neo4j database to CSV files 
    using APOC procedures, and optionally send a notification email.

    This function generates CSV backups of the following components:
      - **DATASET** nodes and their properties
      - **CATEGORY** nodes, their labels, and names
      - **USES** relationships between datasets and categories
      - **DELETED** nodes and their corresponding replacement nodes
      - **METADATA** nodes and their properties
      - **MERGING** structures (merging nodes, stacks, datasets, categories, 
        equivalents, and transformation properties)

    Each component is exported into a dated CSV file under 
    `/backups/download/` using `apoc.export.csv.query`.

    Parameters
    ----------
    database : str
        The name of the database to back up.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, a completion email is sent to `rjbischo@catmapper.org` 
        after the exports are generated.

    Returns
    -------
    str
        A message confirming successful completion, e.g.:
        `"backup2CSV completed for <database>"`.
        If an error occurs, a string containing the exception message.

    Side Effects
    ------------
    - Creates multiple CSV files in `/backups/download/`, one per exported 
      component.
    - Optionally sends a notification email after backup completion.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages.
    """
    try:
        driver = getDriver(database)

        results = [database]
        query_datasets = """
            with 'match (d:DATASET) unwind keys(d) as property return distinct elementId(d) as nodeID, property, d[property] as value' as query CALL apoc.export.csv.query(query, '/backups/download/datasetNodes_' + toString(date()) + '.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
        """

        datasets = getQuery(query_datasets, driver)
        results.append(datasets)

        query_CATEGORIES = """
            with 'match (d:CATEGORY) unwind keys(d) as property return distinct elementId(d) as nodeID, apoc.text.join(labels(d),"; ") as label, apoc.text.join(d.names,"; ") as names, property, d[property] as value' as query CALL apoc.export.csv.query(query, '/backups/download/categoryNodes_' + toString(date()) + '.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
        """

        CATEGORIES = getQuery(query_CATEGORIES, driver)
        results.append(CATEGORIES)

        query_USES = """
            with 'match (n:CATEGORY)<-[r:USES]-(d:DATASET) 
            unwind keys(r) as property with n,r,d, property where not property = "logID" return distinct elementId(r) as relID, n.CMID as CMID, n.CMName as CMName, d.CMName as dataset, d.CMID as datasetID, property, r[property] as value order by CMName' as query CALL apoc.export.csv.query(query, '/backups/download/USESties_' + toString(date()) + '.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
            """

        USES = getQuery(query_USES, driver)
        results.append(USES)

        query_DELETED = """
            with 'match (d:DELETED) optional match (d)-[:IS]-(now) unwind keys(d) as property return distinct elementId(d) as nodeID, elementId(now) as newNodeID, d.CMID as CMID, now.CMID as newCMID' as query CALL apoc.export.csv.query(query, '/backups/download/deletedNodes_' + toString(date()) + '.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
            """
        DELETED = getQuery(query_DELETED, driver)
        results.append(DELETED)
        
        query_Metadata = """
            with 'match (m:METADATA) unwind keys(m) as property return distinct elementId(m) as nodeID, m.CMID as CMID, m.CMName as CMName, property, m[property] as value' as query CALL apoc.export.csv.query(query, '/backups/download/metadata_' + toString(date()) + '.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
            """
        Metadata = getQuery(query_Metadata, driver)
        results.append(Metadata)
        
        query_Merging = """
        with '
        MATCH (m:MERGING)-[:MERGING]->(s:STACK)-[:MERGING]->(d:DATASET)-[ru:USES]->(c:CATEGORY) OPTIONAL MATCH (c)-[:EQUIVALENT]->(e:CATEGORY) OPTIONAL MATCH (s)-[rm:MERGING]->(c) RETURN m.CMID as mergingID, m.CMName as mergingName, s.CMID as stackID, s.CMName as stackName, d.CMID as datasetID, d.CMName as datasetName, head(apoc.coll.flatten(collect(rm.varName),true)) as varName, rm.transform as transform, rm.Rtransform as Rtransform, rm.Rfunction as Rfunction, rm.summaryStatistic as summaryStatistic, custom.getLabel(c) as domain, c.CMID as CMID, c.CMName as CMName, ru.Key as Key, e.CMID as equivalentCMID, e.CMName as equivalentName ORDER BY mergingName, stackName, datasetName, domain, CMName
        UNION ALL
        MATCH (m:MERGING)-[:MERGING]->(d:DATASET)-[ru:USES]->(c:CATEGORY) OPTIONAL MATCH (c)-[:EQUIVALENT]->(e:CATEGORY) OPTIONAL MATCH (m)-[rm:MERGING]->(c) RETURN m.CMID as mergingID, m.CMName as mergingName, "" as stackID, "" as stackName, d.CMID as datasetID, d.CMName as datasetName, head(apoc.coll.flatten(collect(rm.varName),true)) as varName, rm.transform as transform, rm.Rtransform as Rtransform, rm.Rfunction as Rfunction, rm.summaryStatistic as summaryStatistic, custom.getLabel(c) as domain, c.CMID as CMID, c.CMName as CMName, ru.Key as Key, e.CMID as equivalentCMID, e.CMName as equivalentName ORDER BY mergingName, stackName, datasetName, domain, CMName
        ' as query CALL apoc.export.csv.query(query, '/backups/download/merging_' + toString(date()) + '.csv', {})
        YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
        RETURN count(*);
        """
        Merging = getQuery(query_Merging, driver)
        results.append(Merging)

        # query_LOGS = """
        #     with 'match (l:LOG) unwind keys(l) as property return distinct elementId(l) as nodeID, property, l[property] as value' as query CALL apoc.export.csv.query(query, '/backups/download/logs_' + toString(date()) + '.csv', {})
        #     YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
        #     RETURN count(*);
        # """
        # LOGS = getQuery(query_LOGS, driver)
        # print(LOGS)

        if isinstance(mail, Mail):
            sendEmail(mail, subject=f"Weekly CSV Backup {database}", recipients=[
                      "rjbischo@catmapper.org"], body="Weekly CSV backup completed.", sender=config['MAIL']['mail_default'])

        return f"backup2CSV completed for {database}"

    except Exception as e:
        return str(e)


def getBadCMID(database, mail=None, return_type="data"):
    """
    Identify invalid or outdated CMIDs used in dataset relationships within a Neo4j database,
    and optionally export results to Excel or send via email.

    This function iterates over all relationship properties in the graph metadata, checking
    whether CMIDs referenced in `USES` relationships correspond to valid `CATEGORY` nodes.
    It also detects CMIDs that have been deleted and replaced, returning the mapping to
    their current valid IDs when available.

    Results can be returned as structured data, summary information, and/or saved to an
    Excel file. If a valid `Mail` object is provided, the file is also sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, an email with the results file attached is sent to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return results as a list of dictionaries (records).
        - "info" : return a dictionary with:
            * "info": number of invalid CMIDs found
            * "filepath": path to the generated Excel file (or None if no file was created)

    Returns
    -------
    list of dict or dict or str
        If return_type == "data":
            A list of dictionaries, each containing:
                - CMID: category ID
                - CMName: category name
                - Key: relationship key
                - datasetID: dataset ID
                - dataset: dataset name
                - propertyType: the relationship property type checked
                - property: list of CMIDs in the relationship
                - badCMID: invalid CMID
                - newCMID (optional): replacement CMID if found in the `DELETED` mapping
        If return_type == "info":
            A dict with summary info and the file path to the exported Excel file.
        If no invalid CMIDs are found:
            "No bad CMIDs found" (if return_type == "data"),
            or {"info": 0, "filepath": None} (if return_type == "info").
        If an error occurs:
            A string starting with "Error: " containing the exception message.

    Side Effects
    ------------
    - Creates a temporary Excel file under `/tmp` if bad CMIDs are found.
    - Sends an email with the results file attached if a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as strings.
    """
    try:

        driver = getDriver(database)
        properties = getPropertiesMetadata(driver)
        properties = pd.DataFrame(properties)
        properties = properties[properties["relationship"].notna()]

        results = []

        for property in properties["property"]:
            print("Checking CMIDs for property: ", property)

            query = f"""
            MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
            with apoc.coll.toSet(apoc.coll.flatten(collect(distinct r.{property}),true)) as val
            call {{with val match (c:CATEGORY) where c.CMID in val return collect(c.CMID) as val2}}
            with [i in val where not i in val2] as badlist
            unwind badlist as bad
            with bad
            match (c:CATEGORY)<-[r:USES]-(d:DATASET) where bad in r.{property}
            return c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset, '{property}' as propertyType, apoc.text.join(r.{property},"; ") as property, bad as badCMID
            """
            result = getQuery(query, driver, type="df")
            if len(result) > 0:
                results.append(result)

        if len(results) > 0:
            results = pd.concat(results)

            query2 = """
            MATCH (old:DELETED)-[:IS]->(c)
            where old.CMID in $cmids
            return old.CMID as badCMID, c.CMID as newCMID
            """

            deleted = getQuery(
                query2, driver, {"cmids": list(results["badCMID"].unique())}, type="df")
            
            fp1 = None
            if isinstance(results, pd.DataFrame) and not results.empty:
                if isinstance(deleted, pd.DataFrame) and not deleted.empty:
                    results = results.merge(
                        deleted, on="badCMID", how="left", suffixes=("", "_new"))
                with tempfile.NamedTemporaryFile(delete=False,                                             
                                             prefix=f"badCMIDs_{database}_",
                                             suffix=f".xlsx", dir="/tmp") as tmpfile:
                                        fp1 = tmpfile.name
                                        results.to_excel(fp1, index=False)
                if isinstance(mail, Mail):
                    sendEmail(mail, subject=f"Bad CMIDs for {database}", recipients=[
                              "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

            if return_type == "data":
                return results.to_dict(orient="records")
            elif return_type == "info":
                return {"info": str(len(results)), "filepath": fp1}
        else:
            if return_type == "data":
                return "No bad CMIDs found"
            elif return_type == "info":
                return {"info": "0", "filepath": None}

    except Exception as e:
        return "Error: " + str(e)


def getMultipleLabels(database, mail=None, return_type="data"):
    """
    Identify `USES` relationships in which multiple labels are assigned 
    to a dataset–category link in a Neo4j database, and optionally 
    export results to Excel or send via email.

    This function detects cases where the `r.label` property of a 
    `USES` relationship is a list of strings containing more than one 
    label. These indicate datasets assigned to multiple subdomains 
    simultaneously, which may represent data modeling errors.

    Results can be returned as structured data, summary information, 
    and/or saved to an Excel file. If a valid `Mail` object is provided, 
    the file is also sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, an email with the results file attached is sent 
        to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return results as a list of dictionaries (records).
        - "info" : return a dictionary with:
            * "info": number of relationships with multiple labels
            * "filepath": path to the generated Excel file 
              (or None if no file was created)

    Returns
    -------
    list of dict or dict or str
        If return_type == "data":
            A list of dictionaries, each containing:
                - CMID: category ID
                - CMName: category name
                - datasetID: dataset ID
                - Key: relationship key
                - label: semicolon-delimited string of labels assigned
        If return_type == "info":
            A dict with summary info and the file path to the exported 
            Excel file.
        If no relationships with multiple labels are found:
            "No multiple labels found" (if return_type == "data"),
            or {"info": 0, "filepath": None} (if return_type == "info").
        If an error occurs:
            A string containing the exception message.

    Side Effects
    ------------
    - Creates a temporary Excel file under `/tmp` if multiple-label 
      relationships are found.
    - Sends an email with the results file attached if a Mail object 
      is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as strings.
    """
    try:
        driver = getDriver(database)
        query = """
        match (d:DATASET)-[r:USES]->(c:CATEGORY) where apoc.meta.cypher.type(r.label) = "LIST OF STRING" and size(r.label) > 1 return c.CMID as CMID, c.CMName as CMName, d.CMID as datasetID, r.Key as Key, apoc.text.join(r.label, "; ") as label
        """
        results = getQuery(query, driver, type="df")

        fp1 = None
        if isinstance(results, pd.DataFrame) and not results.empty:
            with tempfile.NamedTemporaryFile(delete=False, 
                                             prefix=f"multipleLabels_{database}_",
                                             suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
            results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Multiple Labels for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

            if return_type == "data":        
                return results.to_dict(orient="records")
            elif return_type == "info":
                return {"info": str(len(results)), "filepath": fp1}

        else:
            if return_type == "data":   
                return "No multiple labels found"
            elif return_type == "info":
                return {"info":" 0", "filepath": None}

    except Exception as e:
        return str(e)


def getBadJSON(database, mail=None, return_type="data"):
    """
    Validate JSON properties stored in a Neo4j database and identify 
    invalid records for specific fields, optionally exporting results 
    to Excel or sending via email.

    This function checks two JSON-encoded properties on nodes:
      - `geoCoords`
      - `parentContext`

    It validates the JSON structure of each property and collects 
    any invalid entries. Results are written to separate Excel files 
    under `/tmp`. If a valid `Mail` object is provided, these files 
    are attached and sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, separate emails with attachments are sent for 
        invalid `geoCoords` and `parentContext` records.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return results as a dictionary containing full 
          validation output.
        - "info" : return a dictionary with a summary string and 
          file paths for the generated Excel files.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            A dict with keys:
                - "geoCoords": list of invalid `geoCoords` records
                - "parentContext": list of invalid `parentContext` records
                - "emailSent": string "True" if any emails were sent, 
                  "False" otherwise
        If return_type == "info":
            A dict with:
                - "info": summary string of counts for invalid properties
                - "filepath": list containing file paths for the Excel 
                  files (or None if no invalid entries were found)
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Creates up to two temporary Excel files under `/tmp` for invalid 
      JSON records, one for each property checked.
    - Sends one or two emails (depending on which properties fail) with 
      the results attached if a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """
    try:
        fd, fp1 = tempfile.mkstemp(prefix = f"geoCoords_{database}_", suffix=".xlsx", dir="/tmp")
        os.close(fd)
        fd, fp2 = tempfile.mkstemp(prefix = f"parentContext_{database}_", suffix=".xlsx", dir="/tmp")
        os.close(fd)
        results1 = validateJSON(
            database=database, property='geoCoords', path=fp1)
        results2 = validateJSON(
            database=database, property='parentContext', path=fp2)

        mailSent = "False"

        if isinstance(mail, Mail):
            if len(results1) > 1:
                sendEmail(mail, subject=f"Invalid geoCoords properties for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
                mailSent = "True"

            if len(results2) > 1:
                sendEmail(mail, subject=f"Invalid parentContext properties for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp2])
                mailSent = "True"
        if return_type == "data":
            return {"geoCoords": len(results1), "parentContext": len(results2), "geoCoords": results1, "parentContext": results2, "emailSent": mailSent}
        elif return_type == "info":
            if len(results1) == 0:
                fp1 = None
            if len(results2) == 0:
                fp2 = None
            return {"info": f"Invalid geoCoords: {len(results1)}; Invalid parentContext: {len(results2)}", "filepath": [fp1, fp2]}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadDomains(database, mail=None, return_type="data"):
    """
    Identify invalid or missing labels in a Neo4j database, and optionally 
    export results to Excel or send via email.

    This function performs three main checks:
      1. **Bad subdomain labels**: Nodes with labels belonging to a group 
         but not defined in that group's allowed label list.
      2. **Missing CATEGORY label**: Nodes referenced by `USES` relationships 
         that are missing the `CATEGORY` label.
      3. **Missing DATASET label**: Nodes linked to categories by `USES` 
         relationships that are missing the `DATASET` label.

    Results for each check are written to separate Excel files under `/tmp` 
    if issues are found. If a valid `Mail` object is provided, the files are 
    sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, separate emails with attachments are sent for each 
        type of issue found.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return results as a dictionary containing detailed 
          records for all checks.
        - "info" : return a dictionary with summary strings and file 
          paths for the generated Excel files.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            A dict with keys:
                - "bad_labels_count": number of nodes with invalid labels
                - "missing_category_count": number of nodes missing `CATEGORY`
                - "missing_dataset_count": number of nodes missing `DATASET`
                - "bad_labels": list of dicts with CMID, CMName, and invalid label(s)
                - "missing_category": list of dicts for nodes missing `CATEGORY`
                - "missing_dataset": list of dicts for nodes missing `DATASET`
        If return_type == "info":
            A dict with:
                - "info": summary string of counts for all three checks
                - "filepath": list of file paths for the Excel files 
                  ([bad_labels_path, missing_category_path, missing_dataset_path])
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Creates up to three temporary Excel files under `/tmp` if issues are found.
    - Sends one or more emails with attachments if a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """

    try:
        driver = getDriver(database)
        labels = getQuery(
            "match (l:LABEL) return l.CMName as label, l.groupLabel as groupLabel", driver, type="df")
        groups = list(labels['groupLabel'].unique())

        matches = getQuery(
            "match (c) return distinct [i in  labels(c) where not i = 'CATEGORY'] as label", driver, type="df")

        bad_labels = []

        for group in groups:
            sub_labels = labels[labels['groupLabel'] == group]
            sub_labels = list(sub_labels['label'].values)
            for row in matches.iterrows():
                label = row[1]['label']
                if group in label:
                    bad = [x for x in label if not x in sub_labels]
                    if len(bad) > 0:
                        print("bad label for " + group +
                              ": " + ", ".join(label))
                        fmt_label = ":".join(label)
                        query = f"""
                        match (c:{fmt_label})
                        return c.CMID as CMID, c.CMName as CMName, '{fmt_label}' as label
                        """
                        result = getQuery(query, driver)
                        bad_labels.append(result)

        if len(bad_labels) > 0:
            bad_labels = pd.concat([pd.DataFrame(item) for item in bad_labels])
        else:
            bad_labels = pd.DataFrame(columns=["CMID", "CMName", "label"])
            fp1 = None
        if isinstance(bad_labels, pd.DataFrame) and not bad_labels.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"bad_labels_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                fp1 = tmpfile.name
            bad_labels.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Bad Labels for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        missing_category = getQuery(
            "match (c)<-[:USES]-(d:DATASET) where not 'CATEGORY' in labels(c) return c.CMID as CMID, c.CMName as CMName", driver, type="df")

        fp2 = None
        if isinstance(missing_category, pd.DataFrame) and not missing_category.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"missing_category_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp2 = tmpfile.name
                    missing_category.to_excel(fp2, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Missing CATEGORY Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp2])

        missing_dataset = getQuery(
            "match (c:CATEGORY)<-[:USES]-(d) where not 'DATASET' in labels(d) return d.CMID as CMID, d.CMName as CMName", driver, type="df")

        fp3 = None
        if isinstance(missing_dataset, pd.DataFrame) and not missing_dataset.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"missing_dataset_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp3 = tmpfile.name
                    missing_dataset.to_excel(fp3, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Missing DATASET Label for {database}", recipients=[
                    "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp3])

        if return_type == "data":
            return {"bad_labels_count": len(bad_labels), "missing_category_count": len(missing_category), "missing_dataset_count": len(missing_dataset), "bad_labels": bad_labels.to_dict(orient="records"), "missing_category": missing_category.to_dict(orient="records"), "missing_dataset": missing_dataset.to_dict(orient="records")}
        elif return_type == "info":
            return {"info": f"Wrong Subdomain: {len(bad_labels)}; Missing CATEGORY domain: {len(missing_category)}; Missing DATASET domain: {len(missing_dataset)}", "filepath": [fp1, fp2, fp3]}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadRelations(database, mail=None, return_type="data"):
    """
    Detect invalid or inconsistent relationship structures between categories 
    and subdomains in a Neo4j database, and optionally export results to Excel 
    or send via email.

    This function checks relationship integrity for category hierarchies and 
    subdomain assignments. It verifies whether parent–child category relationships 
    (`CONTAINS` and other defined relationships) align correctly with expected 
    group labels. It also handles special cases for `ETHNICITY` in the SocioMap 
    database.

    The results include:
      - Categories assigned as parents to child groups where the parent is 
        missing the appropriate group label.
      - Mis-specified `CONTAINS` relationships between categories.
      - Invalid relationships involving `ETHNICITY` categories (SocioMap only).

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, an email with the results file attached is sent 
        to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return detailed results as a dictionary.
        - "info" : return a dictionary with summary information 
          and file paths for the exported Excel file.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            A dict with keys:
                - "bad_relationship_labels_count": number of invalid 
                  relationship entries found
                - "bad_relationship_labels": list of dicts with details, 
                  including parent/child CMIDs, names, domains, relationship 
                  type, dataset references, and property values
        If return_type == "info":
            A dict with:
                - "info": number of invalid relationships
                - "filepath": list with the path to the generated Excel file 
                  (or None if no issues were found)
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Creates a temporary Excel file under `/tmp` if invalid relationships 
      are found.
    - Sends an email with the results file attached if a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """
    try:
        driver = getDriver(database)
        labels = getQuery(
            "match (l:LABEL) where not l.relationship is null return distinct l.groupLabel as group, l.relationship as relationship", driver)

        if database.lower() == "sociomap":
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
                matches = getQuery(
                    f"match (p:CATEGORY)-[:{relationship}]->(c:  {group})<-[r:USES]-(d:DATASET) where not '{group}' in labels(p) unwind keys(r) as property with p.CMID as parentCMID, p.CMName as parentCMName, c.CMID as childCMID, c.CNName as childCMName, r.Key as Key, d.datasetID as datasetID, d.shortName as shortName, '{relationship}' as relationship, apoc.text.join([i in labels(p) where not i in ['CATEGORY']],'; ') as domains, property, r[property] as value where parentCMID = value or parentCMID in value return distinct parentCMID, parentCMName, childCMID, childCMName, Key, datasetID, shortName, relationship, domains, property, value", driver)
                results.append(matches)
                results.append(matches)

            matchContains = getQuery(
                f"MATCH (p:CATEGORY)-[:CONTAINS]->(c:CATEGORY) WHERE NOT 'GENERIC' IN labels(p) WITH p, c, [x IN labels(p) WHERE x IN {groups}] AS parentLabels, [y IN labels(c) WHERE y IN {groups}] AS childLabels UNWIND parentLabels AS parentLabel UNWIND childLabels AS childLabel WITH p, c, parentLabel, childLabel WHERE NOT parentLabel = $group AND childLabel = $group RETURN DISTINCT p.CMID AS parentCMID, p.CMName AS parentCMName, parentLabel + '->' + childLabel AS domains, c.CMID AS childCMID, c.CMName AS childCMName, 'CONTAINS' AS relationship", driver, params={'group': group})
            contains.append(matchContains)

        results = pd.concat([pd.DataFrame(item) for item in results])
        contains = pd.concat([pd.DataFrame(item) for item in contains])
        results = pd.concat([results, contains])
        results = results.drop_duplicates()

        fp1 = None
        if isinstance(results, pd.DataFrame) and not results.empty:
            with tempfile.NamedTemporaryFile(delete=False, 
                                             prefix=f"bad_relationship_labels_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
            results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Bad Relationship Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        if return_type == "data":
            return {"bad_relationship_labels_count": len(results), "bad_relationship_labels": results.to_dict(orient="records")}
        elif return_type == "info":
            return {"info": str(len(results)), "filepath": [fp1]}

    except Exception as e:
        result = str(e)
        return result, 500



def CMNameNotInName(database, mail=None, return_type="data"):
    """
    Identify categories where the primary `CMName` is missing from 
    the list of alternate names, and optionally correct, export, 
    or email the results.

    This function queries all `CATEGORY` nodes to find cases where 
    the `CMName` field is not included in the `names` property. 
    For each invalid category:
      - A relationship update is triggered (`addCMNameRel`).
      - Alternate names are updated in the database (`updateAltNames`).
    The affected CMIDs are written to an Excel file if any are found.
    If a valid `Mail` object is provided, the file is sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, the Excel file is sent to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return results as a dictionary with details.
        - "info" : return a dictionary with summary information 
          and the file path.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            A dict with keys:
                - "Total": number of categories with mismatched names
                - "Name not in CMName": DataFrame of affected CMIDs
        If return_type == "info":
            A dict with:
                - "info": number of mismatched categories
                - "filepath": path to the generated Excel file 
                  (or None if no mismatches were found)
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Updates relationships and alternate names in the database 
      for categories with invalid naming.
    - Creates a temporary Excel file under `/tmp` if mismatches are found.
    - Sends an email with the file attached if a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """
    try:
        driver = getDriver(database)

        query = """
        MATCH (n:CATEGORY)
        WHERE NOT n.CMName in n.names
        RETURN n.CMID as CMID
        """

        dataset_query = """MATCH (n:DATASET)
                        WHERE any(v IN [n.datasetCitation, n.shortName, n.CMName] 
                                WHERE v IS NOT NULL AND (n.names IS NULL OR NOT v IN n.names))
                        SET n.names = coalesce(n.names, []) +
                                    [v IN [n.datasetCitation, n.shortName, n.CMName]
                                    WHERE v IS NOT NULL AND NOT v IN coalesce(n.names, [])]
                        RETURN n.CMID as CMID
                        """
        cmids = getQuery(query, driver, type="list")

        dataset_cmids = getQuery(dataset_query,driver, type="list")

        fp1 = None
        if len(cmids) > 0:
            addCMNameRel(database, CMID=cmids)
            updateAltNames(driver, CMID=cmids)
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"BadCMNames_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                fp1 = tmpfile.name
                cmids = pd.DataFrame(cmids)
                cmids.columns = ["CMID"]
                cmids.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Bad Relationship Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
        if return_type == "data":
            return {"Total": len(cmids), "Name not in CMName": cmids}
        elif return_type == "info":
            return {"info": str(len(cmids)), "filepath": fp1}

    except Exception as e:
        result = str(e)
        return result, 500


def fixMetaTypes(database, return_type="data"):
    """
    Validate and correct property types for nodes and relationships 
    in a Neo4j database based on metadata definitions.

    This function retrieves property metadata (from `getPropertiesMetadata`) 
    and ensures that stored values match their expected `metaType`. If a 
    property’s type does not match, it is reformatted using 
    `custom.formatProperties` to conform to the correct type.

    Specifically:
      - Node properties and relationship properties are validated separately.
      - Each property is checked against its expected metaType:
          * "STRING" → Neo4j type "STRING"
          * other types → Neo4j type "LIST OF STRING"
      - Properties with mismatched types are reformatted and updated in place.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        Currently unused in this function (included for consistency with 
        other validation functions).
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return a status dictionary confirming the updates.
        - "info" : return a simplified dictionary with a completion message.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            {"status": "success", "message": "Meta types updated successfully"}
        If return_type == "info":
            {"info": "Completed updating metatypes"}
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Executes Cypher queries to reformat and update properties 
      on nodes and relationships whose stored types do not match 
      their expected metaTypes.
    - Logs the number of updated values for each property via `print`.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """
    try:
        driver = getDriver(database)
        properties = getPropertiesMetadata(driver)
        properties = pd.DataFrame(properties)
        properties = properties[properties['metaType'].notna()]
        node_properties = properties[properties['type'] == 'node']
        relationship_properties = properties[properties['type']
                                             == 'relationship']

        for property, metaType in zip(node_properties['property'], node_properties['metaType']):
            metaType = metaType.upper()
            metaType_neo4j = "STRING" if metaType == "STRING" else "LIST OF STRING"
            query = f"""
            MATCH (n)
            WHERE n.{property} IS NOT NULL AND apoc.meta.cypher.type(n.{property}) <> "{metaType_neo4j}"
            SET n.{property} = custom.formatProperties([n.{property}],'{metaType}',';')[0].prop
            return count(*) as count
            """
            result = getQuery(query, driver)
            print(f"Updated {property} to {metaType}: {result}")

        for property, metaType in zip(relationship_properties['property'], relationship_properties['metaType']):
            metaType = metaType.upper()
            metaType_neo4j = "STRING" if metaType == "STRING" else "LIST OF STRING"
            query = f"""
            MATCH (n)-[rel]->(m)
            WHERE rel.{property} IS NOT NULL AND apoc.meta.cypher.type(rel.{property}) <> "{metaType_neo4j}"
            SET rel.{property} = custom.formatProperties([rel.{property}],'{metaType}',';')[0].prop
            return count(*) as count
            """
            result = getQuery(query, driver)
            print(f"Updated {property} to {metaType}: {result}")
        if return_type == "data":
            return {"status": "success", "message": "Meta types updated successfully"}
        elif return_type == "info":
            return {"info": "Completed updating metatypes"}
    except Exception as e:
        result = str(e)
        return result, 500

def noUSES(database, save=True, mail=None, return_type="data"):
    """
    Identify categories in a Neo4j database that are not connected 
    to any datasets via `USES` relationships, and optionally 
    export results to Excel or send via email.

    This function searches for `CATEGORY` nodes that have no incoming 
    `USES` edges from any `DATASET` nodes. These represent categories 
    that are defined in the graph but not actually used.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    save : bool, default=True
        If True, save the results to an Excel file in `/tmp`.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided and results exist, the Excel file is attached 
        and sent via email to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return detailed results as a dictionary.
        - "info" : return a dictionary with summary information 
          and the file path.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            {"Total": number of unused categories,
             "No USES": list of dicts with CMID and CMName}
        If return_type == "info":
            {"info": number of unused categories,
             "filepath": path to the Excel file or None}
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Optionally creates a temporary Excel file under `/tmp` with 
      the unused categories.
    - Optionally sends an email with the results file attached if 
      a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """

    try:
        driver = getDriver(database)
        query = """
        MATCH (c:CATEGORY)
        WHERE NOT (c)<-[:USES]-(:DATASET)
        RETURN c.CMID as CMID, c.CMName as CMName
        """
        results = getQuery(query, driver, type="df")

        fp1 = None
        if isinstance(results, pd.DataFrame) and not results.empty:
            if save:
                with tempfile.NamedTemporaryFile(delete=False, prefix=f"no_uses_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    results.to_excel(fp1, index=False)
                if isinstance(mail, Mail):
                    sendEmail(mail, subject=f"No USES for {database}", recipients=[
                            "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
        if return_type == "data":
            return {"Total": len(results), "No USES": results.to_dict(orient="records")}
        elif return_type == "info":
            return {"info": str(len(results)), "filepath": fp1}
    except Exception as e:
        result = str(e)
        return result, 500
    
def checkUSES(database, save=True, mail=None, return_type="data"):
    """
    Validate `USES` relationships in a Neo4j database by checking for 
    missing or malformed properties, and optionally export results to 
    Excel or send via email.

    This function inspects all `USES` relationships between `CATEGORY` 
    and `DATASET` nodes to detect the following issues:
      - Missing or empty `label`
      - Missing or empty `Key`
      - `Key` values not containing the required ": " delimiter
      - Missing or empty `Name`

    Detected issues are aggregated into a single result set. Results are 
    written to an Excel file under `/tmp` if issues exist. If a valid 
    `Mail` object is provided, the file is sent via email.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    save : bool, default=True
        If True, save the results to an Excel file in `/tmp`.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided and results exist, the Excel file is attached 
        and sent via email to `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return detailed results as a dictionary.
        - "info" : return a dictionary with summary information 
          and the file path.

    Returns
    -------
    dict or tuple
        If return_type == "data":
            {"Total": number of invalid relationships,
             "Check USES": list of dicts with details of each issue 
                            (error type, CMID, CMName, Key, datasetID, dataset)}
        If return_type == "info":
            {"info": number of invalid relationships,
             "filepath": path to the Excel file or None}
        If an error occurs:
            A tuple of (error_message, 500).

    Side Effects
    ------------
    - Optionally creates a temporary Excel file under `/tmp` with 
      invalid relationship records.
    - Optionally sends an email with the results file attached if 
      a Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages 
    with HTTP status 500.
    """

    try:
        driver = getDriver(database)
        
        # Check for missing label, Key, and Name in USES relationships
        
        # query = """
        # MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        # where r.label is null or r.label = ''
        # RETURN "No label" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        # UNION ALL
        # MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        # where r.Key is null or r.Key = ''
        # RETURN "No Key" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        # UNION ALL
        # MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        # where not r.Key contains ": "
        # RETURN "Malformed Key" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        # UNION ALL
        # MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        # where r.Name is null or r.Name = ''
        # RETURN "No Name" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        # """

        query = """
        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        where r.label is null or r.label = ''
        RETURN "No label" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        UNION ALL
        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        where r.Key is null or r.Key = ''
        RETURN "No Key" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        UNION ALL
        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        WITH c, d, r, [segment IN split(r.Key, ";") | trim(segment)] AS segments
        WHERE any(seg IN segments WHERE NOT seg CONTAINS ": ")
        RETURN "Malformed Key" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        UNION ALL
        MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
        where r.Name is null or r.Name = ''
        RETURN "No Name" as error, c.CMID as CMID, c.CMName as CMName, r.Key as Key, d.CMID as datasetID, d.CMName as dataset
        """

        result = getQuery(query, driver, type="df")
        fp1 = None
        if isinstance(result, pd.DataFrame) and not result.empty:
            if save:
                with tempfile.NamedTemporaryFile(delete=False,
                                                 prefix=f"check_uses_{database}_",
                                                 suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    result.to_excel(fp1, index=False)
                if isinstance(mail, Mail):
                    sendEmail(mail, subject=f"Check USES for {database}", recipients=[
                            "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        if return_type == "data":
            return {"Total": len(result), "Check USES": result.to_dict(orient="records")}
        elif return_type == "info":
            return {"info": str(len(result)), "filepath": fp1}

    except Exception as e:
        result = str(e)
        return result, 500


def reportChanges(database, dateStart=None, dateEnd=None, action="default", user=None, mail=None, return_type="data"):
    """
    Generate a report of logged changes in a Neo4j database, filtered 
    by date range, action type, and user, and optionally export results 
    to Excel or send via email.

    This function queries the `LOG` nodes to summarize database changes. 
    Supported filters include:
      - **Date range**: defaults to yesterday through today if not provided.
      - **Action type**: defaults to all major actions 
        ("created node", "created relationship", "deleted", "merged", "changed").
      - **User**: restricts results to changes by a given user.

    Results include counts of actions grouped by date and user. If valid 
    user information exists in a separate `userdb`, usernames and full 
    names are joined into the results.

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    dateStart : str or datetime-like, optional
        Start date for filtering logs (default: yesterday).
    dateEnd : str or datetime-like, optional
        End date for filtering logs (default: today).
    action : str or list of str, default="default"
        Action(s) to filter on. 
        - "default": include ["created node", "created relationship", 
          "deleted", "merged", "changed"].
        - str: a single action string.
        - list: list of action strings.
    user : str, optional
        User ID to filter on. Defaults to None (all users).
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, the Excel file with results is emailed to 
        `admin@catmapper.org`.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return full results as a list of dictionaries.
        - "info" : return an HTML summary of counts per action and 
          the path to the Excel file.

    Returns
    -------
    list of dict or dict or str
        If return_type == "data":
            List of dictionaries, one per log entry, including:
                - action
                - date
                - user
                - count
                - username
                - fullname
        If return_type == "info":
            {"info": HTML summary table of action counts,
             "filepath": path to the Excel file}
        If no changes are found:
            [] (if return_type == "data") or
            {"info": "No changes...", "filepath": None}
        If an error occurs:
            A string beginning with "Error in reportChanges: ...".

    Side Effects
    ------------
    - Creates a temporary Excel file under `/tmp` containing the results.
    - Optionally sends an email with the results file attached if a 
      Mail object is provided.

    Raises
    ------
    None directly. Exceptions are caught and returned as error messages.
    """

    try:
        driver = getDriver(database)
        if action == "default":
            action = ["created node","created relationship", "deleted", "merged", "changed"]
        elif isinstance(action, str):
            action = [action]
        elif not isinstance(action, list):
            return "Invalid action parameter. Must be a string or a list of strings."

        if user is None:
            user = ""
        
        params = {
            'user': user,
            'dateStart': dateStart if dateStart else (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
            'dateEnd': dateEnd if dateEnd else pd.Timestamp.now().strftime('%Y-%m-%d')
        }
        
        if isinstance(params['dateStart'], str):
            params['dateStart'] = pd.to_datetime(params['dateStart']).strftime('%Y-%m-%d')
        if isinstance(params['dateEnd'], str):
            params['dateEnd'] = pd.to_datetime(params['dateEnd']).strftime('%Y-%m-%d')
        if not params['user'] == "":
            params['user'] = params['user'].strip()
            user_query = "AND l.user = toString(row.user)"
        else: 
            user_query = ""
            
        queries = [] 
        for act in action:
             queries.append(f"""
        unwind $rows as row
        MATCH (l:LOG)
        WHERE l.action starts with "{act}" AND date(datetime(l.timestamp)) >= date(row.dateStart) AND date(datetime(l.timestamp)) <= date(row.dateEnd) {user_query}
        RETURN "{act}" as action, toString(date(datetime(l.timestamp))) AS date, l.user AS user, count(*) AS count order by date DESC, user
        """)

        query = " UNION ALL ".join(queries)

        # return {"query": query, "params": {"rows": [data]}}
        results = getQuery(query, driver, params={"rows": params},type="df")
        if isinstance(results, str) or len(results) == 0:
            if return_type == "data":
                return []
            else:
                return {"info": f"No changes to the database were found for the dates specified: {params['dateStart']} to {params['dateEnd']}", "filepath": None }

        driver_uses = getDriver("userdb")
        query_uses = """
        MATCH (u:USER) return u.userid as user, u.username as username, u.first + " " + u.last as fullname
        """
        users_df = getQuery(query_uses, driver=driver_uses, type="df")
        
        if len(users_df) == 0 or isinstance(users_df, str):
            return "Error retrieving users from userdb"

        results = results.merge(users_df, on='user', how='left')

        fp1 = None
        if isinstance(results,pd.DataFrame):
            with tempfile.NamedTemporaryFile(delete=False,
                                             prefix=f"DBchanges_{database}_{params['dateStart']}_to_{params['dateEnd']}_",
                                             suffix=".xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
            results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Database Changes for {database} and {action} from {params['dateStart']} to {params['dateEnd']}", recipients=[
                        "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        if return_type == "data":
            return results.to_dict(orient="records")
        else:
            agg = results.groupby("action", as_index=False)["count"].sum()
            agg_html = agg.to_html(index=False,border=0, classes="dataframe", justify="left").replace("\n", "")
            return {"info": agg_html,"filepath": fp1}

    except Exception as e:
        return "Error in reportChanges: " + str(e)
    
def missingCMName(database, mail=None, return_type="data"):
    """
    Identify categories in a Neo4j database that are missing 
    a `CMName` property, and optionally export results to Excel 
    or send via email.

    This function searches for `CATEGORY`, `DATASET`, and `METADATA` nodes that lack a defined `CMName`. 

    Parameters
    ----------
    database : str
        The database name used to obtain a Neo4j driver instance.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided and results exist, the Excel file is attached 
        and sent via email.
    return_type : {"data", "info"}, default="data"
        Determines the format of the return value:
        - "data" : return detailed results as a dictionary.
        - "info" : return a dictionary with summary information 
          and the file path.
    Returns
    -------
    dict or tuple
        If return_type == "data":
            {"Total": number of nodes missing CMName,
             "Missing CMName": list of dicts with CMID and labels}
        If return_type == "info":
            {"info": number of nodes missing CMName,
             "filepath": path to the Excel file or None}
        If an error occurs:
            A tuple of (error_message, 500).
    """
    
    try:
        driver = getDriver(database)
        query = """
        MATCH (n)
        WHERE (n:CATEGORY OR n:DATASET OR n:METADATA) AND (n.CMName IS NULL OR n.CMName = '')
        RETURN n.CMID as CMID, labels(n) as labels
        """
        results = getQuery(query, driver, type="df")

        fp1 = None
        if isinstance(results, pd.DataFrame) and not results.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"missing_cmname_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                fp1 = tmpfile.name
                results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Missing CMName for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
        if return_type == "data":
            return {"Total": len(results), "Missing CMName": results.to_dict(orient="records")}
        elif return_type == "info":
            return {"info": str(len(results)), "filepath": fp1}
    except Exception as e:
        result = str(e)
        return result, 500

def getBadContextual(database, mail=None, return_type="data"):
    try:
        driver = getDriver(database)
        query = """
        match (d:DATASET) with collect(d.shortName) as sn
        match (c:CATEGORY)<-[r:CONTAINS]-(p:CATEGORY) where not r.referenceKey is null 
        with sn, r  unwind r.referenceKey as rf with r, sn, split(rf," Key: ")[0] as rf 
        where not rf in sn return distinct rf
        """
        results = getQuery(query, driver, type="df")

        fp1 = None
        if isinstance(results, pd.DataFrame) and not results.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"invalidShortName_refKeys_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                fp1 = tmpfile.name
                results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Invalid short names for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
        query = """
        MATCH (n)-[r]->(m)
        WHERE type(r) IN ["CONTAINS", "LANGUOID_OF", "RELIGION_OF", "DISTRICT_OF"]
        WITH n, m, type(r) AS relType, COUNT(r) AS relCount
        WHERE relCount > 1
        RETURN n.CMID AS CMID,
            m.CMID AS targetCMID,
            relType,
            relCount
        """
        results2 = getQuery(query, driver, type="df")

        fp2 = None
        if isinstance(results2, pd.DataFrame) and not results2.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"duplicate_contextual_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                fp2 = tmpfile.name
                results2.to_excel(fp2, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Duplicate contextual ties for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp2])
        
        query = """
                MATCH (n)-[r:CONTAINS]->(n)
                RETURN n.CMID AS startCMID, [n.CMID] AS relatedNodes, 'Self-loop' AS issueType, type(r) AS relType

                UNION ALL

                MATCH (a)-[r1:CONTAINS]->(b)
                WHERE (b)-[:CONTAINS]->(a) AND id(a) < id(b)  // avoids double reporting
                RETURN a.CMID AS startCMID, [b.CMID] AS relatedNodes, 'Reciprocal' AS issueType, type(r1) AS relType

                UNION ALL

                MATCH p = (n)-[:CONTAINS*3..]->(n)
                RETURN n.CMID AS startCMID,
                    [x IN nodes(p) | x.CMID] AS relatedNodes,
                    'Cycle' AS issueType,
                    'CONTAINS' AS relType
                """
        results3 = getQuery(query, driver, type="df")

        fp3 = None
        if isinstance(results3, pd.DataFrame) and not results3.empty:
            with tempfile.NamedTemporaryFile(delete=False, prefix=f"cyclic_contextual_{database}_", suffix=".xlsx", dir="/tmp") as tmpfile:
                fp3 = tmpfile.name
                results3.to_excel(fp3, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Cyclic contextual ties for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp3])

        if return_type == "data":
            return {"Invalid short names": len(results), "Invalid short names": results.to_dict(orient="records"),"Duplicate contextual ties": len(results2), "Duplicate contextual ties": results2.to_dict(orient="records"),"Cyclical contextual ties": len(results3), "Cyclical contextual ties": results3.to_dict(orient="records")}
        elif return_type == "info":
            return {"Invalid short names": str(len(results)), "Invalid short names filepath": fp1,"Duplicate contextual ties": str(len(results2)), "Duplicate contextual ties filepath": fp2,"Cyclical contextual ties": str(len(results3)), "Cyclical contextual ties filepath": fp3}
    except Exception as e:
        result = str(e)
        return result, 500

    
def runRoutinesStream(databases="all", mail=None):
    """
    Run a sequence of validation and processing routines for one or more 
    Neo4j databases, stream progress logs to the client, and optionally 
    send an email summary table with attachments.

    This function executes multiple predefined routines (e.g., `reportChanges`, 
    `checkDomains`, `getBadCMID`, `noUSES`, `checkUSES`, `processUSES`) 
    against the selected databases. While running, results are streamed 
    incrementally to the client as HTML. At the end of execution, results 
    are compiled into a summary HTML table. If a valid `Mail` object is 
    provided, this table is sent via email along with any generated files.

    Parameters
    ----------
    databases : {"all", str, list}, default="all"
        Specifies which databases to run routines against.
        - "all" : run against both "ArchaMap" and "SocioMap".
        - str   : run against a single named database.
        - list  : run against a list of database names.
    mail : Mail, optional
        A Mail object for sending notifications (default: None).
        If provided, the final HTML table and attachments are emailed 
        to `rjbischo@asu.edu`.

    Returns
    -------
    flask.Response
        A streaming HTTP response (`text/html` MIME type) that:
        - Streams incremental routine progress as HTML output.
        - At the end, yields a message about email status if mail was sent.

    Side Effects
    ------------
    - Executes multiple validation and processing routines against 
      the given databases.
    - Creates one or more temporary Excel files in `/tmp` for routines 
      that generate outputs.
    - Sends an email with the summary table and attachments if a 
      Mail object is provided.
    - Produces streaming output viewable in a web browser.

    Raises
    ------
    None directly. Exceptions raised by individual routines are caught 
    and streamed back as error messages in the output. The summary table 
    records exceptions in place of results.
    """
    
    files = []
    info = []  # keeps full streaming log
    results = {}  # keeps table values

    def emit(msg):
        info.append(msg)
        return msg + "\n\n"

    def generate():
        yield emit("Routines started at " + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + "<br>")

        if databases == "all":
            dbs = ["ArchaMap", "SocioMap"]
        elif isinstance(databases, str):
            dbs = [databases]
        else:
            dbs = databases

        print("in")

        routines = [
            ("Modifications", lambda db: reportChanges(db, return_type="info")),
            ("Check Domains", lambda db: checkDomains(db, mail=None, return_type="info")),
            ("Bad Domains", lambda db: getBadDomains(db, mail=None, return_type="info")),
            ("Bad CMID", lambda db: getBadCMID(db, mail=None, return_type="info")),
            ("Multiple Labels", lambda db: getMultipleLabels(db, mail=None, return_type="info")),
            ("Bad JSON", lambda db: getBadJSON(db, mail=None, return_type="info")),
            ("Bad Relations", lambda db: getBadRelations(db, mail=None, return_type="info")),
            ("CMName Not In Name", lambda db: CMNameNotInName(db, mail=None, return_type="info")),
            ("Missing CMName", lambda db: missingCMName(db, mail=None, return_type="info")),
            ("Invalid shortname", lambda db: getBadContextual(db, mail=None, return_type="info")),
            ("No USES", lambda db: noUSES(db, save=True, mail=None, return_type="info")),
            ("Check USES", lambda db: checkUSES(db, save=True, mail=None, return_type="info")),
            ("Process USES", lambda db: processUSES(db, detailed=False)),
            ("Process DATASETs", lambda db: processDATASETs(db)),
            ("Fix MetaTypes", lambda db: fixMetaTypes(db, return_type="info")),
        ]

        # initialize results dict
        results.update({name: {db: "" for db in dbs} for name, _ in routines})

        for db in dbs:
            yield emit(f"<h1>Running routines for {db}</h1>")

            for name, func in routines:
                yield emit(f"<h2>{name} for {db}:</h2>")
                try:
                    res = func(db)
                    if isinstance(res, str):
                        results[name][db] = res
                        yield emit(results[name][db])
                    elif isinstance(res, dict) and "info" in res:
                        results[name][db] = res["info"]
                        yield emit(results[name][db])
                        if res.get("filepath"):
                            files.append(res["filepath"])
                    else:
                        results[name][db] = str(res)
                        yield emit(results[name][db])
                except Exception as e:
                    results[name][db] = f"Exception: {e}"
                    yield emit(results[name][db])

        # flatten file list
        flattened = []
        for x in files:
            if isinstance(x, list):
                flattened.extend(x)
            else:
                flattened.append(x)
        files_out = [f for f in flattened if f is not None]

        # build HTML table for email
        header = "<tr><th>Routine</th>" + "".join([f"<th>{db}</th>" for db in dbs]) + "</tr>"
        rows = []
        for name in results:
            row = f"<tr><td>{name}</td>" + "".join(
                [f"<td>{results[name][db]}</td>" for db in dbs]
            ) + "</tr>"
            rows.append(row)
        table_html = "<table border='1'>" + header + "".join(rows) + "</table>"
        
        # static routine descriptions table
        routine_info_table = """
        <br><h2>Routine Descriptions</h2>
        <table border="1">
          <tr><th>Label</th><th>Function Name</th><th>Description</th></tr>
          <tr><td>Modifications</td><td>reportChanges</td><td>Generates a report of logged changes (nodes, relationships, merges, deletions, edits) within a date range, optionally grouped by user.</td></tr>
          <tr><td>Check Domains</td><td>checkDomains</td><td>Detects missing or inconsistent domain/subdomain assignments in USES relationships.</td></tr>
          <tr><td>Bad Domains</td><td>getBadDomains</td><td>Identifies invalid or missing labels: bad subdomain labels, nodes missing CATEGORY, or nodes missing DATASET.</td></tr>
          <tr><td>Bad CMID</td><td>getBadCMID</td><td>Finds invalid or outdated CMIDs used in USES relationships, including replacements from deleted nodes.</td></tr>
          <tr><td>Multiple Labels</td><td>getMultipleLabels</td><td>Flags USES relationships that have multiple subdomain labels assigned.</td></tr>
          <tr><td>Bad JSON</td><td>getBadJSON</td><td>Validates JSON properties (geoCoords, parentContext) and reports invalid entries.</td></tr>
          <tr><td>Bad Relations</td><td>getBadRelations</td><td>Checks for invalid or inconsistent parent–child category relationships and mis-specified CONTAINS links.</td></tr>
          <tr><td>CMName Not In Name</td><td>CMNameNotInName</td><td>Finds categories where the primary CMName is missing from the alternate names list and updates them.</td></tr>
          <tr><td>Missing CMName</td><td>missingCMName</td><td>Identifies CATEGORY, DATASET, and METADATA nodes that lack a defined CMName property.</td></tr>
          <tr><td>Invalid shortName and bad contextual ties</td><td>getbadContextual</td><td>Identifies bad shortnames and also duplicate and cylical contextual ties.</td></tr>
          <tr><td>No USES</td><td>noUSES</td><td>Lists categories that are not connected to any datasets through USES relationships.</td></tr>
          <tr><td>Check USES</td><td>checkUSES</td><td>Validates USES relationships, checking for missing or malformed label, Key, or Name fields.</td></tr>
          <tr><td>Process USES</td><td>processUSES</td><td>Processes and reconciles USES relationships for consistency and downstream use.</td></tr>
          <tr><td>Process DATASETs</td><td>processDATASETs</td><td>Processes dataset nodes to ensure correct structure and metadata integration.</td></tr>
          <tr><td>Fix MetaTypes</td><td>fixMetaTypes</td><td>Validates and corrects property data types on nodes and relationships based on metadata definitions.</td></tr>
        </table>
        """

        email_body = table_html + routine_info_table

        # send mail only after everything is done
        if isinstance(mail, Mail):
            status = sendEmail(
                mail,
                subject=f"Routines for {' and '.join(dbs)} - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
                recipients=["admin@catmapper.org"],
                body=email_body,  # IMPORTANT: table, not full log
                sender=config['MAIL']['mail_default'],
                attachments=files_out or []
            )
            yield emit(f'<br><h2>Mail sent with status: {status or "no status returned"}</h2>')

    return Response(stream_with_context(generate()), mimetype="text/html")