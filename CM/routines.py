''' routines.py '''

# This is a module for automatic routines in CatMapper

from neo4j import GraphDatabase
from CM.utils import *
from CM.email import *
from CM.USES import *
import pandas as pd
import json
import tempfile
from flask_mail import Mail
from configparser import ConfigParser
config = ConfigParser()
config.read('config.ini')

def is_valid_json(json_string):
    try:
        json.loads(json_string)
        return True
    except json.JSONDecodeError:
        return False


def validateJSON(database, property='parentContext', path="/mnt/storage/app/tmp/invalid_json.xlsx"):
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
            with tempfile.NamedTemporaryFile(delete=False, suffix="_missingDomains.xlsx", dir="/tmp") as tmpfile:
                fp1 = tmpfile.name
                results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Missing Domains for {database}", recipients=[
                            "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])
        if return_type == "data":        
            return results.to_dict(orient="records")
        else:
            return {"info": f"Completed and returned {len(results)} rows","filepath": fp1}

    except Exception as e:
        return str(e)


# need to create function based off this
# with "eventType" as prop match (a)-[r]->(b) where not r[prop] is null and r[prop] = [] call apoc.cypher.doIt("with r set r." + prop + " = NULL",{r:r}) yield value return count(*)

def backup2CSV(database, mail=None):
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

        # query_LOGS = """
        #     with 'match (l:LOG) unwind keys(l) as property return distinct elementId(l) as nodeID, property, l[property] as value' as query CALL apoc.export.csv.query(query, '/backups/download/logs_' + toString(date()) + '.csv', {})
        #     YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
        #     RETURN count(*);
        # """
        # LOGS = getQuery(query_LOGS, driver)
        # print(LOGS)

        if isinstance(mail, Mail):
            sendEmail(mail, subject="Weekly CSV Backup", recipients=[
                      "rjbischo@catmapper.org"], body="Weekly CSV backup completed.", sender=config['MAIL']['mail_default'])

        return f"backup2CSV completed for {database}"

    except Exception as e:
        return str(e)


def getBadCMID(database, mail=None):
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

            if isinstance(results, pd.DataFrame) and not results.empty:
                if isinstance(deleted, pd.DataFrame) and not deleted.empty:
                    results = results.merge(
                        deleted, on="badCMID", how="left", suffixes=("", "_new"))

                if isinstance(mail, Mail):
                    with tempfile.NamedTemporaryFile(delete=False, suffix="_badCMID.xlsx", dir="/tmp") as tmpfile:
                        fp1 = tmpfile.name
                        results.to_excel(fp1, index=False)
                    sendEmail(mail, subject=f"Bad CMIDs for {database}", recipients=[
                              "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

            return results.to_dict(orient="records")
        else:
            return "No bad CMIDs found"

    except Exception as e:
        return "Error: " + str(e)


def getMultipleLabels(database, mail=None):
    try:
        driver = getDriver(database)
        query = """
        match (d:DATASET)-[r:USES]->(c:CATEGORY) where apoc.meta.cypher.type(r.label) = "LIST OF STRING" and size(r.label) > 1 return c.CMID as CMID, c.CMName as CMName, d.CMID as datasetID, r.Key as Key, apoc.text.join(r.label, "; ") as label
        """
        results = getQuery(query, driver, type="df")

        if isinstance(results, pd.DataFrame) and not results.empty:

            if isinstance(mail, Mail):
                with tempfile.NamedTemporaryFile(delete=False, suffix="_multipleLabels.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    results.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Multiple Labels for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

            return results

        else:
            return "No multiple labels found"

    except Exception as e:
        return str(e)


def getBadJSON(database, mail=None):
    try:
        fd, fp1 = tempfile.mkstemp(suffix="_geoCoords.xlsx", dir="/tmp")
        os.close(fd)
        fd, fp2 = tempfile.mkstemp(suffix="_parentContext.xlsx", dir="/tmp")
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

        return {"geoCoords": len(results1), "parentContext": len(results2), "geoCoords": results1, "parentContext": results2, "emailSent": mailSent}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadDomains(database, mail=None):
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
        if isinstance(bad_labels, pd.DataFrame) and not bad_labels.empty:
            if isinstance(mail, Mail):
                with tempfile.NamedTemporaryFile(delete=False, suffix="_bad_labels.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    bad_labels.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Bad Labels for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        missing_category = getQuery(
            "match (c)<-[:USES]-(d:DATASET) where not 'CATEGORY' in labels(c) return c.CMID as CMID, c.CMName as CMName", driver, type="df")

        if isinstance(missing_category, pd.DataFrame) and not missing_category.empty:
            if isinstance(mail, Mail):
                with tempfile.NamedTemporaryFile(delete=False, suffix="_missing_category.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    missing_category.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Missing CATEGORY Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        missing_dataset = getQuery(
            "match (c)<-[:USES]-(d:DATASET) where not 'DATASET' in labels(d) return d.CMID as CMID, d.CMName as CMName", driver, type="df")

        if isinstance(missing_dataset, pd.DataFrame) and not missing_dataset.empty:
            if isinstance(mail, Mail):
                with tempfile.NamedTemporaryFile(delete=False, suffix="_missing_dataset.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    missing_dataset.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Missing DATASET Label for {database}", recipients=[
                    "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        return {"bad_labels_count": len(bad_labels), "missing_category_count": len(missing_category), "missing_dataset_count": len(missing_dataset), "bad_labels": bad_labels.to_dict(orient="records"), "missing_category": missing_category.to_dict(orient="records"), "missing_dataset": missing_dataset.to_dict(orient="records")}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadRelations(database, mail=None):
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

        if isinstance(results, pd.DataFrame) and not results.empty:
            if isinstance(mail, Mail):
                with tempfile.NamedTemporaryFile(delete=False, suffix="_bad_relationship_labels.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    results.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Bad Relationship Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        return {"bad_relationship_labels_count": len(results), "bad_relationship_labels": results.to_dict(orient="records")}

    except Exception as e:
        result = str(e)
        return result, 500


def CMNameNotInName(database, mail=None):
    try:
        driver = getDriver(database)

        query = """
        MATCH (n:CATEGORY)
        WHERE NOT n.CMName in n.names
        RETURN n.CMID as CMID
        """

        cmids = getQuery(query, driver, type="list")

        if len(cmids) > 0:
            addCMNameRel(database, CMID=cmids)
            updateAltNames(driver, CMID=cmids)

            if isinstance(mail, Mail):
                with tempfile.NamedTemporaryFile(delete=False, suffix="_BadCMNames.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    cmids = pd.DataFrame(cmids)
                    cmids.columns = ["CMID"]
                    cmids.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Bad Relationship Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        return {"Total": len(cmids), "Name not in CMName": cmids}

    except Exception as e:
        result = str(e)
        return result, 500


def fixMetaTypes(database):
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
        return "completed"
    except Exception as e:
        result = str(e)
        return result, 500

def noUSES(database, save=True, mail=None):
    """
    This function checks for categories that do not have any USES relationships with datasets.
    Parameters:
    - database: The name of the database to check.
    - save: If True, saves the results to an Excel file.
    Returns:
    - A dictionary with the total count and a list of categories without USES relationships.
    """
    try:
        driver = getDriver(database)
        query = """
        MATCH (c:CATEGORY)
        WHERE NOT (c)<-[:USES]-(:DATASET)
        RETURN c.CMID as CMID, c.CMName as CMName
        """
        results = getQuery(query, driver, type="df")

        if isinstance(results, pd.DataFrame) and not results.empty:
            if save:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_no_uses.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    results.to_excel(fp1, index=False)
                if isinstance(mail, Mail):
                    sendEmail(mail, subject=f"No USES for {database}", recipients=[
                            "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        return {"Total": len(results), "No USES": results.to_dict(orient="records")}

    except Exception as e:
        result = str(e)
        return result, 500
    
def checkUSES(database, save = True, mail=None):
    """
    This function checks for categories that have USES relationships with datasets but do not have a 'USES' relationship.
    Parameters:
    - database: The name of the database to check.
    - save: If True, saves the results to an Excel file.
    - mail: mail object used to send an email with the results.
    Returns:
    - A dictionary with the total count and a list of categories with USES relationships.
    """
    try:
        driver = getDriver(database)
        
        # Check for missing label, Key, and Name in USES relationships
        
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
        where not r.Key contains ": "
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
                with tempfile.NamedTemporaryFile(delete=False, suffix="_check_uses.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
                    result.to_excel(fp1, index=False)
                if isinstance(mail, Mail):
                    sendEmail(mail, subject=f"Check USES for {database}", recipients=[
                            "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        return {"Total": len(result), "Check USES": result.to_dict(orient="records")}

    except Exception as e:
        result = str(e)
        return result, 500


def reportChanges(database, dateStart = None, dateEnd = None, action = "default", user = None, mail = None, return_type = "data"):
    """
    This function generates a report of changes in the database based on the logs.
    Parameters:
    - database: The name of the database to check.
    - dateStart: The start date to filter changes (optional) -- returns yesterday's date if not provided.
    - dateEnd: The end date to filter changes (optional) -- returns today's date if not provided.
    - action: The type of action to filter changes (optional) -- default value is "default" which returns all of these actions "created node", "created relationship", "deleted", "merged", "changed". Can be passed as a list.
    - user: The user who made the changes (optional) -- defaults to None.
    Returns:
    - A json object containing the reported changes.
    """
    try:
        driver = getDriver(database)
        if action == "default":
            action = ["created node","created relationship", "deleted", "merged", "changed"]
        elif isinstance(action, str):
            action = [action]
        elif not isinstance(action, list):
            return "Invalid action parameter. Must be a string or a list of strings."
        if not user:
            user = ""
        data = {
            'user': user,
            'dateStart': dateStart if dateStart else (pd.Timestamp.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d'),
            'dateEnd': dateEnd if dateEnd else pd.Timestamp.now().strftime('%Y-%m-%d')
        }
        if isinstance(data['dateStart'], str):
            data['dateStart'] = pd.to_datetime(data['dateStart']).strftime('%Y-%m-%d')
        if isinstance(data['dateEnd'], str):
            data['dateEnd'] = pd.to_datetime(data['dateEnd']).strftime('%Y-%m-%d')
        if not data['user'] == "":
            data['user'] = data['user'].strip()
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
        results = getQuery(query, driver, params={"rows":data},type="df")
        
        driver_uses = getDriver("userdb")
        query_uses = """
        MATCH (u:USER) return u.userid as user, u.username as username, u.first + " " + u.last as fullname
        """
        users_df = getQuery(query_uses, driver=driver_uses, type="df")

        results = results.merge(users_df, on='user', how='left')

        fp1 = None
        if isinstance(results,pd.DataFrame):
            with tempfile.NamedTemporaryFile(delete=False, suffix="_DBchanges.xlsx", dir="/tmp") as tmpfile:
                    fp1 = tmpfile.name
            results.to_excel(fp1, index=False)
            if isinstance(mail, Mail):
                sendEmail(mail, subject=f"Database Changes for {database} and {action} from {data['dateStart']} to {data['dateEnd']}", recipients=[
                        "admin@catmapper.org"], body="See attached", sender=config['MAIL']['mail_default'], attachments=[fp1])

        if return_type == "data":
            return results.to_dict(orient="records")
        else:
            agg = results.groupby("action", as_index=False)["count"].sum()
            agg_html = agg.to_html(index=False,border=0.5, classes="dataframe", justify="left")
            return {"info": agg_html,"filepath": fp1}

    except Exception as e:
        return str(e)
    
def runRoutines(database,mail):
    files = []
    info = []
    info.append("Routines started at " + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    info.append("Modifications to " + database + ":")
    data = reportChanges(database, return_type = "info")
    info.append(data.get("info"))
    files.append(data.get("filepath"))
    
    info.append("Check Domains for " + database + ":")
    data = checkDomains(database, mail=None, return_type="info")
    info.append(data.get("info"))
    files.append(data.get("filepath"))

    # info.append("Processing USES for " + database + ":")
    # data_USES = processUSES(database, CMID = "AM1", detailed = False)
    # info.append(data_USES)
    
    # info.append("Processing DATASETs for " + database + ":")
    # data_Dataset = processDATASETs(database)
    # info.append(data_Dataset)
    
    files = [f for f in files if f is not None]

    if isinstance(mail, Mail):
        status = sendEmail(mail, subject=f"Routines for {database} - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", recipients=["rjbischo@asu.edu"], body="<br>".join(info), sender=config['MAIL']['mail_default'], attachments=files or [])
        return f"""
        Routines completed with status "{status or 'no status returned'}": <br>
        Files: <br>
        {"<br>".join(str(f) for f in (files or []) if f is not None)}
        <br>
        Info: <br>
        {"<br>".join(str(i) for i in (info or []) if i is not None)}
        """
    else:
        return info