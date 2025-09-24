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
            return {"info": f"Domains check: {len(results)} results","filepath": fp1}

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
                return {"info": f"Bad Domains check: {len(results)} results", "filepath": fp1}
        else:
            if return_type == "data":
                return "No bad CMIDs found"
            elif return_type == "info":
                return {"info": "No bad CMIDs found", "filepath": None}

    except Exception as e:
        return "Error: " + str(e)


def getMultipleLabels(database, mail=None, return_type="data"):
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
                return {"info": f"Multiple labels check: {len(results)} results","filepath": fp1}

        else:
            if return_type == "data":   
                return "No multiple labels found"
            elif return_type == "info":
                return {"info": "No multiple labels found", "filepath": None}   

    except Exception as e:
        return str(e)


def getBadJSON(database, mail=None, return_type="data"):
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
            return {"info": f"Bad JSON check: {len(results1) + len(results2)} results", "filepath": [fp1, fp2]}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadDomains(database, mail=None, return_type="data"):
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
            "match (c)<-[:USES]-(d:DATASET) where not 'DATASET' in labels(d) return d.CMID as CMID, d.CMName as CMName", driver, type="df")

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
            return {"info": f"Bad Domains check: {len(bad_labels) + len(missing_category) + len(missing_dataset)} results", "filepath": [fp1, fp2, fp3]}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadRelations(database, mail=None, return_type="data"):
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
            return {"info": f"Bad Relationship Labels for {database}: {len(results)} results", "filepath": [fp1]}

    except Exception as e:
        result = str(e)
        return result, 500



def CMNameNotInName(database, mail=None, return_type="data"):
    try:
        driver = getDriver(database)

        query = """
        MATCH (n:CATEGORY)
        WHERE NOT n.CMName in n.names
        RETURN n.CMID as CMID
        """

        cmids = getQuery(query, driver, type="list")

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
            return {"info": f"CMName not in names check: {len(cmids)} results", "filepath": fp1}

    except Exception as e:
        result = str(e)
        return result, 500


def fixMetaTypes(database, return_type="data"):
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
            return {"info": "Meta types updated successfully"}
    except Exception as e:
        result = str(e)
        return result, 500

def noUSES(database, save=True, mail=None, return_type="data"):
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
            return {"info": f"Categories with no USES relationships found: {len(results)}", "filepath": fp1}
    except Exception as e:
        result = str(e)
        return result, 500
    
def checkUSES(database, save = True, mail=None, return_type="data"):
    """
    This function checks for categories that have USES relationships with datasets but various properties are not included. These categories are: label, Key, and Name.
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
            return {"info": f"Errors in USES ties: {len(result)}", "filepath": fp1}

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
    
# def runRoutines(databases = "all",mail = None):
#     files = []
#     info = []
#     info.append("Routines started at " + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'))
    
#     if databases == "all":
#         databases = ["SocioMap","ArchaMap"]
#     elif isinstance(databases, str):
#         databases = [databases]

#     for database in databases:

#         info.append("<h1>Running routines for " + database + "</h1>")

#         info.append("<h2>Modifications to " + database + ":</h2>")
#         data_changes = reportChanges(database, return_type = "info")
#         if isinstance(data_changes, str):
#             return "Error in reportChanges: " + data_changes
#         info.append(data_changes.get("info"))
#         files.append(data_changes.get("filepath"))
        
#         info.append("<h3>Check Domains for " + database + ":</h3>")
#         data_domains = checkDomains(database, mail=None, return_type="info")
#         if isinstance(data_domains, str):
#             return "Error in checkDomains: " + data_domains
#         info.append(data_domains.get("info"))
#         files.append(data_domains.get("filepath"))    
        
#         info.append("<h3>Return bad domains for " + database + ":</h3>")
#         data_badDomains = getBadDomains(database, mail=None, return_type="info")
#         if isinstance(data_badDomains, str):
#             return "Error in getBadDomains: " + data_badDomains
#         info.append(data_badDomains.get("info"))
#         files.append(data_badDomains.get("filepath"))
        
#         info.append("<h3>Check CMIDs for " + database + ":</h3>")
#         data_badCMID = getBadCMID(database, mail=None, return_type="info")
#         if isinstance(data_badCMID, str):
#             return "Error in getBadCMID: " + data_badCMID
#         info.append(data_badCMID.get("info"))
#         files.append(data_badCMID.get("filepath"))
        
#         info.append("<h3>Check Labels for " + database + ":</h3>")
#         data_labels = getMultipleLabels(database, mail=None, return_type="info")
#         if isinstance(data_labels, str):
#             return "Error in getMultipleLabels: " + data_labels
#         info.append(data_labels.get("info"))
#         files.append(data_labels.get("filepath"))
        
#         info.append("<h3>Check JSON for " + database + ":</h3>")
#         data_badJSON = getBadJSON(database, mail=None, return_type="info")
#         if isinstance(data_badJSON, str):
#             return "Error in getBadJSON: " + data_badJSON
#         info.append(data_badJSON.get("info"))
#         files.append(data_badJSON.get("filepath"))

#         info.append("<h3>Check Relationships for " + database + ":</h3>")
#         data_badRelations = getBadRelations(database, mail=None, return_type="info")
#         if isinstance(data_badRelations, str):
#             return "Error in getBadRelations: " + data_badRelations
#         info.append(data_badRelations.get("info"))
#         files.append(data_badRelations.get("filepath"))

#         info.append("<h3>Check CMName in names for " + database + ":</h3>")
#         data_CMName = CMNameNotInName(database, mail=None, return_type="info")
#         if isinstance(data_CMName, str):
#             return "Error in CMNameNotInName: " + data_CMName
#         info.append(data_CMName.get("info"))
#         files.append(data_CMName.get("filepath"))
        
#         info.append("<h3>Check for categories with no USES ties for " + database + ":</h3>")
#         data_noUSES = noUSES(database, save=True, mail=None, return_type="info")
#         if isinstance(data_noUSES, str):
#             return "Error in noUSES: " + data_noUSES
#         info.append(data_noUSES.get("info"))
#         files.append(data_noUSES.get("filepath"))

#         info.append("<h3>Check for errors in USES ties for " + database + ":</h3>")
#         data_checkUSES = checkUSES(database, save=True, mail=None, return_type="info")
#         if isinstance(data_checkUSES, str):
#             return "Error in checkUSES: " + data_checkUSES
#         info.append(data_checkUSES.get("info"))
#         files.append(data_checkUSES.get("filepath"))        

#         info.append("<h3>Processing USES for " + database + ":</h3>")
#         data_USES = processUSES(database, detailed = False)
#         info.append(data_USES)
        
#         info.append("<h3>Processing DATASETs for " + database + ":</h3>")
#         data_Dataset = processDATASETs(database)
#         info.append(data_Dataset)
        
#         info.append("<h3>Fixing metaTypes for " + database + ":</h3>")
#         data_meta = fixMetaTypes(database, return_type="info")
#         info.append(data_meta.get("info"))
    
#     flattened = []
#     for x in files:
#         if isinstance(x, list):
#             flattened.extend(x)
#         else:
#             flattened.append(x)
#     files = [f for f in flattened if f is not None]

#     if isinstance(mail, Mail):
#         status = sendEmail(mail, subject=f"Routines for {' and '.join(databases)} - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", recipients=["rjbischo@asu.edu"], body="<br>".join(info), sender=config['MAIL']['mail_default'], attachments=files or [])
#         return f"""
#         Routines completed with status "{status or 'no status returned'}": <br>
#         Files: <br>
#         {"<br>".join(str(f) for f in (files or []) if f is not None)}
#         <br>
#         Info: <br>
#         {"<br>".join(str(i) for i in (info or []) if i is not None)}
#         """
#     else:
#         return info
    
    
def runRoutinesStream(databases="all", mail=None):
    files = []
    info = []

    def emit(msg):
        info.append(msg)
        return msg + "\n\n"

    def generate():
        yield emit("Routines started at " + pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S') + "<br>")

        if databases == "all":
            dbs = ["SocioMap","ArchaMap"]
        elif isinstance(databases, str):
            dbs = [databases]
        else:
            dbs = databases

        for database in dbs:
            yield emit(f"<h1>Running routines for {database}</h1>")

            yield emit(f"<h2>Modifications to {database}:</h2>")
            data_changes = reportChanges(database, return_type="info")
            if isinstance(data_changes, str):
                yield emit("Error in reportChanges: " + data_changes)
                return
            yield emit(str(data_changes.get("info")))
            files.append(data_changes.get("filepath"))
            
            yield emit("<h3>Check Domains for " + database + ":</h3>")
            data_domains = checkDomains(database, mail=None, return_type="info")
            if isinstance(data_domains, str):
                yield emit("Error in checkDomains: " + data_domains)
                return 
            yield emit(data_domains.get("info"))
            files.append(data_domains.get("filepath"))    
            
            yield emit("<h3>Return bad domains for " + database + ":</h3>")
            data_badDomains = getBadDomains(database, mail=None, return_type="info")
            if isinstance(data_badDomains, str):
                yield emit("Error in getBadDomains: " + data_badDomains)
                return
            yield emit(data_badDomains.get("info"))
            files.append(data_badDomains.get("filepath"))
            
            yield emit("<h3>Check CMIDs for " + database + ":</h3>")
            data_badCMID = getBadCMID(database, mail=None, return_type="info")
            if isinstance(data_badCMID, str):
                yield emit("Error in getBadCMID: " + data_badCMID)
                return
            yield emit(data_badCMID.get("info"))
            files.append(data_badCMID.get("filepath"))
            
            yield emit("<h3>Check Labels for " + database + ":</h3>")
            data_labels = getMultipleLabels(database, mail=None, return_type="info")
            if isinstance(data_labels, str):
                yield emit("Error in getMultipleLabels: " + data_labels)
                return
            yield emit(data_labels.get("info"))
            files.append(data_labels.get("filepath"))
            
            yield emit("<h3>Check JSON for " + database + ":</h3>")
            data_badJSON = getBadJSON(database, mail=None, return_type="info")
            if isinstance(data_badJSON, str):
                yield emit("Error in getBadJSON: " + data_badJSON)
                return
            yield emit(data_badJSON.get("info"))
            files.append(data_badJSON.get("filepath"))

            yield emit("<h3>Check Relationships for " + database + ":</h3>")
            data_badRelations = getBadRelations(database, mail=None, return_type="info")
            if isinstance(data_badRelations, str):
                yield emit("Error in getBadRelations: " + data_badRelations)
                return
            yield emit(data_badRelations.get("info"))
            files.append(data_badRelations.get("filepath"))

            yield emit("<h3>Check CMName in names for " + database + ":</h3>")
            data_CMName = CMNameNotInName(database, mail=None, return_type="info")
            if isinstance(data_CMName, str):
                yield emit("Error in CMNameNotInName: " + data_CMName)
                return
            yield emit(data_CMName.get("info"))
            files.append(data_CMName.get("filepath"))
            
            yield emit("<h3>Check for categories with no USES ties for " + database + ":</h3>")
            data_noUSES = noUSES(database, save=True, mail=None, return_type="info")
            if isinstance(data_noUSES, str):
                yield emit("Error in noUSES: " + data_noUSES)
                return
            yield emit(data_noUSES.get("info"))
            files.append(data_noUSES.get("filepath"))

            yield emit("<h3>Check for errors in USES ties for " + database + ":</h3>")
            data_checkUSES = checkUSES(database, save=True, mail=None, return_type="info")
            if isinstance(data_checkUSES, str):
                yield emit("Error in checkUSES: " + data_checkUSES)
                return
            yield emit(data_checkUSES.get("info"))
            files.append(data_checkUSES.get("filepath"))        

            yield emit("<h3>Processing USES for " + database + ":</h3>")
            data_USES = processUSES(database, detailed = False)
            yield emit(data_USES)
            
            yield emit("<h3>Processing DATASETs for " + database + ":</h3>")
            data_Dataset = processDATASETs(database)
            yield emit(data_Dataset)
            
            yield emit("<h3>Fixing metaTypes for " + database + ":</h3>")
            data_meta = fixMetaTypes(database, return_type="info")
            yield emit(data_meta.get("info"))

        # flatten file list
        flattened = []
        for x in files:
            if isinstance(x, list):
                flattened.extend(x)
            else:
                flattened.append(x)
        files_out = [f for f in flattened if f is not None]

        # send mail only after everything is done
        if isinstance(mail, Mail):
            status = sendEmail(
                mail,
                subject=f"Routines for {' and '.join(dbs)} - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}",
                recipients=["rjbischo@asu.edu"],
                body="<br>".join(info),
                sender=config['MAIL']['mail_default'],
                attachments=files_out or []
            )
            yield emit(f'<br><h2>Mail sent with status: {status or "no status returned"}</h2>')

    return Response(stream_with_context(generate()), mimetype="text/html")