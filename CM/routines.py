''' routines.py '''

# This is a module for automatic routines in CatMapper

from neo4j import GraphDatabase
from CM.utils import *
from CM.email import *
from CM.USES import *
import pandas as pd
import json
import tempfile


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


def addLog(database):
    try:
        driver = getDriver(database)
        query = """
        match ()-[r:USES]-() where r.log is null set r.log = custom.makeLog([],"0","added log property")
        """
        getQuery(query, driver)

        query = """
        match (c) where c.log is null set c.log = [custom.makeLog([],"0","added log property")]
        """
        getQuery(query, driver)

        return "Completed"
    except Exception as e:
        return str(e)


def checkDomains(data, database):
    try:
        if data is None:
            data = False
        elif data.lower() == "true":
            data = True
        else:
            data = False
        driver = getDriver(database)
        query = """
        match (n:CATEGORY)
        where size(labels(n)) = 1
        optional match (n)<-[r:USES]-(d:DATASET)
        return "CATEGORY" as query, n.CMID as CMID, n.CMName as CMName, r.label as label, d.CMID as datasetID
        UNION ALL
        match (n)
        where isEmpty([i in labels(n) where i in ["DATASET","METADATA","USER","CATEGORY","DELETED"]])
        optional match (n)<-[r:USES]-(d:DATASET)
        return "MissingCATEGORY" as query,  n.CMID as CMID, n.CMName as CMName, r.label as label, d.CMID as datasetID
        """
        result = getQuery(query, driver)

        if data:
            return result
        else:
            return "Completed"

    except Exception as e:
        return str(e)


# need to create function based off this
# with "eventType" as prop match (a)-[r]->(b) where not r[prop] is null and r[prop] = [] call apoc.cypher.doIt("with r set r." + prop + " = NULL",{r:r}) yield value return count(*)

def backup2CSV(database, mail=None):
    try:
        driver = getDriver(database)
        query = """
            with 'match (d:DATASET) unwind keys(d) as property return distinct id(d) as nodeID, property, d[property] as value' as query CALL apoc.export.csv.query(query, '/backups/datasetNodes.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
        """

        result = getQuery(query, driver)
        print(result)

        query2 = """
            with 'match (n:CATEGORY)<-[r:USES]-(d:DATASET) unwind keys(r) as property return distinct id(r) as relID, n.CMID as CMID, n.CMName as CMName, d.CMName as dataset, d.CMID as datasetID, property, r[property] as value order by CMName' as query CALL apoc.export.csv.query(query, '/backups/USESties.csv', {})
            YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
            RETURN count(*);
            """

        result2 = getQuery(query2, driver)
        # print(result2)

        if mail:
            sendEmail(mail, subject="Weekly CSV Backup", recipients=[
                      "rjbischo@catmapper.org"], body="Weekly CSV backup completed.", sender=os.getenv("mail_default"))

        return "backup2CSV completed"

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

                if mail is not None:
                    with tempfile.NamedTemporaryFile(delete=False, suffix="_badCMID.xlsx", dir="/tmpapi") as tmpfile:
                        fp1 = tmpfile.name
                        results.to_excel(fp1, index=False)
                    sendEmail(mail, subject=f"Bad CMIDs for {database}", recipients=[
                              "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

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

            if mail is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_multipleLabels.xlsx", dir="/tmpapi") as tmpfile:
                    fp1 = tmpfile.name
                    results.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Multiple Labels for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

            return results

        else:
            return "No multiple labels found"

    except Exception as e:
        return str(e)


def getBadJSON(database, mail=None):
    try:
        fd, fp1 = tempfile.mkstemp(suffix="_geoCoords.xlsx", dir="/tmpapi")
        os.close(fd)
        fd, fp2 = tempfile.mkstemp(suffix="_parentContext.xlsx", dir="/tmpapi")
        os.close(fd)
        results1 = validateJSON(
            database=database, property='geoCoords', path=fp1)
        results2 = validateJSON(
            database=database, property='parentContext', path=fp2)

        mailSent = "False"

        if mail is not None:
            if len(results1) > 1:
                sendEmail(mail, subject=f"Invalid geoCoords properties for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])
                mailSent = "True"

            if len(results2) > 1:
                sendEmail(mail, subject=f"Invalid parentContext properties for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp2])
                mailSent = "True"

        return {"geoCoords": len(results1), "parentContext": len(results2), "geoCoords": results1, "parentContext": results2, "emailSent": mailSent}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadDomains(database, mail=None):
    try:
        driver = getDriver(database)
        labels = getQuery(
            "match (l:LABEL) return l.label as label, l.groupLabel as groupLabel", driver, type="df")
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
            if mail is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_bad_labels.xlsx", dir="/tmpapi") as tmpfile:
                    fp1 = tmpfile.name
                    bad_labels.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Bad Labels for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

        missing_category = getQuery(
            "match (c)<-[:USES]-(d:DATASET) where not 'CATEGORY' in labels(c) return c.CMID as CMID, c.CMName as CMName", driver, type="df")

        if isinstance(missing_category, pd.DataFrame) and not missing_category.empty:
            if mail is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_missing_category.xlsx", dir="/tmpapi") as tmpfile:
                    fp1 = tmpfile.name
                    missing_category.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Missing CATEGORY Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

        missing_dataset = getQuery(
            "match (c)<-[:USES]-(d:DATASET) where not 'DATASET' in labels(d) return d.CMID as CMID, d.CMName as CMName", driver, type="df")

        if isinstance(missing_dataset, pd.DataFrame) and not missing_dataset.empty:
            if mail is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_missing_dataset.xlsx", dir="/tmpapi") as tmpfile:
                    fp1 = tmpfile.name
                    missing_dataset.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Missing DATASET Label for {database}", recipients=[
                    "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

        return {"bad_labels_count": len(bad_labels), "missing_category_count": len(missing_category), "missing_dataset_count": len(missing_dataset), "bad_labels": bad_labels.to_dict(orient="records"), "missing_category": missing_category.to_dict(orient="records"), "missing_dataset": missing_dataset.to_dict(orient="records")}
    except Exception as e:
        result = str(e)
        return result, 500


def getBadRelations(database, mail=None):
    try:
        driver = getDriver(database)
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
            if mail is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_bad_relationship_labels.xlsx", dir="/tmpapi") as tmpfile:
                    fp1 = tmpfile.name
                    results.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Bad Relationship Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

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

            if mail is not None:
                with tempfile.NamedTemporaryFile(delete=False, suffix="_BadCMNames.xlsx", dir="/tmpapi") as tmpfile:
                    fp1 = tmpfile.name
                    cmids = pd.DataFrame(cmids)
                    cmids.columns = ["CMID"]
                    cmids.to_excel(fp1, index=False)
                sendEmail(mail, subject=f"Bad Relationship Label for {database}", recipients=[
                          "admin@catmapper.org"], body="See attached", sender=os.getenv("mail_default"), attachments=[fp1])

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
    except Exception as e:
        result = str(e)
        return result, 500
