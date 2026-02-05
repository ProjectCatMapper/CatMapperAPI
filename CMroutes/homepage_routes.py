from flask import Blueprint, request, jsonify
import pandas as pd
from CM import getDriver, getQuery

homepage_bp = Blueprint('homepage', __name__)

@homepage_bp.route('/foci', methods=['GET'])
def getFoci():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        query1 = """
match (d:DATASET)
where d.foci is not null
unwind d.foci as foci with d, foci
return custom.getName(foci) as Focus, count(distinct d) as Datasets order by Focus
"""

        query2 = """
match (d:DATASET)
where d.foci is not null
optional match (d)-[:USES]->(c:CATEGORY)
with d, c unwind labels(c) as label
with d,c, label
where label in ["DISTRICT","LANGUOID","ETHNICITY","RELIGION"]
unwind d.foci as foci with foci, label, count(distinct c) as n
return custom.getName(foci) as Focus, custom.getDisplayName(label) as domain, n order by Focus, domain
"""
        data1 = getQuery(query1, driver)
        data2 = getQuery(query2, driver)


        df1 = pd.DataFrame(data1)

        df1.dropna(axis=1, how='all', inplace=True)

        df2 = pd.DataFrame(data2)

        df2.dropna(axis=1, how='all', inplace=True)

        cols = [col for col in df2.columns if col not in ['domain', 'n']]
        df2 = df2.pivot_table(index=cols, columns='domain',
                              values='n', aggfunc='first').reset_index()

        df = df1.join(df2.set_index('Focus'), on='Focus')

        columns_to_convert = df.columns.difference(['Focus'])
        df[columns_to_convert] = df[columns_to_convert].fillna(0)
        df[columns_to_convert] = df[columns_to_convert].astype(int)

        return df.to_json(orient='records')
    except Exception as e:
        # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500


@homepage_bp.route('/addFoci', methods=['GET'])
def addFoci():
    try:
        database = request.args.get('database')
        datasetID = request.args.get('datasetID')
        foci = request.args.get('foci')

        driver = getDriver(database)

        query = "MATCH (v:VARIABLE {CMID: $foci}) return v.CMID as CMID"
        verifyFoci = getQuery(query, driver, params={"foci": foci})

        query = "MATCH (d:DATASET {CMID: $datasetID}) return d.CMID as CMID"
        verifydb = getQuery(query, driver, params={"datasetID": datasetID})

        if not datasetID in [item["CMID"] for item in verifydb]:
            raise Exception("datasetID does not exist - please check the CMID")

        if foci in [item["CMID"] for item in verifyFoci]:
            query = "MATCH (d:DATASET {CMID: $datasetID}) with d, apoc.coll.toSet(coalesce(d.foci,[]) + $foci) as result set d.foci = result return d.CMID as datasetID, d.foci as foci"
            result = getQuery(query, driver, params={
                "foci": foci, "datasetID": datasetID})
        else:
            raise Exception("foci does not exist - please check the CMID")

        return result

    except Exception as e:
        result = str(e)
        return result, 500
    
@homepage_bp.route('/homepagecount',methods=['GET'])
def gethomepageCount():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        database_lower = database.lower()

        # Define the mappings for each database type
        if database_lower == "sociomap":
            # Mapping: { "DATABASE_LABEL": "Display Name" }
            mapping = {
                "ETHNICITY": "Ethnicities",
                "RELIGION": "Religions",
                "LANGUOID": "Languages",
                "DISTRICT": "Districts"
            }
        elif database_lower == "archamap":
            mapping = {
                "SITE": "Sites",
                "PERIOD": "Periods",
                "CULTURE": "Cultures",
                "CERAMIC": "Artifact",
                "STONE": "Artifact",
                "PROJECTILE_POINT": "Artifact",
                "WEAPON": "Artifact",
                "COIN": "Artifact"
            }
        else:
            raise Exception("database not recognized")

        # Query uses the keys from our mapping
        query = """
            UNWIND $labels AS lbl
            RETURN lbl AS label, apoc.meta.nodes.count([lbl]) AS node_count
        """

        raw_data = getQuery(query=query, driver=driver, params={"labels": list(mapping.keys())})

        # logic for Archamap: Summing artifacts into one display row
        if database_lower == "archamap":
            counts = {item['label']: item['node_count'] for item in raw_data}
            artifact_labels = ["CERAMIC", "STONE", "PROJECTILE_POINT", "WEAPON", "COIN"]
            total_artifacts = sum(counts.get(lbl, 0) for lbl in artifact_labels)
            
            # Manually construct the combined list with the 'display' key
            data = [
                {"label": "SITE", "display": "Sites", "node_count": counts.get("SITE", 0)},
                {"label": "PERIOD", "display": "Periods", "node_count": counts.get("PERIOD", 0)},
                {"label": "CULTURE", "display": "Cultures", "node_count": counts.get("CULTURE", 0)},
                {"label": "Artifact", "display": "Artifact Types", "node_count": total_artifacts}
            ]
        else:
            # Logic for Sociomap: Simple 1-to-1 mapping
            data = []
            for item in raw_data:
                data.append({
                    "label": item["label"],
                    "display": mapping.get(item["label"]),
                    "node_count": item["node_count"]
                })

        return data

    except Exception as e:
        result = str(e)
        return result, 500



@homepage_bp.route('/progress', methods=['GET'])
def getProgress():
    try:
        database = request.args.get('database')

        driver = getDriver(database)

        query = """
        match (l:LABEL)
        where l.public = 'TRUE' and l.groupLabel = l.CMName and not l.CMName IN ["CATEGORY","GENERIC"]
        return l.CMName as label, l.displayName as newlabel
        """
        domains = getQuery(query=query, driver=driver, type = "df")

        query = """
        UNWIND $labels AS label
        RETURN 
            label, 
            apoc.meta.nodes.count([label]) AS current, 
            'nodes' AS type
        ORDER BY label

        UNION

        CALL apoc.meta.stats() YIELD relTypesCount
        UNWIND keys(relTypesCount) AS relType
        WITH relType, relTypesCount[relType] AS current
        WHERE NOT relType IN ["IS", "MERGING"]
        RETURN 
            relType AS label, 
            current, 
            'relations' AS type
        ORDER BY label

        UNION

        MATCH (:DATASET)-[:USES]->(b)
        WITH labels(b) AS lbls
        UNWIND lbls AS label
        WITH label, count(*) AS current
        WHERE label IN ["SITE", "CULTURE", "PERIOD"]
        RETURN 
            label, 
            current, 
            'encodings' AS type
        ORDER BY label
        """

        data = getQuery(query=query, driver=driver, params={
            "labels": domains["label"]})

        df = pd.DataFrame(data)

        query = """
        match (n:TRANSLATION) where n.table = "display" return n.table as table, n.from as label, n.to as newlabel order by label
        """
        translations = getQuery(query=query, driver=driver)
        translations = pd.DataFrame(translations)
        translations = pd.concat(
            [translations, domains], axis=0, ignore_index=True)
        translations = translations.drop('table', axis=1)

        df = df.merge(translations, on="label", how="inner")
        df = df.drop('label', axis=1)
        df = df.rename(columns={'newlabel': 'label'})

        nodes = df[df['type'] == 'nodes'].copy()
        nodes = nodes.drop('type', axis=1)
        nodes = nodes.to_dict(orient='records')

        encodings = df[df['type'] == 'encodings'].copy()
        encodings = encodings.drop('type', axis=1)
        encodings = encodings.to_dict(orient='records')

        relations = df[df['type'] == 'relations'].copy()
        relations = relations.drop('type', axis=1)
        relations = relations.to_dict(orient='records')

        return {"nodes": nodes, "encodings": encodings, "relations": relations}

    except Exception as e:
        result = str(e)
        return result, 500