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

        with driver.session() as session:
            result1 = session.run(query1)
            result2 = session.run(query2)
            data1 = [dict(record) for record in result1]
            data2 = [dict(record) for record in result2]
            driver.close()

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

        if database == "SocioMap":
            domains = ["ETHNICITY","RELIGION","LANGUOID","DISTRICT"]
        elif database == "ArchaMap":
            domains = ["SITE","PERIOD","CULTURE","CERAMIC","STONE","PROJECTILE_POINT","WEAPON","COIN"]
        
        query = """
                UNWIND $labels AS lbl
                RETURN lbl AS label, count { MATCH (n) WHERE lbl IN labels(n) } AS node_count
                """

        data = getQuery(query=query, driver=driver, params={
            "labels": domains})
        
        if database == "ArchaMap":
            data.append({"label": "Artifact","node_count":data[3]["node_count"] + data[4]["node_count"] + data[5]["node_count"] + data[6]["node_count"] + data[7]["node_count"]})
            del data[7]
            del data[6]
            del data[5]
            del data[4]
            del data[3]
        
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
        domains = getQuery(query=query, driver=driver)
        domains = pd.DataFrame(domains)

        query = """
        match (a)
        unwind labels(a) as label
        with label, count(*) as current
        where label in $labels
        return label, current, 'nodes' as type
        order by label
        union match ()-[r]->()
        where not type(r) in ["IS","MERGING"]
        with type(r) as label, count(*) as current
        return label, current, 'relations' as type
        order by label
        union match (a:DATASET)-[r:USES]->(b)
        unwind labels(b) as label
        with label, count(r) as current
        where label in $labels
        return distinct label, current, 'encodings' as type
        order by label
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
