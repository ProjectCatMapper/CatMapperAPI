#from fastapi import FastAPI
from flask import Flask,request
from flask import jsonify
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from flask_cors import CORS
from fuzzywuzzy import fuzz
import json
import ast
import string as str

load_dotenv()
uri = os.getenv("uri")
user = os.getenv("user")
pwd = os.getenv("pwd")
uri1 = os.getenv("uri1")
user1 = os.getenv("user1")
pwd1 = os.getenv("pwd1")
uriAM = os.getenv("uriAM")
pwdAM = os.getenv("pwdAM")

def connectionSM():
    driverSM = GraphDatabase.driver(uri=uri,auth=(user,pwd))
    return driverSM

def connectionGIS():
    driverGIS = GraphDatabase.driver(uri=uri1,auth=(user1,pwd1))
    return driverGIS

def connectionAM():
    driverAM = GraphDatabase.driver(uri=uriAM,auth=(user,pwdAM))
    return driverAM

def verifyUser(driver,user,pwd):
    with driver.session() as session:
        query = "match (u:USER {userID: $user,password: $pwd}) return true as verified"
        result = session.run(query, user = user, pwd = pwd)
        result = [dict(record) for record in result]
        driver.close()
    return result

def getPolygon(CMID,driver):
    try:
        with driver.session() as session:
            query = "match (:CATEGORY {CMID: $CMID})<-[r:USES]-(:DATASET) where not r.geoPolygon is null return distinct r.geoPolygon as geomID"
            result = session.run(query, CMID = CMID)
            for record in result:
                geomID = record["geomID"]
            driver.close()
        driverGIS = connectionGIS() 
        print(driverGIS)
        with driverGIS.session() as session:
            query = "with apoc.coll.toSet(apoc.coll.flatten([$geomID],true)) as geomid match (g:GEOMETRY) where g.geomID in geomid return g.geometry as geometry"
            result = session.run(query, geomID = geomID)
            geometry = [dict(record) for record in result]
            driverGIS.close()
        return geometry
    except Exception as e: 
        print(e)

        return e


#app=FastAPI()
app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS']='Content-Type'
app.config['PERMANENT_SESSION_LIFETIME'] = 999999999

@app.route("/")
def root ():
    return {"response":"you are in root"}

#tvalue = request.json['tvalue']
 
text =['','','']
@app.route("/count",methods=['GET','POST'])
def abst():
    results = []
    results1 =[]
    if request.method == 'GET':
        text[0] = request.args.get('label')
        text[1] = request.args.get('value')
        text[2] = request.args.get('options')
        print(text[0])
        print(text[1])
        print(text[2])
        driver_neo4j = connectionSM()
        session = driver_neo4j.session()
        if text[2] == "" or text[2] == "Name":
          if text[1] == "":
            q1 = "MATCH (n:"+text[0]+") RETURN n"
            results = session.run(q1)
            #return {"response":[{"Name":row["properties"]} for row in results]}(
            return results.data()
          else:
            q1 = "MATCH (n:"+text[0]+") RETURN n"
            #-[:DISTRICT_OF]-(m:COUNTRY)
            #q1 = "MATCH (n:"+text[0]+") WHERE NONE(prop in keys(n) where TOSTRING(n[prop]) CONTAINS "+text[1]+") RETURN n"
            results = session.run(q1)
            results = results.data()
            for i in range(0,len(results)):
                temp = str(results[i])
                #if text[1] in temp:
                if fuzz.WRatio(text[1],temp) > 40:
                    results1.append(results[i])
            #return {"response":[{"Name":row["properties"]} for row in results]}(
            print(results1)
            return results1
        else:
          if text[1] == "":
            q1 = "MATCH (n:"+text[0]+") RETURN n"
            results = session.run(q1)
            #return {"response":[{"Name":row["properties"]} for row in results]}(
            return results.data()
          else:
            q1 = "MATCH (n:"+text[0]+") where n."+text[2]+" = '"+text[1]+"' RETURN n"
            results = session.run(q1)
            results = results.data()
            #print(results)
            return jsonify(results)
        
        

@app.route("/category",methods=['GET'])
def catm():
    center = 0
    relnames= []
    relations = ["USES","CONTAINS","DISTRICT_OF","LANGUOID_OF","RELIGION_OF"]
    cmid = request.args.get('value')
    driver_neo4j = connectionSM()
    session = driver_neo4j.session()
    driverGIS = connectionGIS()
    sessionGIS = driverGIS.session()
    q = f"match (a) where a.CMID = '{cmid}' optional match (a)-[r]-() return distinct head(labels(a)) as label, type(r) as types"
    result = session.run(q)
    for record in result:
        label = record["label"]
        rel_name = record["types"]
    for i in rel_name:
        if i in relations:
            relnames.append(i['label'])
    q =   f''' 
match (a)<-[r:USES]-(d:DATASET)
where a.CMID = '{cmid}'
with custom.anytoList(collect(r.Name),true) as Name, r.country as LocationID, d.project as Source, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd, 
toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
call apoc.when(LocationID is not null,'return custom.getName($id) as Location','return null',{{id:LocationID}}) yield value
return Name, custom.anytoList(collect(value.Location),true) as Location, type as Type, 
apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),
toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),
toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Population est.`,  
apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`, Source, Version, Link order by `Time span`, Source, Name
'''
    results = session.run(q)
    results = [dict(record) for record in results]
    # q1 = '''match (a)<-[r:USES]-(d:DATASET) where a.CMID = "'''+cmid+'''" and (r.geoCoords is not null or r.geoPolygon is not null) return r.geoCoords as point, r.geoPolygon as polygon, d.shortName as source, r.Key as Key'''
    # results1 = session.run(q1)
    # resultsm = results1.data()
    polygon = getPolygon(cmid,driver_neo4j)
    # return jsonify({"results":[results],"polygon":[polygon]})
        # points = []
        # flag = 0
        # try:
        #     for i in range(0,len(resultsm)):
        #         if resultsm[i]['polygon'] is not None:
        #             print("...................................")
        #             flag =1
        #             geomid = resultsm[i]['polygon']
        #             print(type(geomid))
        #             if isinstance(geomid,list):
        #                 geomid = str(geomid[0])
        #                 q1 = '''match (g:GEOMETRY)<-[r:USES {geomid: "'''+geomid+'''"}]-() return g.geometry'''
        #                 results1=''
        #                 results1 = sessionGIS.run(q1)
        #                 results1 = results1.data()
        #                 results1 = (results1[0]['g.geometry']).replace("u\'","\'")
        #                 results1 = json.loads(results1)
        #                 with open('data.json', 'w', encoding='utf-8') as f:
        #                     json.dump(results1, f, ensure_ascii=False, indent=4)
        #                 if results1['type'] == "Polygon":
        #                     center = (results1['coordinates'][0][0])[::-1]
        #                 if results1['type'] == "MultiPolygon":
        #                     center = (results1['coordinates'][0][0][0])[::-1]
        #             else:
        #                 geomid = str(geomid)
        #                 q1 = '''match (g:GEOMETRY {geomID: "'''+geomid+'''"}) return g.geometry'''
        #                 results1=''
        #                 results1 = sessionGIS.run(q1)
        #                 results1 = results1.data()
        #                 results1 = (results1[0]['g.geometry']).replace("u\'","\'")
        #                 if "features" in results1:
        #                     results1 = json.loads(results1)
        #                     results1 = results1['features'][0]
        #                     if results1['geometry']['type'] == "Polygon":
        #                         center = (results1['geometry']['coordinates'][0][0])[::-1]
        #                     if results1['geometry']['type'] == "MultiPolygon":
        #                         center = (results1['geometry']['coordinates'][0][0][0])[::-1]
        #         else:
        #             results1 = json.loads(results1)
        #             if results1['type'] == "Polygon":
        #                 center = (results1['coordinates'][0][0])[::-1]
        #             if results1['type'] == "MultiPolygon":

        #                 center = (results1['coordinates'][0][0][0])[::-1]

        #             with open('data.json', 'w', encoding='utf-8') as f:
        #                 json.dump(results1, f, ensure_ascii=False, indent=4)

                        
        #         break
            
        #     if flag == 0:
        #         results1=[]

            
            # poid=[]
            # for i in range(0,len(resultsm)):
            #     if resultsm[i]['point'] is not None:
            #         #poid[resultsm[i]['source']] = json.loads(resultsm[i]['point'])['coordinates'][0]
            #         print((resultsm[i]['point']))
            #         if isinstance(resultsm[i]['point'], list):
            #             cood=(json.loads(resultsm[i]['point'][0])['coordinates'])
            #         else:
            #             cood=(json.loads(resultsm[i]['point'])['coordinates'])
            #         if isinstance(cood, list):
            #             cood = cood[::-1]
            #         print(cood)
            #         poid.append(dict([("id",resultsm[i]['source']),('coordinates',cood)]))
                    #poid[i]['id'] = resultsm[i]['source']
                    #poid[i]['coordinates'] = json.loads(resultsm[i]['point'])['coordinates'][0]
            # 
            # print(poid)

            # '''
            # for obj in results1:
            #      if "coordinates" in obj:
            #          northing = obj["coordinates"][0]
            #          easting = obj["coordinates"][1]
            #          obj["coordinates"] = [ easting, northing ]
            # '''
            # #results1 = results1['features'][0]['geometry']['coordinates'][0]
            # #return '{} {}'.format(results.data(), results1)
        # except Exception as e:
        # # In case of an error, return an error response with an appropriate HTTP status code
        #     print(e)
        #     results1 = []
        # print(polygon)
        # print(type(polygon))
    payload = {
    "current_response": results,
    "future_response": [polygon],
    # "center": center,
    # "poid": poid,
    "label":label,
    "relnames": relnames
    }
        
    #     print(payload)
    return jsonify(payload)
        #return (results.data())

# Function to serialize a Neo4j Node object into a serializable dictionary
def serialize_node(node):
    return {
        "id": node.id,
        "labels": list(node.labels),
        "properties": dict(node)
    }

# Function to serialize Neo4j Relationship object into a serializable dictionary
def serialize_relationship(relationship):
    return {
        "type": relationship.type,
        "start_node_id": relationship.start_node.id,
        "end_node_id": relationship.end_node.id,
        "properties": dict(relationship.items())
    }

@app.route('/properties', methods=['GET'])
def getProperties():
    try:
        cmid = request.args.get('cmid')
        database = request.args.get('database')

        if database == "SocioMap":
            driverSM = connectionSM()
        elif database == "ArchaMap":
            driverSM = connectionAM()
        else:
            raise Exception("must specify database as SocioMap or ArchaMap")
        
        with driverSM.session() as session:
            # Define the Cypher query
            cypher_query = f"MATCH (a) WHERE a.CMID = '{cmid}' optional match (a)<-[r:USES]-(d:DATASET) return a,r,d"
            
            # Execute the Cypher queries
            result = session.run(cypher_query)
        
            # Process the query results and generate the dynamic JSON
            data = {
                "CATEGORY": {},
                "USES": {}  # Use a dictionary instead of a list
            }

            unique_nodes = set()  # To track unique nodes

            for record in result:
                a = record['a']
                r = record['r']
                d = record['d']

                # Ensure nodes are unique
                if a is not None:
                    data["CATEGORY"] = serialize_node(a)
                    unique_nodes.add(a)

                # Create a dictionary for the relationship and dataset data with r.id as the key
                if r is not None:
                    if r.id not in data["USES"]:
                        data["USES"][r.id] = {
                            "relationship": {},
                            "dataset": {}
                        }

                    # Add relationship data
                    data["USES"][r.id]["relationship"] = serialize_relationship(r)

                # Add dataset data
                if d is not None:
                    data["USES"][r.id]["dataset"] = serialize_node(d)

            driverSM.close()
                
        return jsonify(data)
    
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        error_message = str(e)  # Convert the exception to a string
        data = {"error": error_message}

        return jsonify(data), 500
    
@app.route('/search', methods=['GET'])
def getSearch():
    try:
        database = request.args.get('database')
        term = request.args.get('term')
        property = request.args.get('property')
        domain = request.args.get('domain')
        yearStart = request.args.get('yearStart')
        yearEnd = request.args.get('yearEnd')
        context = request.args.get('context')
        limit = request.args.get('limit')
        query = request.args.get('query')

        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        else:
            raise Exception("must specify database as 'SocioMap' or 'ArchaMap'")
        
        if domain is None:
            domain = "CATEGORY"

        # need to add check to make sure property is valid and domain is valid

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
call db.index.fulltext.queryRelationships('keys',replace($term,':','\\:')) yield relationship
with endnode(relationship) as a, relationship.Key as matching
where '{domain}' in labels(a)
with a, matching, 0 as score
"""
        elif property == "Name":
                qStart = f"""
call {{ with custom.cleanText($term) as term
call db.index.fulltext.queryNodes('{domain}', replace(term,"'","\\'")) yield node return node
union with custom.cleanText($term) as term
call db.index.fulltext.queryNodes('{domain}',replace(term,"'","\\'") + '~') yield node return node}}
with node as a, node.CMName as matching, apoc.text.levenshteinDistance(custom.cleanText(node.CMName), custom.cleanText($term)) as score
"""
        else:
            qStart = f"""
match (a) where tolower(a.{property}) = tolower($term)
with a, a.{property} as matching, 0 as score
"""

        # filter by domain

        qDomain = f" where '{domain}' in labels(a) "

        qUnique = """
with a, collect(matching) as matchingL, collect(score) as scores call {with matchingL, scores unwind matchingL as matching unwind scores as score return distinct matching, score order by score limit 1}
with a, matching, score
"""



            # filter by context
        if context is not None:
            qContext = """
where (a)<-[]-({CMID: $context})
with a, matching, score
"""
        else:
            context = ""
            qContext = " "

            # filter by year
        if yearStart is not None:
            if domain == "DATASET":
                qYear = """
call {with a with a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
else a.ApplicableYears end as yearMatch, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
with a, years, apoc.convert.toIntList(apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}
with node as a, matching, score
"""   
            else:
                qYear = """
call {with a with a, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years 
match (a)<-[r:USES]-(:DATASET) unwind r.yearStart as yearStart 
unwind r.yearEnd as yearEnd with years, a, r, apoc.coll.toSet(collect(yearStart) + collect(yearEnd)) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}
with node as a, matching, score order by score desc
"""   
        else: 
            qYear = " "
        
        # limit results
        qLimit = f"with distinct a, matching, score order by score limit {limit} "

        # get country
        qCountry = """
optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
with a, matching, collect(c.CMName) as country, score
"""



        # return results
        qReturn = """
return distinct a.CMID as CMID, a.CMName as CMName, 
[i in labels(a) where not i = 'CATEGORY'] as domain, matching, score as matchingDistance, 
country order by matchingDistance
"""

        cypher_query = qStart + qDomain + qUnique + qContext + qYear + qLimit + qCountry + qReturn
            
        if query != 'true':   
            # Execute the Cypher queries
            with driver.session() as session:
                result = session.run(cypher_query, term = term, context = context)
            
                # Process the query results and generate the dynamic JSON
                data = [dict(record) for record in result]

                driver.close()
                    
            return jsonify(data)
        else:
            # return([qStart,qDomain,qUnique,qContext,qYear,qLimit,qCountry,qReturn])
            return(cypher_query)
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        error_message = str(e)  # Convert the exception to a string
        data = {"error": error_message}

        return jsonify(data), 500

@app.route('/translate', methods=['GET','POST'])
def getTranslate():
    try:
        if request.method == "GET":
            database = request.args.get('database')
            term = request.args.get('term')
            property = request.args.get('property')
            domain = request.args.get('domain')
            yearStart = request.args.get('yearStart')
            yearEnd = request.args.get('yearEnd')
            context = request.args.get('context')
            dataset = request.args.get('dataset')
            query = request.args.get('query')
            # if request.method == 'POST':
            #     data = request.get_data()
            term = ast.literal_eval(term)
            x = len(term)
            empty = '"",' * x
            empty = empty.rstrip(",")
            if yearStart is None:
                yearStart = f'[{empty}]'
            if yearEnd is None:
                yearEnd = f'[{empty}]'
            if context is None:
                context = f'[{empty}]'
            if dataset is None:
                dataset = f'[{empty}]'
            if query is None:
                query = 'false'

            domain = ast.literal_eval(domain)
            dom = domain[0]
            yearStart = ast.literal_eval(yearStart)
            yearEnd = ast.literal_eval(yearEnd)
            context = ast.literal_eval(context)
            dataset = ast.literal_eval(dataset)
            rows = []
            for term_item, domain_item, context_item, dataset_item, yearStart_item, yearEnd_item in zip(term, domain, context, dataset, yearStart, yearEnd):
                rows.append({"term": term_item, "domain": domain_item, "context": context_item, "dataset": dataset_item, "yearStart": yearStart_item, "yearEnd": yearEnd_item})

        if request.method == 'POST':
            rows = request.get_data()  
            rows = json.loads(rows)
            database = rows.get("database")[0]
            context = rows.get("context")
            if context == {}:
                context = [""]
            dataset = rows.get("dataset")
            if dataset == {}:
                dataset = [""]
            property = rows.get("property")[0]
            domain = rows.get("domain")
            dom = domain[0]
            yearStart = rows.get("yearStart")
            if yearStart == {}:
                yearStart = [""]
            query = rows.get("query")[0]
            if query == {}:
                query = "false"
            rows = rows.get("rows")

        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")
        
        # Define the Cypher query
        
        qLoad = "unwind $rows as row with row call {"

        if property == "Key":
                qStart = """
with row call db.index.fulltext.queryRelationships('keys',replace(row.term,':','\\:')) yield relationship
with endnode(relationship) as a, relationship.Key as matching
with row, a, matching, 0 as score
"""
        elif property == "Name":
            qStart = f"""
with row call {{ with row with row, custom.cleanText(row.term) as term
call db.index.fulltext.queryNodes('{dom}', replace(term,\"'\",'\\\'')) yield node return node
union with row with row, custom.cleanText(row.term) as term
call db.index.fulltext.queryNodes('{dom}',replace(term,\"'\",'\\\'') + '~') yield node return node}}
with row, node as a, node.CMName as matching, apoc.text.levenshteinDistance(custom.cleanText(node.CMName), custom.cleanText(row.term)) as score
"""
        else:
            qStart = """ 
with row call apoc.cypher.run('match (a) where tolower(a.' + row.property + ') = tolower(\"' + row.term + '\") return a, a.' + row.property + ' as matching',{}) yield value 
with row, value.a as a, value.matching as matching, 0 as score
"""

        # filter by domain

        qDomain = f" where row.domain in labels(a) with row, a, matching, score "

        # filter by context
        if context[0] != "":
            qContext = """
where (a)<-[]-({CMID: row.context})
with row, a, matching, score
"""
        else:
            qContext = " "

        # filter by dataset
        if dataset[0] != "":
            qDataset = """
where (a)<-[:USES]-(:DATASET {CMID: row.dataset})
with row, a, matching, score
"""
        else:
            qDataset = " "

            # filter by year
        if yearStart[0] != "":
            if dom == "DATASET":
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
return distinct a, matching, score}
with row, a, matching, score
"""

        # get country
        qCountry = """
optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
with row, a, matching, collect(c.CMName) as country, score
"""



        # return results
        if request.method == 'POST':
            qReturn = """
return distinct row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, [i in labels(a) where not i = 'CATEGORY'] as label, 
matching, score as matchingDistance, country order by matchingDistance
"""
        else:    
            qReturn = """
return distinct row.term as term, a.CMID as CMID, a.CMName as CMName, [i in labels(a) where not i = 'CATEGORY'] as label, 
matching, score as matchingDistance, country order by matchingDistance
"""
        cypher_query = qLoad + qStart + qDomain + qContext + qDataset + qYear + qLimit + qCountry + qReturn
        if query == "true":
            return [{"query": cypher_query,"params":rows}]
        else:
            # Execute the Cypher queries
            with driver.session() as session:
                result = session.run(cypher_query, rows = rows)
            
                # Process the query results and generate the dynamic JSON
                if(query == "true"):
                    data = [cypher_queryQ.replace("\n"," "),result.value()]
                else:
                    data = [dict(record) for record in result]

                driver.close()
                    
            return jsonify(data)

    
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        error_message = str(e)  # Convert the exception to a string
        data = {"error": error_message}

        return jsonify(data), 500

@app.route('/query', methods=['POST'])
def getTest():
    try:
        rows = request.get_data()  
        rows = json.loads(rows)
        database = rows.get("database")[0]
        query = rows.get("query")[0]
        user = rows.get("user")[0]
        pwd = rows.get("pwd")[0]
        params = rows.get("params")
        
        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        
        try:
            verified = verifyUser(driver,user,pwd)
            verified = verified[0]

            for item in verified:
                verified = item
            
        except Exception as e:
            error_message = "User is not verified"  # Convert the exception to a string
            data = {"error": error_message,"response":str(e)}

            return jsonify(data), 500
    
        if verified == "verified":
            with driver.session() as session:
                result = session.run(query,params)
                data = [dict(record) for record in result]
                driver.close()
            return jsonify(data)
        else:
            data = {"error": "User is not verified","verified": verified}
            return jsonify(data)

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        error_message = str(e)  # Convert the exception to a string
        data = {"error": error_message}

        return jsonify(data), 500


@app.route('/geometry', methods=['GET'])
def getGeometry():
    try:
        database = request.args.get('database')
        cmid = request.args.get('cmid')
        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        else:
            pass

        geometry = getPolygon(cmid,driver)
        return geometry
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        error_message = e  # Convert the exception to a string
        data = {"error": error_message}

        return jsonify(data), 500

if __name__== "__main__":
    app.run(debug=True,port=5001)




