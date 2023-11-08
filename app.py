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

def connection():
    driver = GraphDatabase.driver(uri=uri,auth=(user,pwd))
    return driver

def connection1():
    driver1 = GraphDatabase.driver(uri=uri1,auth=(user1,pwd1))
    return driver1

def connectionAM():
    driverAM = GraphDatabase.driver(uri=uriAM,auth=(user,pwdAM))
    return driverAM

#app=FastAPI()
app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS']='Content-Type'

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
        driver_neo4j =connection()
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
            return results
        
        

socioid=[""]
@app.route("/category",methods=['GET'])
def catm():
        center = 0
        relnames= []
        relations = ["USES","CONTAINS","DISTRICT_OF","LANGUOID_OF","RELIGION_OF"]
        socioid[0] = request.args.get('value')
        driver_neo4j =connection()
        session = driver_neo4j.session()
        driver_neo4j1 =connection1()
        session1 = driver_neo4j1.session()
        q = "match (a) where a.CMID = '"+socioid[0]+"' return id(a) as id,labels(a) as label"
        r = session.run(q)
        r = str(r.data()[0]['id'])
        label = session.run(q)
        label = str(label.data()[0]['label'][-1])
        q = "MATCH (n:"+label+" {CMID:'"+socioid[0]+"'})-[r]-(n1) RETURN DISTINCT TYPE(r) as label"
        rel_name = session.run(q).data()
        for i in rel_name:
            if i['label'] in relations:
                relnames.append(i['label'])
        q =   ''' match (a)<-[r:USES]-(d:DATASET)
where id(a) = '''+r+'''
with custom.anytoList(collect(r.Name),true) as Name, r.country as LocationID, d.project as Source, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd, toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
call apoc.when(LocationID is not null,'return custom.getName($id) as Location','return null',{id:LocationID}) yield value
return Name, custom.anytoList(collect(value.Location),true) as Location, type as Type, apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Population est.`,  apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`, Source, Version, Link order by `Time span`, Source, Name'''
        results = session.run(q)
        q1 = '''match (a)<-[r:USES]-(d:DATASET) where id(a) = '''+r+''' and (r.geoCoords is not null or r.geoPolygon is not null) return r.geoCoords as point, r.geoPolygon as polygon, d.shortName as source, r.Key as Key'''
        results1 = session.run(q1)
        resultsm = results1.data()
        flag = 0
        for i in range(0,len(resultsm)):
            if resultsm[i]['polygon'] is not None:
                print("...................................")
                flag =1
                relid = resultsm[i]['polygon']
                if isinstance(relid,list):
                    relid = str(relid[0])
                    q1 = '''match (g:GEOMETRY)<-[r:USES {relid: "'''+relid+'''"}]-() return g.geometry'''
                    results1=''
                    results1 = session1.run(q1)
                    results1 = results1.data()
                    results1 = (results1[0]['g.geometry']).replace("u\'","\'")
                    results1 = json.loads(results1)
                    with open('data.json', 'w', encoding='utf-8') as f:
                         json.dump(results1, f, ensure_ascii=False, indent=4)
                    if results1['type'] == "Polygon":
                        center = (results1['coordinates'][0][0])[::-1]
                    if results1['type'] == "MultiPolygon":
                        center = (results1['coordinates'][0][0][0])[::-1]
                else:
                    relid = str(relid)
                    q1 = '''match (g:GEOMETRY)<-[r:USES {relid: "'''+relid+'''"}]-() return g.geometry'''
                    results1=''
                    results1 = session1.run(q1)
                    results1 = results1.data()
                    results1 = (results1[0]['g.geometry']).replace("u\'","\'")
                    if "features" in results1:
                        results1 = json.loads(results1)
                        results1 = results1['features'][0]
                        if results1['geometry']['type'] == "Polygon":
                            center = (results1['geometry']['coordinates'][0][0])[::-1]
                        if results1['geometry']['type'] == "MultiPolygon":
                            center = (results1['geometry']['coordinates'][0][0][0])[::-1]
                    else:
                        results1 = json.loads(results1)
                        if results1['type'] == "Polygon":
                            center = (results1['coordinates'][0][0])[::-1]
                        if results1['type'] == "MultiPolygon":

                            center = (results1['coordinates'][0][0][0])[::-1]
                    with open('data.json', 'w', encoding='utf-8') as f:
                         json.dump(results1, f, ensure_ascii=False, indent=4)
                    
                break
        
        if flag == 0:
            results1=[]

        
        poid=[]
        for i in range(0,len(resultsm)):
            if resultsm[i]['point'] is not None:
                #poid[resultsm[i]['source']] = json.loads(resultsm[i]['point'])['coordinates'][0]
                print((resultsm[i]['point']))
                if isinstance(resultsm[i]['point'], list):
                    cood=(json.loads(resultsm[i]['point'][0])['coordinates'])
                else:
                    cood=(json.loads(resultsm[i]['point'])['coordinates'])
                if isinstance(cood, list):
                    cood = cood[::-1]
                print(cood)
                poid.append(dict([("id",resultsm[i]['source']),('coordinates',cood)]))
                #poid[i]['id'] = resultsm[i]['source']
                #poid[i]['coordinates'] = json.loads(resultsm[i]['point'])['coordinates'][0]
        
        print(poid)

        '''
        for obj in results1:
             if "coordinates" in obj:
                 northing = obj["coordinates"][0]
                 easting = obj["coordinates"][1]
                 obj["coordinates"] = [ easting, northing ]
        '''
        #results1 = results1['features'][0]['geometry']['coordinates'][0]
        #return '{} {}'.format(results.data(), results1)
        
        payload = {
    "current_response": results.data(),
    "future_response": results1,
    "center": center,
    "poid": poid,
    "label":label,
    "relnames": relnames
}
        
        print(payload)
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
            driver = connection()
        elif database == "ArchaMap":
            driver = connectionAM()
        else:
            raise Exception("must specify database as SocioMap or ArchaMap")
        
        with driver.session() as session:
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

            driver.close()
                
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

        if database == "SocioMap":
            driver = connection()
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
            limit = 10
        
        if property is None and term is not None:
            raise Exception("Must specify a property (e.g., Name, CMID, or Key)")
        
        with driver.session() as session:
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
                if domain == "DATASET":
                    qStart = """
                        call { with custom.cleanText($term) as term
                        call db.index.fulltext.queryNodes('DATASET', replace(term,\"'\",'\\\'')) yield node return node
                        union with custom.cleanText($term) as term
                        call db.index.fulltext.queryNodes('DATASET',replace(term,\"'\",'\\\'') + '~') yield node return node}
                        with node as a, node.CMName as matching, apoc.text.levenshteinDistance(custom.cleanText(node.CMName), custom.cleanText($term)) as score
                        """
                else:
                    qStart = f"""
                        call {{ with custom.cleanText($term) as term
                        call db.index.fulltext.queryRelationships('CATEGORY', replace(term,\"'\",'\\\'')) yield relationship return relationship
                        union with custom.cleanText($term) as term
                        call db.index.fulltext.queryRelationships('CATEGORY',replace(term,\"'\",'\\\'') + '~') yield relationship return relationship}}
                        match (a:{domain})<-[relationship]-()
                        call {{ with relationship with custom.anytoList(collect(relationship.Name)) as namelist
                        return custom.matchingDist(apoc.coll.flatten([namelist],true),$term) as matching
                        }}
                        with a, matching.matching as matching, matching.score as score
                        """
            else:
                qStart = f""""
                match (a:{domain}) where tolower(a.{property}) = tolower($term)
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
            
            # Execute the Cypher queries
            result = session.run(cypher_query, term = term, context = context)
        
            # Process the query results and generate the dynamic JSON
            data = [dict(record) for record in result]

            driver.close()
                
        return jsonify(data)
    
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
            rows = list(rows)

        if database == "SocioMap":
            driver = connection()
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
            if dom == "DATASET":
                qStart = """
                            with row call { with row with row, custom.cleanText(row.term) as term
                            call db.index.fulltext.queryNodes('DATASET', replace(term,\"'\",'\\\'')) yield node return node
                            union with row with row, custom.cleanText(row.term) as term
                            call db.index.fulltext.queryNodes('DATASET',replace(term,\"'\",'\\\'') + '~') yield node return node}
                            with row, node as a, node.CMName as matching, apoc.text.levenshteinDistance(custom.cleanText(node.CMName), custom.cleanText(row.term)) as score
                        """
            else:
                qStart = """
                            with row call { with row with row, custom.cleanText(row.term) as term
                            call db.index.fulltext.queryRelationships('CATEGORY', replace(term,\"'\",'\\\'')) yield relationship return relationship
                            union with row with row, custom.cleanText(row.term) as term
                            call db.index.fulltext.queryRelationships('CATEGORY',replace(term,\"'\",'\\\'') + '~') yield relationship return relationship}
                            match (a)<-[relationship]-()
                            call {with row, relationship with row, custom.anytoList(collect(relationship.Name)) as namelist
                            return custom.matchingDist(apoc.coll.flatten([namelist],true),row.term) as matching
                            }
                            with row, a, matching.matching as matching, matching.score as score
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
                        where (a)<-[:USES]-(:DATASET{CMID: row.dataset})
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
        qReturn = """
        return distinct row.term as term, a.CMID as CMID, a.CMName as CMName, [i in labels(a) where not i = 'CATEGORY'] as label, 
        matching, score as matchingDistance, country order by matchingDistance
        """
        cypher_query = qLoad + qStart + qDomain + qContext + qDataset + qYear + qLimit + qCountry + qReturn
        if(query == "true"):
            cypher_queryQ = qLoad + qStart + qDomain + qContext + qDataset + qYear + qLimit + qCountry + qReturn
            cypher_query = qLoad + " return row"
            # Execute the Cypher queries
        with driver.session() as session:
            result = session.run(cypher_query, rows = rows)
        
            # Process the query results and generate the dynamic JSON
            if(query == "true"):
                data = [cypher_queryQ.replace("\n"," "),result.value()]
            else:
                data = [dict(record) for record in result]

            driver.close()
                
        rowsType = type(rows)
        return [rowsType,jsonify(data)]
    
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        error_message = str(e)  # Convert the exception to a string
        data = {"error": error_message}

        return jsonify(data), 500

@app.route('/test', methods=['GET','POST'])
def getTest():
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
        if query is None:
            query = 'false'

        domain = ast.literal_eval(domain)
        dom = domain[0]
        yearStart = ast.literal_eval(yearStart)
        yearEnd = ast.literal_eval(yearEnd)
        context = ast.literal_eval(context)
        rows = []
        for term_item, domain_item, context_item, yearStart_item, yearEnd_item in zip(term, domain, context, yearStart,yearEnd):
            rows.append({"term": term_item, "domain": domain_item, "context": context_item, "yearStart": yearStart_item, "yearEnd": yearEnd_item})
        if database == "SocioMap":
            driver = connection()
        elif database == "ArchaMap":
            driver = connectionAM()
        return(jsonify(rows))
    if request.method == 'POST':
        data = request.get_data()  
        data = json.loads(data)
        return(data)
         

if __name__== "__main__":
    app.run(debug=True,port=5001)




