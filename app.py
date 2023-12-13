#from fastapi import FastAPI
from flask import Flask,request
from flask import jsonify
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from flask_cors import CORS
from fuzzywuzzy import fuzz
import json
import re
import string

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
    driver = GraphDatabase.driver(uri=uri,auth=(user,pwd))
    return driver

def connectionGIS():
    driver = GraphDatabase.driver(uri=uri1,auth=(user1,pwd1))
    return driver

def connectionAM():
    driver = GraphDatabase.driver(uri=uriAM,auth=(user,pwdAM))
    return driver

def verifyUser(driver,user,pwd):
    with driver.session() as session:
        query = "match (u:USER {userID: $user,password: $pwd}) return true as verified"
        result = session.run(query, user = user, pwd = pwd)
        result = [dict(record) for record in result]
        driver.close()
    return result

def getPolygon(CMID,driver, simple = True):
    try:
        with driver.session() as session:
            query = """
    match (:CATEGORY {CMID: $CMID})<-[r:USES]-(d:DATASET) where not r.geoPolygon is null 
    return distinct r.geoPolygon as geomID, d.shortName as source
    """
            results = session.run(query, CMID = CMID)
            result = [dict(record) for record in results]
            driver.close()
        driverGIS = connectionGIS() 
        with driverGIS.session() as session:
            if simple == True:
                query = """
    unwind $rows as row 
    unwind row.geomID as geomID
    unwind row.source as source
    with geomID, source
    match (g:GEOMETRY)
    where g.geomID = geomID
    return source, coalesce(g.simplified,g.geometry) as geometry, g.simplified is not null as simple
    """         
            else:
                query = """
    unwind $rows as row 
    unwind row.geomID as geomID
    unwind row.source as source
    with geomID, source
    match (g:GEOMETRY) 
    where g.geomID = geomID
    return source, g.geometry as geometry
    """         
            # query = "unwind $rows as row return row"
            polygons = session.run(query, rows = result)
            polygons = [dict(record) for record in polygons]
            driverGIS.close()
        return polygons
    except Exception as e:
        return {"firstResult":result,"query":query,"error":str(e)}
    
def getPoints(CMID,driver):
    with driver.session() as session:
        query = "match (:CATEGORY {CMID: $CMID})<-[r:USES]-(d:DATASET) where not r.geoCoords is null return distinct r.geoCoords as geometry, d.shortName as source"
        result = session.run(query, CMID = CMID)
        points = [dict(record) for record in result]
        driver.close()
    return points

def getRelations(CMID,driver):
    with driver.session() as session:
        query = "match ({CMID: $CMID})-[r]-() return distinct type(r) as relation"
        result = session.run(query, CMID = CMID)
        for record in result:
            relations = record["relation"]
        driver.close()
    return relations

def flatten_json(json_obj, parent_key='', sep='_'):
    flat_dict = {}
    for key, value in json_obj.items():
        new_key = f"{key}" if parent_key else key
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        else:
            flat_dict[new_key] = value
    return flat_dict

#app=FastAPI()
app = Flask(__name__)
CORS(app)
app.config['CORS_HEADERS']='Content-Type'
app.config['PERMANENT_SESSION_LIFETIME'] = 999999999
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

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
        
        
socioid=[""]
@app.route("/category",methods=['GET'])
def catm():
        center = 0
        relnames= []
        relations = ["USES","CONTAINS","DISTRICT_OF","LANGUOID_OF","RELIGION_OF"]
        socioid[0] = request.args.get('value')
        driver_neo4j = connectionSM()
        session = driver_neo4j.session()
        driver_neo4j1 =connectionGIS()
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
                    q1 = '''match (g:GEOMETRY) where g.geomID = "'''+relid+'''" return g.geometry'''
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
                    q1 = '''match (g:GEOMETRY) where g.geomID = "'''+relid+'''" return g.geometry'''
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
        
        #print(payload)
        return jsonify(payload)
        #return (results.data())

@app.route("/network",methods=['GET'])
def net():
    p0 = request.args.get('value')
    p1 = request.args.get('id')
    p2 = request.args.get('relation')
    driver_neo4j =connectionSM()
    session = driver_neo4j.session()
    def get_properties(self):
        return self._properties
    def get_properties(self):
        return self._properties
    q = "MATCH (n:"+p0+" {CMID:'"+p1+"'})-[r:"+p2+"]-(OtherNodes) RETURN n,r,OtherNodes"
    r = session.run(q)
    print(r)
    resultnet = r.data()

    for i in resultnet:
        pass

    return resultnet


@app.route("/explore",methods=['GET'])
def getExplore():
    
    cmid = request.args.get('cmid')
    database = request.args.get('database')

    if database == "SocioMap":
        driver = connectionSM()
        label = re.search("^SM",cmid)
    elif database == "ArchaMap":
        driver = connectionAM()
        label = re.search("^AM",cmid)
    else:
        pass

    if label is not None:
        label = "CATEGORY"
    else: 
        label = "DATASET"

    if label == "CATEGORY":
        qInfo = '''
unwind $cmid as cmid match (a)<-[r:USES]-(d:DATASET)
where a.CMID = cmid with a,r,d
call apoc.when(r.country is not null,'return custom.getName($id) as name','return null as name',{id:r.country}) yield value as country
call apoc.when(r.language is not null,'return custom.getGlot($id) as name','return null as name',{id:r.language}) yield value as language
call apoc.when(r.religion is not null,'return custom.getName($id) as name','return null as name',{id:r.religion}) yield value as religion
with a,r,d, country, language, religion,
case when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is not null then custom.getMinYear(r.yearStart) + '-' + custom.getMaxYear(r.yearEnd)
when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is null then custom.getMinYear(r.yearStart) + '-present'
when custom.getMinYear(r.yearStart) is null and custom.getMaxYear(r.yearEnd) is not null then custom.getMaxYear(r.yearEnd)
else null
end as timeSpan
return a.CMName as CMName, custom.anytoList(collect(split(country.name,', ')),true) as Location, 
a.CMID as CMID, apoc.text.join([i in labels(a) where not i = 'CATEGORY'],', ') as Labels, 
custom.anytoList(collect(split(language.name,', ')),true) as Languages, custom.anytoList(collect(split(religion.name,', ')),true) as Religions, 
custom.anytoList(collect(split(timeSpan,', ')),true) as `Date range`
'''        
        qSamples = ''' 
unwind $cmid as cmid
match (a)<-[r:USES]-(d:DATASET)
where a.CMID = cmid
with custom.anytoList(collect(r.Name),true) as Name, r.country as LocationID, d.project as Source, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd, 
toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
call apoc.when(LocationID is not null,'return custom.getName($id) as Location','return null',{id:LocationID}) yield value
return Name, custom.anytoList(collect(value.Location),true) as Location, type as Type, 
apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),
toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),
toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Population est.`,  
apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`, Source, Version, Link order by `Time span`, Source, Name
'''
        with driver.session() as session:
            samples = session.run(qSamples, cmid = cmid)
            samples = [dict(record) for record in samples]
            driver.close()
    else:
        qInfo = '''
unwind $cmid as cmid 
match (a:DATASET) 
where a.CMID = cmid 
with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
'return null as name',{id:a.District}) yield value as Location 
return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID, 
labels(a) as Labels, a.parent as Dataset, a.DatasetCitation as Citation, a.DatasetLocation as `Dataset Location`, a.Note as Note
'''
        samples = []
    
    with driver.session() as session:
        info = session.run(qInfo, cmid = cmid)
        info = [dict(record) for record in info]
        driver.close()

    polygons = getPolygon(cmid,driver)
    points = getPoints(cmid,driver)

    return jsonify({
        "info": info,
        "samples": samples,
        "polygons": polygons,
        "points": points
    })
    

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

@app.route('/networks', methods=['GET'])
def getNetwork():
    try:
        cmid = request.args.get('cmid')
        cmid = re.split(",",cmid)
        domain = request.args.get('domain')
        if domain is not None:
            domain = re.split(",",domain)
        else:
            domain = ["CATEGORY","DATASET"]

        endcmid = request.args.get('endcmid')
        relation = request.args.get('relation')
        if relation is None:
            relation = "USES"
        database = request.args.get('database')

        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        else:
            raise Exception("must specify database as SocioMap or ArchaMap")

        if endcmid is not None:
            cypher_query = """
unwind $cmid as cmid unwind $endcmid as endcmid unwind $relation as relation 
MATCH (a) 
WHERE a.CMID = cmid
optional match (a)-[r]-(e) 
where type(r) = relation and e.CMID = endcmid and
not isEmpty([label IN labels(e) 
WHERE label IN apoc.coll.flatten([$domain],true)]) 
return distinct a,r,e limit 10
"""        
        else:
            cypher_query = """
unwind $cmid as cmid unwind $relation as relation MATCH (a) 
WHERE a.CMID = cmid 
optional match (a)-[r]-(e) 
where type(r) = relation and
not isEmpty([label IN labels(e) 
WHERE label IN apoc.coll.flatten([$domain],true)]) 
return distinct a,r,e limit 10
"""        
        
        with driver.session() as session:
            # Execute the Cypher queries
            result = session.run(cypher_query, cmid = cmid, relation = relation,domain = domain, endcmid = endcmid)
            node = []
            rel = []
            end = []
            for record in result:
                a = record['a']
                node.append({"node":serialize_node(a)})
                r = record['r']
                e = record['e']
                r = serialize_relationship(r)
                e = serialize_node(e)
                rel.append({"relation":r})
                end.append({"end":e})

        driver.close()
        node = [flatten_json(entry) for entry in node]
        rel = [flatten_json(entry) for entry in rel]
        end = [flatten_json(entry) for entry in end]

        return {"node":node,"relations":rel,"relNodes":end,"query":cypher_query,"params":[{"cmid":cmid,"database":database,"domain":domain,"relation":relation,"endcmid":endcmid}]}
    except Exception as e:
        return str(e), 500
    
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

        # need to add check to mak sure property is valid and domain is valid

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
with endnode(relationship) as a, relationship.Key as matching, case when $term contains ":" then $term else ": " + $term end as term
where '{domain}' in labels(a) and matching ends with term
with a, matching, 0 as score
"""
        elif property == "Name":
            if domain != "DATASET":
                qStart = f"""
call {{ with custom.cleanText($term) as term
call db.index.fulltext.queryNodes('{domain}', replace(term,"'","\\'")) yield node return node
union with custom.cleanText($term) as term
call db.index.fulltext.queryNodes('{domain}',replace(term,"'","\\'") + '~') yield node return node}}
with node as a
with a, custom.matchingDist(a.names, $term) as matching
with a, matching.matching as matching, toInteger(matching.score) as score
"""
            else:
                qStart = f"""
call {{ with custom.cleanText($term) as term
call db.index.fulltext.queryNodes('{domain}', replace(term,"'","\\'")) yield node return node
union with custom.cleanText($term) as term
call db.index.fulltext.queryNodes('{domain}',replace(term,"'","\\'") + '~') yield node return node}}
with node as a
with a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], $term) as matching
with a, matching.matching as matching, toInteger(matching.score) as score
"""
        else:
            qStart = f"""
match (a) where tolower(a.{property}) = tolower($term)
with a, a.{property} as matching, 0 as score
"""

        # filter by domain

        qDomain = f" where '{domain}' in labels(a) "

        qUnique = """
with a, collect(matching) as matchingL, 
collect(score) as scores call {with matchingL, 
scores unwind matchingL as matching 
unwind scores as score return distinct matching, score order by score limit 1}
with a, matching, score
"""



            # filter by context
        if context is not None:
            qContext = """
where (a)<--({CMID: $context})
with a, matching, score
"""
        else:
            context = ""
            qContext = " "

            # filter by year
        if yearStart is not None:
            if domain == "DATASET":
                qYear = f"""
call {{with a with a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
else a.ApplicableYears end as yearMatch, range(toInteger('{yearStart}'),toInteger('{yearEnd}')) as years
with a, years, apoc.convert.toIntList(apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}}
with node as a, matching, score
"""   
            else:
                qYear = f"""
call {{ with a with a, range(toInteger('{yearStart}'),toInteger('{yearEnd}')) as inputYears 
match (a)<-[r:USES]-(:DATASET) with a, inputYears, range(apoc.coll.min([i in apoc.coll.flatten(collect(r.yearStart),true) | 
toInteger(i)]), apoc.coll.max([i in apoc.coll.flatten(collect(r.yearEnd),true) | 
toInteger(i)])) as years where not isEmpty([i in inputYears where i in years]) return a as node}}
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
            return({"query":cypher_query,"term": term,"context":context})
    except Exception as e:
        return str(e), 500

@app.route('/translate', methods=['POST'])
def getTranslate():
    try:
        data = request.get_data()  
        data = json.loads(data)
        database = data.get("database")[0]
        context = data.get("context")
        if context == {}:
            context = [""]
        dataset = data.get("dataset")
        if dataset == {}:
            dataset = [""]
        property = data.get("property")[0]
        domain = data.get("domain")
        dom = domain[0]
        yearStart = data.get("yearStart")
        if yearStart == {}:
            yearStart = [""]
        query = data.get("query")[0]
        if query == {}:
            query = "false"
        rows = data.get("rows")

        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")
        
        # Define the Cypher query
    
        qLoad = "unwind $rows as row with row call {"

        if property == "Key":
            qStart = f"""
with row call db.index.fulltext.queryRelationships('keys',replace(row.term,':','\\:')) yield relationship
with row, endnode(relationship) as a, relationship.Key as matching, case when row.term contains ":" then row.term else ": " + row.term end as term
where '{dom}' in labels(a) and matching ends with term
with row, a, matching, 0 as score
"""
        elif property == "Name":
    
            if dom != "DATASET":
                qStart = f"""
with row call {{ with row with row, custom.cleanText(row.term) as term
call db.index.fulltext.queryNodes('{dom}', replace(term,"'","\\'")) yield node return node
union with row with row, custom.cleanText(row.term) as term
call db.index.fulltext.queryNodes('{dom}',replace(term,"'","\\'") + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist(a.names, row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""
            else:
                qStart = f"""
with row call {{ with row with row, custom.cleanText(row.term) as term
call db.index.fulltext.queryNodes('{dom}', replace(term,"'","\\'")) yield node return node
union with row with row, custom.cleanText(row.term) as term
call db.index.fulltext.queryNodes('{dom}',replace(term,"'","\\'") + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""
        else:
            qStart = f""" 
with row call apoc.cypher.run('match (a:{dom}) where not a.{property} is null and tolower(a.{property}) = tolower(\"' + row.term + '\") return a, a.{property} as matching',{{}}) yield value 
with row, value.a as a, value.matching as matching, 0 as score
"""

    # filter by domain

        qDomain = f" where '{dom}' in labels(a) with row, a, matching, score "

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
        qReturn = """
return distinct row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, [i in labels(a) where not i = 'CATEGORY'] as label, 
matching, score as matchingDistance, country order by matchingDistance
"""
        cypher_query = qLoad + qStart + qDomain + qContext + qDataset + qYear + qLimit + qCountry + qReturn
        if query == "true":
            with driver.session() as session:
                result = session.run("unwind $rows as rows unwind rows as row return row.term as term, row.CMuniqueRowID as CMuniqueRowID", rows = rows)
                qResult = [dict(record) for record in result]
            return [{"query": cypher_query.replace("\n"," "),"params":qResult}]
        else:
        # Execute the Cypher queries
            with driver.session() as session:
                result = session.run(cypher_query, rows = rows)
        
            # Process the query results and generate the dynamic JSON
                data = [dict(record) for record in result]

                driver.close()
                
        return data

    except Exception as e:
        return str(e), 500

@app.route('/query', methods=['POST'])
def getQuery():
    try:
        rows = request.get_data()  
        rows = json.loads(rows)
        database = rows.get("database")
        query = rows.get("query")
        user = rows.get("user")
        pwd = rows.get("pwd")
        params = rows.get("params")
        
        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        
        try:
            verified = verifyUser(driver,user,pwd)
            for item in verified:
                verified = item

            for item in verified:
                verified = item
            
        except Exception as e:
            return str(e), 500
    
        if verified == "verified":
            with driver.session() as session:
                result = session.run(query,params)
                data = [dict(record) for record in result]
                driver.close()
            return jsonify(data)
        else:
            data = {"error": "User is not verified","verified": verified}
            return jsonify(data), 500

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        data = str(e)

        return data, 500


@app.route('/geometry', methods=['GET'])
def getGeometry():
    database = request.args.get('database')
    cmid = request.args.get('cmid')
    simple = request.args.get('simple')
    if simple is None:
        simple = True
    if database == "SocioMap":
        driver = connectionSM()
    elif database == "ArchaMap":
        driver = connectionAM()
    elif database == "gisdb":
        driver = connectionGIS()

    polygons = getPolygon(cmid,driver,simple = True)
    points = getPoints(cmid,driver)
    return jsonify({"polygons":polygons,"points":points})

@app.route('/newuser', methods=['POST'])
def getnewuser():
    try:
        data = request.get_data()  
        data = json.loads(data)
        database = data.get("database")
        firstName = data.get("firstName")
        lastName = data.get("lastName")
        email = data.get("email")
        username = data.get("username")
        password = data.get("password")
        
        if database == "SocioMap":
            driver = connectionSM()
        elif database == "ArchaMap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")   
        
        queryExists = """
match (u:USER {username: $username}) 
return true as exists
"""
        with driver.session() as session:
            result = session.run(queryExists, username = username)
            data = [dict(record) for record in result]
            driver.close()
        
        if isinstance(data, list) and data and data[0].get("exists") is not None:
            raise Exception("Username already exists. Please try another username.")
        
        queryExists = """
match (u:USER {Email: $email}) 
return true as exists
"""
        with driver.session() as session:
            result = session.run(queryExists, email = email)
            data = [dict(record) for record in result]
            driver.close()
        
        if isinstance(data, list) and data and data[0].get("exists") is not None:
            raise Exception("Account with this email already exists. Please contact admin@catmapper.org to reset password.")
        
        query = """
match (p:USER) with toInteger(p.userID) + 1 as id order by id desc limit 1
merge (u:USER {username: $username}) 
on create set u.username = $username,
u.First = $firstName,
u.Last = $lastName,
u.Email = $email,
u.access = "new",
u.log = toString(datetime()) + ": created user via API",
u.password = $password,
u.userID = toString(id),
u.role = 'user'
return u.userID as userID
"""
        with driver.session() as session:
            result = session.run(query,firstName = firstName, lastName = lastName, email = email, password = password,username = username)
            data = [dict(record) for record in result]
            driver.close()
        return jsonify(data)


    except Exception as e:
        # Check for specific error messages
        error_message = str(e)
        
        if "Account with this email already exists." in error_message:
            return f"Error: {error_message}", 400  # Return 400 Bad Request

        elif "Username already exists" in error_message:
            return f"Error: {error_message}", 400  # Return 400 Bad Request

        else:
            # Default error message
            return f"Error: please contact admin@catmapper.org. Error: {error_message}", 500 # Return 400 Bad Request


@app.route('/test', methods=['POST'])
def getTest():
    rows = request.get_data()  
    rows = json.loads(rows)
    cypher_query = "unwind $rows as row return row.term as term"
    driver = connectionSM()
        
    with driver.session() as session:
        # Execute the Cypher queries
        result = session.run(cypher_query, rows = rows["rows"])
        node = [dict(record) for record in result]
        driver.close()

    return {"row":node}

if __name__== "__main__":
    app.run(debug=True,port=5001)



