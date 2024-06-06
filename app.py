#from fastapi import FastAPI
from flask import Flask,request
from flask import jsonify, render_template, make_response
from neo4j import GraphDatabase
import os
from dotenv import load_dotenv
from flask_cors import CORS
from fuzzywuzzy import fuzz
from bs4 import BeautifulSoup
import json
import re
import string
from flasgger import Swagger, LazyString, LazyJSONEncoder
import CM
import pysodium
import pandas as pd
import numpy as np
from collections import defaultdict
import logging
# from werkzeug.middleware.proxy_fix import ProxyFix

load_dotenv()
uri = os.getenv("uri")
user = os.getenv("user")
pwd = os.getenv("pwd")
uri1 = os.getenv("uri1")
user1 = os.getenv("user1")
pwd1 = os.getenv("pwd1")
uriAM = os.getenv("uriAM")
pwdAM = os.getenv("pwdAM")
apikeyEnv = os.getenv("apikey")

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
        new_key = key if parent_key else key
        if isinstance(value, dict):
            flat_dict.update(flatten_json(value, new_key, sep=sep))
        else:
            flat_dict[new_key] = value
    return flat_dict

def custom_sort(elem):
    if elem == 'CONTAINS':
        return 0
    elif elem == 'DISTRICT_OF':
        return 1
    elif elem == 'USES':
        return 2
    else:
        return 3


#app=FastAPI()
app = Flask(__name__)

CORS(app)
app.config['CORS_HEADERS']='Content-Type'
app.config['PERMANENT_SESSION_LIFETIME'] = 999999999
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

# swagger documentation
# app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_host=1)
app.json_encoder = LazyJSONEncoder

template = dict(swaggerUiPrefix="/api")
swagger = Swagger(app, template=template)

@app.route("/")
def root ():
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('api.html'),200,headers)

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
        
        cmid = request.args.get('cmid')
        database = request.args.get('database')

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            pass

        relnames= []
        relations = ["USES","CONTAINS","DISTRICT_OF","LANGUOID_OF","RELIGION_OF"]
        q = "match (a) where a.CMID = '"+cmid+"' return id(a) as id,labels(a) as label"
        session = driver.session()
        labels = session.run(q)
        labels = labels.data()
        if labels:
            labels = str(labels[0]['label'][-1])
        else:
            labels = ""
        q = "MATCH (n:"+labels+" {CMID:'"+cmid+"'})-[r]-(n1) RETURN DISTINCT TYPE(r) as label"
        rel_name = session.run(q).data()
        for i in rel_name:
            if i['label'] in relations:
                relnames.append(i['label'])
        driver.close()

        if str.lower(database) == "sociomap":
            driver = connectionSM()
            label = re.search("^SM",cmid)
        elif str.lower(database) == "archamap":
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
    call apoc.when(r.country is not null and not r.country = [],'return custom.getName($id) as name','return null as name',{id:r.country}) yield value as country
    call apoc.when(r.district is not null and not r.district = [],'return custom.getName($id) as name','return null as name',{id:r.district}) yield value as district
    call apoc.when(r.language is not null and not r.language = [],'return custom.getGlot($id) as name','return null as name',{id:r.language}) yield value as language
    call apoc.when(r.religion is not null and not r.religion = [],'return custom.getName($id) as name','return null as name',{id:r.religion}) yield value as religion
    with a,r,d, country, district, language, religion,
    case when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is not null then custom.getMinYear(r.yearStart) + '-' + custom.getMaxYear(r.yearEnd)
    when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is null then custom.getMinYear(r.yearStart) + '-present'
    when custom.getMinYear(r.yearStart) is null and custom.getMaxYear(r.yearEnd) is not null then custom.getMaxYear(r.yearEnd)
    else null
    end as timeSpan
    return a.CMName as CMName, apoc.text.join([i in [custom.anytoList(collect(split(country.name,', ')),true),custom.anytoList(collect(split(district.name,', ')),true)] where not i = ''],', ') as Location, 
    a.CMID as CMID, apoc.text.join([i in labels(a) where not i = 'CATEGORY'],', ') as Domains, 
    custom.anytoList(collect(split(language.name,', ')),true) as Languages, custom.anytoList(collect(split(religion.name,', ')),true) as Religions, 
    custom.anytoList(collect(split(timeSpan,', ')),true) as `Date range`
    '''        
            qSamples = ''' 
    unwind $cmid as cmid
    match (a)<-[r:USES]-(d:DATASET)
    where a.CMID = cmid
    with custom.anytoList(collect(r.Name),true) as Name, r.country as countryID,
    r.district as districtID, d.project as Source, d.CMID as datasetID, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd, 
    toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
    call apoc.when(countryID is not null,'return custom.getName($id) as country','return null',{id:countryID}) yield value
    with Name, value as country, districtID, Source, datasetID, Version, Link, recordStart, recordEnd, Population, `Sample size`, type
    call apoc.when(districtID is not null,'return custom.getName($id) as district','return null',{id:districtID}) yield value
    with Name, country, value as district, Source, datasetID, Version, Link, recordStart, recordEnd, Population, `Sample size`, type
    return Name, apoc.text.join([i in [custom.anytoList(collect(country.country),true),custom.anytoList(collect(district.district),true)] where not i = ''],', ') as Location, type as Type, 
    apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),
    toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),
    toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Population est.`,  
    apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`,Source as `Source`, 'https://catmapper.org/js/' + $database + '/' + datasetID  as `link2`,
    Version, Link order by `Time span`, Source, Name
    '''
            qCategories = """
unwind $cmid as cmid 
match (a:ADM0 {CMID: cmid})-[:DISTRICT_OF]-(c:CATEGORY) 
unwind labels(c) as Domain with Domain, count(*) as Count 
return distinct Domain, Count order by Domain
"""

            
            # with driver.session() as session:
            #     samples = session.run(qSamples, cmid = cmid)
            #     samples = [dict(record) for record in samples]
            #     driver.close()
        
        else:
             qInfo = '''
    unwind $cmid as cmid 
    match (a:DATASET) 
    where a.CMID = cmid 
    with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
    'return null as name',{id:a.District}) yield value as Location 
    return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID, 
    labels(a) as Domains, a.parent as Parent, a.DatasetCitation as Citation, "<a href ='" + a.DatasetLocation + "' target='_blank' >" + a.DatasetLocation +"</a>" as `Dataset Location`, a.ApplicableYears as `Applicable Years`, a.Note as Note
    '''
             qSamples = None
        
             qCategories = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY) 
unwind r.label as Domain with Domain, count(*) as Count 
return distinct Domain, Count order by Domain
"""

#             qInfo = '''
# unwind $cmid as cmid 
# match (a:DATASET) 
# where a.CMID = cmid 
# with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
# 'return null as name',{id:a.District}) yield value as Location 
# return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID, 
# labels(a) as Domains, a.parent as Parent, a.DatasetCitation as Citation, a.DatasetLocation as `Dataset Location`, a.ApplicableYears as `Applicable Years`, a.Note as Note
# '''
        #      samples = []
        
        # with driver.session() as session:
        #     info = session.run(qInfo, cmid = cmid)
        #     info = [dict(record) for record in info]
        #     driver.close()

        with driver.session() as session:
            info = session.run(qInfo, cmid = cmid)
            info = [dict(record) for record in info]
            if qCategories is None:
                categories = []
            else: 
                categories = session.run(qCategories,cmid = cmid)
                categories = [dict(record) for record in categories]
            if qSamples is not None:
                samples = session.run(qSamples, cmid = cmid,database = database)
                samples = [dict(record) for record in samples]
            else: 
                samples = []
            driver.close()
        
        if "Dataset Location" in info[0]:
            soup = BeautifulSoup(info[0]["Dataset Location"], 'html.parser')
            link_tag = soup.find('a')
            if link_tag:
                info[0]["Dataset Location"] = link_tag.get('href')  
              
        
        polygons = getPolygon(cmid,driver)
        points = getPoints(cmid,driver)

        with open('poly.json', 'w', encoding='utf-8') as f:
            json.dump(polygons, f, ensure_ascii=False, indent=4)
        
        polysources = []
        
        if len(polygons) != 0:
            # polygons != "" or polygons != [] or 
            if len(polygons) > 1:
                poly = {"type": 'FeatureCollection',"features": []}
                for i in range(0,len(polygons)):
                    poly["features"].append(json.loads(polygons[i]['geometry']))
                    poly["features"][i]["source"] = (polygons[i]['source'])
                    polysources.append(polygons[i]['source'])
                polygons = poly
                # polygons = json.loads(polygons)
            else:
                temp = polygons
                polygons = [json.loads(polygons[0]['geometry'])]
                polygons[0]["source"] = (temp[0]['source'])
                polysources.append(temp[0]['source'])
                temp = None

        with open('new.json', 'w', encoding='utf-8') as f:
            json.dump(polygons, f, ensure_ascii=False, indent=4)
        
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(points, f, ensure_ascii=False, indent=4)
                
        if len(points) > 0:
            point= []
            for i in range(0,len(points)):
                if json.loads(points[i]['geometry'])["type"] != "MultiPoint":
                    point.append({"cood" : json.loads(points[i]['geometry'])["coordinates"][::-1],"source": points[i]["source"]})
                else:
                    temp = points[i]
                    source = temp['source']
                    for j in range(0,len(json.loads(temp['geometry'])['coordinates'])):
                        point.append({'cood' : json.loads(temp['geometry'])['coordinates'][j][::-1], "source" : source })
            if point:
                    points= point
               

        # if len(points) > 0:
        #     for i in range(0,len(points)):
        #         if isinstance(json.loads(points[i]['geometry'])["coordinates"][0],int):
        #             points[i] = {"cood" : json.loads(points[i]['geometry'])["coordinates"][::-1],"source": points[i]["source"]}
        #         elif isinstance(json.loads(points[i]['geometry'])["coordinates"][0],float):
        #             points[i] = {"cood" : json.loads(points[i]['geometry'])["coordinates"][::-1],"source": points[i]["source"]}
        #         else:
        #             for j in range(0,len(json.loads(points[i]['geometry'])["coordinates"])):
        #                 print(points[i])

        with open('data.json', 'w', encoding='utf-8') as f:
                json.dump(points, f, ensure_ascii=False, indent=4)
        
        relnames = sorted(relnames, key=custom_sort)

        print(points)
                                
        return jsonify({
        "info": info[0],
        "samples": samples,
        "categories": categories,
        "polygons": polygons,
        "points": points,
        "label":labels,
        "relnames": relnames,
        "polysource": polysources
    })

#         center = 0
#         relnames= []
#         relations = ["USES","CONTAINS","DISTRICT_OF","LANGUOID_OF","RELIGION_OF"]
#         socioid[0] = request.args.get('cmid')
#         driver_neo4j = connectionSM()
#         session = driver_neo4j.session()
#         driver_neo4j1 =connectionGIS()
#         session1 = driver_neo4j1.session()
#         q = "match (a) where a.CMID = '"+socioid[0]+"' return id(a) as id,labels(a) as label"
#         r = session.run(q)
#         r = str(r.data()[0]['id'])
#         label = session.run(q)
#         label = str(label.data()[0]['label'][-1])
#         q = "MATCH (n:"+label+" {CMID:'"+socioid[0]+"'})-[r]-(n1) RETURN DISTINCT TYPE(r) as label"
#         rel_name = session.run(q).data()
#         for i in rel_name:
#             if i['label'] in relations:
#                 relnames.append(i['label'])
#         q =   ''' match (a)<-[r:USES]-(d:DATASET)
# where id(a) = '''+r+'''
# with custom.anytoList(collect(r.Name),true) as Name, r.country as LocationID, d.project as Source, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd, toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
# call apoc.when(LocationID is not null,'return custom.getName($id) as Location','return null',{id:LocationID}) yield value
# return Name, custom.anytoList(collect(value.Location),true) as Location, type as Type, apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Populationest`,  apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`, Source, Version, Link order by `Time span`, Source, Name'''
#         results = session.run(q)
#         q1 = '''match (a)<-[r:USES]-(d:DATASET) where id(a) = '''+r+''' and (r.geoCoords is not null or r.geoPolygon is not null) return r.geoCoords as point, r.geoPolygon as polygon, d.shortName as source, r.Key as Key'''
#         results1 = session.run(q1)
#         resultsm = results1.data()
#         flag = 0
#         for i in range(0,len(resultsm)):
#             if resultsm[i]['polygon'] is not None:
#                 print("...................................")
#                 flag =1
#                 relid = resultsm[i]['polygon']
#                 if isinstance(relid,list):
#                     relid = str(relid[0])
#                     q1 = '''match (g:GEOMETRY) where g.geomID = "'''+relid+'''" return g.geometry'''
#                     results1=''
#                     results1 = session1.run(q1)
#                     results1 = results1.data()
#                     results1 = (results1[0]['g.geometry']).replace("u\'","\'")
#                     results1 = json.loads(results1)
#                     with open('data.json', 'w', encoding='utf-8') as f:
#                          json.dump(results1, f, ensure_ascii=False, indent=4)
#                     if results1['type'] == "Polygon":
#                         center = (results1['coordinates'][0][0])[::-1]
#                     if results1['type'] == "MultiPolygon":
#                         center = (results1['coordinates'][0][0][0])[::-1]
#                 else:
#                     relid = str(relid)
#                     q1 = '''match (g:GEOMETRY) where g.geomID = "'''+relid+'''" return g.geometry'''
#                     results1=''
#                     results1 = session1.run(q1)
#                     results1 = results1.data()
#                     results1 = (results1[0]['g.geometry']).replace("u\'","\'")
#                     if "features" in results1:
#                         results1 = json.loads(results1)
#                         results1 = results1['features'][0]
#                         if results1['geometry']['type'] == "Polygon":
#                             center = (results1['geometry']['coordinates'][0][0])[::-1]
#                         if results1['geometry']['type'] == "MultiPolygon":
#                             center = (results1['geometry']['coordinates'][0][0][0])[::-1]
#                     else:
#                         results1 = json.loads(results1)
#                         if results1['type'] == "Polygon":
#                             center = (results1['coordinates'][0][0])[::-1]
#                         if results1['type'] == "MultiPolygon":

#                             center = (results1['coordinates'][0][0][0])[::-1]
#                     with open('data.json', 'w', encoding='utf-8') as f:
#                          json.dump(results1, f, ensure_ascii=False, indent=4)
                    
#                 break
        
#         if flag == 0:
#             results1=[]

        
#         poid=[]
#         for i in range(0,len(resultsm)):
#             if resultsm[i]['point'] is not None:
#                 #poid[resultsm[i]['source']] = json.loads(resultsm[i]['point'])['coordinates'][0]
#                 print((resultsm[i]['point']))
#                 if isinstance(resultsm[i]['point'], list):
#                     cood=(json.loads(resultsm[i]['point'][0])['coordinates'])
#                 else:
#                     cood=(json.loads(resultsm[i]['point'])['coordinates'])
#                 if isinstance(cood, list):
#                     cood = cood[::-1]
#                 print(cood)
#                 poid.append(dict([("id",resultsm[i]['source']),('coordinates',cood)]))
#                 #poid[i]['id'] = resultsm[i]['source']
#                 #poid[i]['coordinates'] = json.loads(resultsm[i]['point'])['coordinates'][0]
        
#         print(poid)

#         '''
#         for obj in results1:
#              if "coordinates" in obj:
#                  northing = obj["coordinates"][0]
#                  easting = obj["coordinates"][1]
#                  obj["coordinates"] = [ easting, northing ]
#         '''
    
        
#         payload = {
#     "current_response": results.data(),
#     "future_response": results1,
#     "center": center,
#     "poid": poid,
#     "label":label,
#     "relnames": relnames
# }
        
#         #print(payload)
#         return jsonify(payload)
#         #return (results.data())

@app.route("/network",methods=['GET'])
def net():
    p0 = request.args.get('value')
    p1 = request.args.get('cmid')
    p2 = request.args.get('relation')
    driver_neo4j =connectionSM()
    session = driver_neo4j.session()
    q = "MATCH (n:"+p0+" {CMID:'"+p1+"'})-[r:"+p2+"]-(OtherNodes) RETURN n,r,OtherNodes"
    r = session.run(q)
    resultnet = r.data()
    return resultnet


@app.route("/explore",methods=['GET'])
def getExplore():
    
    try:
        cmid = request.args.get('cmid')
        database = request.args.get('database')

        if str.lower(database) == "sociomap":
            driver = connectionSM()
            label = re.search("^SM",cmid)
        elif str.lower(database) == "archamap":
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
    call apoc.when(r.country is not null and not r.country = [],'return custom.getName($id) as name','return null as name',{id:r.country}) yield value as country
    call apoc.when(r.district is not null and not r.district = [],'return custom.getName($id) as name','return null as name',{id:r.district}) yield value as district
    call apoc.when(r.language is not null and not r.language = [],'return custom.getGlot($id) as name','return null as name',{id:r.language}) yield value as language
    call apoc.when(r.religion is not null and not r.religion = [],'return custom.getName($id) as name','return null as name',{id:r.religion}) yield value as religion
    with a,r,d, country, district, language, religion,
    case when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is not null then custom.getMinYear(r.yearStart) + '-' + custom.getMaxYear(r.yearEnd)
    when custom.getMinYear(r.yearStart) is not null and custom.getMaxYear(r.yearEnd) is null then custom.getMinYear(r.yearStart) + '-present'
    when custom.getMinYear(r.yearStart) is null and custom.getMaxYear(r.yearEnd) is not null then custom.getMaxYear(r.yearEnd)
    else null
    end as timeSpan
    return a.CMName as CMName, apoc.text.join([i in [custom.anytoList(collect(split(country.name,', ')),true),
    custom.anytoList(collect(split(district.name,', ')),true)] where not i = ''],', ') as Location, 
    a.CMID as CMID, apoc.text.join([i in labels(a) where not i = 'CATEGORY'],', ') as Domains, 
    custom.anytoList(collect(split(language.name,', ')),true) as Languages, custom.anytoList(collect(split(religion.name,', ')),true) as Religions, 
    custom.anytoList(collect(split(timeSpan,', ')),true) as `Date range`
    '''        
            qSamples = ''' 
    unwind $cmid as cmid
    match (a)<-[r:USES]-(d:DATASET)
    where a.CMID = cmid
    with custom.anytoList(collect(r.Name),true) as Name, r.country as countryID,
    r.district as districtID, d.project as Source, d.CMID as datasetID, d.DatasetVersion as Version, r.url as Link, r.recordStart as recordStart, r.recordEnd as recordEnd, 
    toIntegerList(apoc.coll.flatten(collect(r.populationEstimate))) as Population, toIntegerList(apoc.coll.flatten(collect(r.sampleSize))) as `Sample size`, r.type as type
    call apoc.when(countryID is not null,'return custom.getName($id) as country','return null',{id:countryID}) yield value
    with Name, value as country, districtID, Source, datasetID, Version, Link, recordStart, recordEnd, Population, `Sample size`, type
    call apoc.when(districtID is not null,'return custom.getName($id) as district','return null',{id:districtID}) yield value
    with Name, country, value as district, Source, datasetID, Version, Link, recordStart, recordEnd, Population, `Sample size`, type
    return Name, apoc.text.join([i in [custom.anytoList(collect(country.country),true),custom.anytoList(collect(district.district),true)] where not i = ''],', ') as Location, type as Type, 
    apoc.text.join(apoc.coll.toSet([coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))),
    toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd)))))),coalesce(toString(apoc.coll.min(apoc.coll.toSet(apoc.coll.flatten(collect(recordEnd))))),
    toString(apoc.coll.max(apoc.coll.toSet(apoc.coll.flatten(collect(recordStart))))))]),'-') as `Time span`,  apoc.coll.sum(apoc.coll.removeAll(Population,[NULL])) as `Population est.`,  
    apoc.coll.sum(apoc.coll.removeAll(`Sample size`,[NULL])) as `Sample size`, '<a href="/app/' + $database + '/?main=view&explore=' + datasetID + '" target="_blank" >' + Source + '</a>' as Source,
    Version, Link order by `Time span`, Source, Name
    '''
            qCategories = """
unwind $cmid as cmid 
match (a:ADM0 {CMID: cmid})-[:DISTRICT_OF]-(c:CATEGORY) 
unwind labels(c) as Domain with Domain, count(*) as Count 
return distinct Domain, Count order by Domain
"""

        else:
            qInfo = '''
    unwind $cmid as cmid 
    match (a:DATASET) 
    where a.CMID = cmid 
    with a call apoc.when(a.District is not null,'return custom.getName($id) as name',
    'return null as name',{id:a.District}) yield value as Location 
    return a.CMName as CMName, custom.anytoList(collect(Location.name),true) as Location, a.CMID as CMID, 
    labels(a) as Domains, a.parent as Parent, a.DatasetCitation as Citation, "<a href ='" + a.DatasetLocation + "' target='_blank' >" + a.DatasetLocation +"</a>" as `Dataset Location`, a.ApplicableYears as `Applicable Years`, a.Note as Note
    '''
            qSamples = None
            qCategories = """
unwind $cmid as cmid match (d:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY) 
unwind r.label as Domain with Domain, count(*) as Count 
return distinct Domain, Count order by Domain
"""

        # return [{"info": qInfo,
        #         "samples":qSamples,
        #         "categories": qCategories}]
        
        with driver.session() as session:
            info = session.run(qInfo, cmid = cmid)
            info = [dict(record) for record in info]
            if qCategories is None:
                categories = []
            else: 
                categories = session.run(qCategories,cmid = cmid)
                categories = [dict(record) for record in categories]
            if qSamples is not None:
                samples = session.run(qSamples, cmid = cmid, database = database)
                samples = [dict(record) for record in samples]
            else: 
                samples = []
            driver.close()

        polygons = getPolygon(cmid,driver)
        points = getPoints(cmid,driver)

        if info is None:
            raise Exception("No results for info")
        if samples is None:
            raise Exception("No results for samples")        

        return jsonify({
            "info": info,
            "samples": samples,
            "polygons": polygons,
            "points": points,
            "categories": categories
        })
    
    except Exception as e:
        return "Error returning results: " + str(e), 500    

# Function to serialize a Neo4j Node object into a serializable dictionary
def serialize_node(node):
    return {
        "id": node.element_id,
        "labels": list(node.labels),
        "properties": dict(node)
    }

# Function to serialize Neo4j Relationship object into a serializable dictionary
def serialize_relationship(relationship):
    return {
        "type": relationship.type,
        "start_node_id": relationship.start_node.element_id,
        "end_node_id": relationship.end_node.element_id,
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

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
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
with collect(distinct a) as a, r, e
return a, collect(distinct r) as r, collect(distinct e) as e
"""        
        else:
            cypher_query = """
unwind $cmid as cmid unwind $relation as relation MATCH (a) 
WHERE a.CMID = cmid 
optional match (a)-[r]-(e) 
where type(r) = relation and
not isEmpty([label IN labels(e) 
WHERE label IN apoc.coll.flatten([$domain],true)]) 
with collect(distinct a) as a, r, e limit 10
return a, collect(distinct r) as r, collect(distinct e) as e
"""        
        
        with driver.session() as session:
            # Execute the Cypher queries
            result = session.run(cypher_query, cmid = cmid, relation = relation,domain = domain, endcmid = endcmid)
            result = CM.unlist([dict(record) for record in result])
            node = []
            rel = []
            end = []
            a = result['a']
            for record in a:
                node.append({"node":serialize_node(record)})
            r = result['r']
            for record in r:
                rel.append({"relation":serialize_relationship(record)})
            e = result['e']
            for record in e:
                end.append({"end":serialize_node(record)})

        driver.close()
        node = [flatten_json(entry) for entry in node]
        rel = [flatten_json(entry) for entry in rel]
        end = [flatten_json(entry) for entry in end]

        return {"node":node,"relations":rel,"relNodes":end,"query":cypher_query,"params":[{"cmid":cmid,"database":database,"domain":domain,"relation":relation,"endcmid":endcmid}]}
    except Exception as e:
        return str(e), 500
    
@app.route('/search', methods=['GET'])
def getSearch():
    """Search endpoint for explore page
    This endpoint is used for database searches of a single or empty term.
    ---
    parameters:
        - name: database
          in: query
          type: string
          enum: ['SocioMap','ArchaMap']
          required: true
          description: Name of the CatMapper database to search
        - name: term
          in: query
          type: string
          required: false
          description: Search term
        - name: property
          in: query
          type: string
          required: false
          enum: ['Name','CMID','Key']
          description: Property to search by
        - name: domain
          in: query
          type: string
          required: false  
          enum: ['DISTRICT','ETHNICITY','STONE']
          default: CATEGORY 
          description: Domain containing the category
        - name: yearStart
          in: query
          type: integer
          required: false  
          description: Earliest year the category existed or data was collected from (will return a result if category year range intersects with year range)
        - name: yearEnd
          in: query
          type: integer
          required: false  
          description: Latest year the category existed or data was collected from
        - name: country
          in: query
          type: string
          required: false  
          description: CMID of ADM0 node with DISTRICT_OF tie
        - name: context
          in: query
          type: string
          required: false  
          description: CMID of parent node in network
        - name: limit
          in: query
          type: string
          required: false 
          default: 10000    
          description: Number of results to limit search to
        - name: query
          in: query
          type: string
          enum: ['true','false']
          required: false     
          description: Whether to return results or cypher query
    response:
        200:
            description: JSON of search results unless query is true, then a JSON with the cypher query is returned.
            schema:
                type: object
                properties:
                    CMID: 
                        type: string
                        example: SM1
                    CMName:
                        type: string
                        example: Afghanistan
                    country: 
                        type: array
                        items:
                            type: string
                        example: ["United States of America"]
                    domain:
                        type: array
                        items:
                            type: string
                        example: ["DISTRICT","FEATURE"]
                    matching: 
                        type: string
                        example: Afghanistan
                    matchingDistance:
                        type: integer
                        example: 1
        500: 
            description: JSON of error
            schema:
            type: string                           
    """
    try:
        database = request.args.get('database')
        term = request.args.get('term')
        property = request.args.get('property')
        domain = request.args.get('domain')
        yearStart = request.args.get('yearStart')
        yearEnd = request.args.get('yearEnd')
        context = request.args.get('context')
        country = request.args.get('country')
        limit = request.args.get('limit')
        query = request.args.get('query')

        if domain == "ANY DOMAIN":
            domain = "CATEGORY"

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception("must specify database as 'SocioMap' or 'ArchaMap'")
        
        if term == "":
            term = None

        if property == "":
            property = None

        if domain == "":
            domain = None

        if domain is None:
            domain = "CATEGORY"

        # need to add check to mak sure property is valid and domain is valid

        if context is not None:
            if context == "null" or context == "":
                context = None
            else:
                if re.search("^SM|^SD|^AD|^AM",context) is None:
                    raise Exception("context must be a valid CMID")
            
        if country is not None:
            if country == "null":
                country = None
            else:
                if re.search("^SM|^SD|^AD|^AM",country) is None:
                    raise Exception("country must be a valid CMID")
                  
        if yearStart == "null":
            yearStart = None

        if yearEnd == "null":
            yearEnd = None

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
            limit = 1000
        
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
call {{with $term as term
call db.index.fulltext.queryNodes('{domain}', '"' + term +'"') yield node return node
union with $term as term
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term)) yield node return node
union with $term as term
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term) + '~') yield node return node}}
with node as a
with a, custom.matchingDist(a.names, $term) as matching
with a, matching.matching as matching, toInteger(matching.score) as score
"""
                
            else:
                qStart = f"""
call {{with $term as term
call db.index.fulltext.queryNodes('{domain}', '"' + term +'"') yield node return node
union with $term as term
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term))  yield node return node
union with $term as term
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(term) + '~') yield node return node}}
with node as a
with a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], $term) as matching
with a, matching.matching as matching, toInteger(matching.score) as score
"""
        elif property == "CMID":
            qStart = """
match (a) where a.CMID = $term
call apoc.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{a:a}) yield value
with value.node as a, value.matching as matching, 0 as score
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
call {{with a where not a.ApplicableYears is null with a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
else a.ApplicableYears end as yearMatch, range(toInteger('{yearStart}'),toInteger('{yearEnd}')) as years
with a, years, [i in apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) | toInteger(i)] as yearMatch 
where not isEmpty([i in yearMatch where toInteger(i) in years]) return a as node}}
with node as a, matching, score
"""   
            else:
                qYear = f"""
call {{ with a with a, range(toInteger('{yearStart}'),toInteger('{yearEnd}')) as inputYears 
match (a)<-[r:USES]-(:DATASET) where r.yearStart is not null and not isEmpty(r.yearStart) with a, inputYears, range(apoc.coll.min([i in apoc.coll.flatten(collect(r.yearStart),true) | 
toInteger(i)]), apoc.coll.max(custom.getYear(collect(r.yearEnd)))) as years where not isEmpty([i in inputYears where i in years]) return a as node}}
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

        cypher_query = qStart + qDomain + qUnique + qCountryFilter + qContext + qYear + qLimit + qCountry + qReturn
            
        if query != 'true':   
            # Execute the Cypher queries
            with driver.session() as session:
                result = session.run(cypher_query, term = term, context = context, country = country)
            
                # Process the query results and generate the dynamic JSON
                data = [dict(record) for record in result]

                driver.close()
            return data
        else:
            print(cypher_query)
            # return([qStart,qDomain,qUnique,qContext,qYear,qLimit,qCountry,qReturn])
            return({"query":cypher_query,"parameters":[{"term": term,"context":context,"country":country,"domain":domain,"yearStart":yearStart,"yearEnd":yearEnd}]})
    except Exception as e:
        return str(e), 500

@app.route('/translate', methods=['POST'])
def getTranslate():
    try:
        data = request.get_data()  
        data = json.loads(data)
        database = CM.unlist(data.get("database"))
        property = CM.unlist(data.get("property"))
        domain = CM.unlist(data.get("domain"))
        key = CM.unlist(data.get("key"))
        if key != 'true':
            key = None
        query = CM.unlist(data.get("query"))
        if query != 'true':
            query = 'false'
        rows = data.get("rows")
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")

        # Define the Cypher query
    
        qLoad = "unwind $rows as row with row call {"

        if property == "Key":
            qStart = f"""
with row call db.index.fulltext.queryRelationships('keys','"' + tolower(row.term) +'"') yield relationship
with row, endnode(relationship) as a, relationship.Key as matching, case when row.term contains ":" then row.term else ": " + row.term end as term
where '{domain}' in labels(a) and matching ends with term
with row, a, matching, 0 as score
"""
        elif property in ["glottocode","ISO","CMID"]:
            if property == "CMID":
                indx = "CMIDindex"
            else:
                indx = property

            qStart = f"""
with row call db.index.fulltext.queryNodes('{indx}','"' + toupper(row.term) +'"') yield node
with row, node as a, toupper(node['{property}']) as matching, toupper(row.term) as term
where matching = term
with row, a call apoc.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{{a:a}}) yield value
with row, value.node as a, value.matching as matching, 0 as score
"""

        elif property == "Name":
    
            if domain != "DATASET":
                qStart = f"""
with row call {{ with row 
call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist(a.names, row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""
            else:
                qStart = f"""
with row call {{ with row 
call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""

        else:
            qStart = f""" 
with row call apoc.cypher.run('match (a:{domain}) 
where not a.{property} is null and tolower(a.{property}) = tolower(\"' + row.term + '\") 
return a, a.{property} as matching',{{}}) yield value 
with row, value.a as a, value.matching as matching, 0 as score
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
            # get keys
            if key is not None:
                qDataset = """
    match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
    with row, a, matching, score, r.Key as Key
    """
            else:
                qDataset = """
    where (a)<-[:USES]-(:DATASET {CMID: row.dataset})
    with row, a, matching, score, '' as Key
    """
        else:
            qDataset = "with row, a, matching, score, '' as Key"

        # filter by year
        if 'yearStart' in rows[0] and 'yearEnd' in rows[0]:
            if domain == "DATASET":
                qYear = """
call {with row, a with row, a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
else a.ApplicableYears end as yearMatch, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
with a, years, apoc.convert.toIntList(apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}
with node as a, matching, score, Key
"""
            else:
                qYear = f"""
call {{with row, a with row, a, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years 
match (a)<-[r:USES]-(:DATASET) unwind r.yearStart as yearStart 
unwind r.yearEnd as yearEnd with years, a, r, apoc.coll.toSet(collect(yearStart) + collect(yearEnd)) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}}
with row, node as a, matching, score, Key order by score desc
"""   
        else: 
            qYear = " "
    
        # limit results
        qLimit = """
with row, collect(a{a, matching, score}) as nodes, collect(score) as scores, Key
with row, nodes, apoc.coll.min(scores) as minScore, Key
unwind nodes as node
with row, node.a as a, node.matching as matching, node.score as score, minScore, Key
where score = minScore
return distinct a, matching, score, Key}
with row, a, matching, score, Key
"""

        # get country
        qCountry = """
optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
with row, a, matching, collect(c.CMName) as country, score, Key
"""

        # return results
        qReturn = """
return distinct row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, custom.getLabel(a) as label, 
matching, score as matchingDistance, country, Key order by matchingDistance
"""
        cypher_query = qLoad + qStart + qDomain + qCountryFilter + qContext + qDataset + qYear + qLimit + qCountry + qReturn
        if query == "true":
            with driver.session() as session:
                result = session.run("unwind $rows as rows unwind rows as row return row.term as term, row.CMuniqueRowID as CMuniqueRowID", rows = rows)
                qResult = [dict(record) for record in result]
                print("printing rows")
                print(rows)
            return [{"query": cypher_query.replace("\n"," "),"params":qResult,"rows":rows}]
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

@app.route('/translate2', methods=['POST'])
def getTranslate2():
    try:
        data = request.get_data()  
        data = json.loads(data)
        database = CM.unlist(data.get("database"))
        property = CM.unlist(data.get("property"))
        domain = CM.unlist(data.get("domain"))

        key = CM.unlist(data.get("key"))
        term = CM.unlist(data.get("term"))
        country = CM.unlist(data.get('country'))
        context = CM.unlist(data.get('context'))
        dataset = CM.unlist(data.get('dataset'))
        yearStart = CM.unlist(data.get('yearStart'))
        yearEnd = CM.unlist(data.get('yearEnd'))
        if key != 'true':
            key = None
        query = CM.unlist(data.get("query"))
        if query != 'true':
            query = 'false'
        table = data.get("table")
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")
        
        
        # format data
        # add rowid, 
        # table = [{'Name':'test1',"key": 1}, {'Name':'test1',"key": 2}, {'Name':'test2',"key": 3}]
        df = pd.DataFrame(table)
        df['CMuniqueRowID'] = df.index
        rows = pd.DataFrame({'term': df[term],'CMuniqueRowID': df["CMuniqueRowID"]})
        if isinstance(country,str) and country in df.columns:
            rows['country'] = df[country]
        if isinstance(context,str) and context in df.columns:
            rows['context'] = df[context]
        if isinstance(dataset,str) and dataset in df.columns:
            rows['dataset'] = df[dataset]
        if isinstance(yearStart,str) and yearStart is not None:
            rows['yearStart'] = yearStart
        if isinstance(yearEnd,str) and yearEnd is not None:
            rows['yearEnd'] = yearEnd
        rows.dropna(subset=['term'], inplace=True)
        rows = rows[rows['term'] != '']
        columns_to_group_by = rows.columns.difference(['CMuniqueRowID']).tolist()
        rows = rows.groupby(columns_to_group_by)['CMuniqueRowID'].apply(list).reset_index()
        rows = rows.to_dict('records')
        
        # Define the Cypher query
    
        qLoad = "unwind $rows as row with row call {"

        if property == "Key":
            qStart = f"""
with row call db.index.fulltext.queryRelationships('keys','"' + tolower(row.term) +'"') yield relationship
with row, endnode(relationship) as a, relationship.Key as matching, case when row.term contains ":" then row.term else ": " + row.term end as term
where '{domain}' in labels(a) and matching ends with term
with row, a, matching, 0 as score
"""
        elif property in ["glottocode","ISO","CMID"]:
            if property == "CMID":
                indx = "CMIDindex"
            else:
                indx = property

            qStart = f"""
with row call db.index.fulltext.queryNodes('{indx}','"' + toupper(row.term) +'"') yield node
with row, node as a, toupper(node['{property}']) as matching, toupper(row.term) as term
where matching = term
with row, a call apoc.when("DELETED" in labels(a),"match (a)-[:IS]->(b) return b as node, a.CMID as matching","return a as node, a.CMID as matching",{{a:a}}) yield value
with row, value.node as a, value.matching as matching, 0 as score
"""

        elif property == "Name":
    
            if domain != "DATASET":
                qStart = f"""
with row call {{ with row 
call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist(a.names, row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""
            else:
                qStart = f"""
with row call {{ with row 
call db.index.fulltext.queryNodes('{domain}', '"' + row.term + '"') yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term)) yield node return node
union with row 
call db.index.fulltext.queryNodes('{domain}', custom.cleanText(row.term) + '~') yield node return node}}
with row, node as a
with row, a, custom.matchingDist([a.CMName, a.shortName, a.DatasetCitation], row.term) as matching
with row, a, matching.matching as matching, toInteger(matching.score) as score
"""

        else:
            qStart = f""" 
with row call apoc.cypher.run('match (a:{domain}) 
where not a.{property} is null and tolower(a.{property}) = tolower(\"' + row.term + '\") 
return a, a.{property} as matching',{{}}) yield value 
with row, value.a as a, value.matching as matching, 0 as score
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
            if property == "Key":
                qDataset = """
    match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
    where r.Key ends with row.term
    with row, a, matching, score, r.Key as Key
    """
            else: 
                qDataset = """
    match (a)<-[r:USES]-(d:DATASET {CMID: row.dataset}) 
    with row, a, matching, score, r.Key as Key
    """
        else:
            qDataset = "with row, a, matching, score, '' as Key"

        if key is None:
            "with row, a, matching, score, '' as Key"

        # filter by year
        if 'yearStart' in rows[0] and 'yearEnd' in rows[0]:
            if domain == "DATASET":
                qYear = """
call {with row, a with row, a, case when a.ApplicableYears contains '-' then split(a.ApplicableYears,'-') 
else a.ApplicableYears end as yearMatch, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years
with a, years, apoc.convert.toIntList(apoc.coll.toSet(apoc.coll.flatten(collect(yearMatch),true))) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}
with node as a, matching, score, Key
"""
            else:
                qYear = f"""
call {{with row, a with row, a, range(toInteger(row.yearStart),toInteger(row.yearEnd)) as years 
match (a)<-[r:USES]-(:DATASET) unwind r.yearStart as yearStart 
unwind r.yearEnd as yearEnd with years, a, r, apoc.coll.toSet(collect(yearStart) + collect(yearEnd)) as yearMatch 
where size([i in yearMatch where toInteger(i) in years]) > 0 return a as node}}
with row, node as a, matching, score, Key order by score desc
"""   
        else: 
            qYear = " "
    
        # limit results
        qLimit = """
with row, collect(a{a, matching, score}) as nodes, collect(score) as scores, Key
with row, nodes, apoc.coll.min(scores) as minScore, Key
unwind nodes as node
with row, node.a as a, node.matching as matching, node.score as score, minScore, Key
where score = minScore
return distinct a, matching, score, Key}
with row, a, matching, score, Key
"""

        # get country
        qCountry = """
optional match (a)<-[:DISTRICT_OF]-(c:ADM0)
with row, a, matching, collect(c.CMName) as country, score, Key
"""

        # return results
        qReturn = """
return distinct row.CMuniqueRowID as CMuniqueRowID, row.term as term, a.CMID as CMID, a.CMName as CMName, custom.getLabel(a) as label, 
matching, score as matchingDistance, country, Key order by matchingDistance
"""
        cypher_query = qLoad + qStart + qDomain + qCountryFilter + qContext + qDataset + qYear + qLimit + qCountry + qReturn
        if query == "true":
            with driver.session() as session:
                result = session.run("unwind $rows as rows unwind rows as row return row.term as term", rows = rows)
                qResult = [dict(record) for record in result]
                print("printing rows")
                print(rows)
            return [{"query": cypher_query.replace("\n"," "),"params":qResult,"rows":rows}]
        else:
        # Execute the Cypher queries
            with driver.session() as session:
                result = session.run(cypher_query, rows = rows)
        
            # Process the query results and generate the dynamic JSON
                data = [dict(record) for record in result]

                driver.close()

        data = pd.DataFrame(data)
        data = data.replace("", pd.NA)
        data = data.dropna(axis='columns', how='all')
        # add matching type
        data = CM.addMatchResults(results = data)
        new_column_names = {col: f"{col}_{term}" for col in data.columns if col != 'CMuniqueRowID'}
        data = data.rename(columns=new_column_names)
        data = data.explode('CMuniqueRowID')
        data = data.drop(f"term_{term}", axis=1)

        data['CMuniqueRowID'] = data['CMuniqueRowID'].astype(int)
        df['CMuniqueRowID'] = df['CMuniqueRowID'].astype(int)

        data = pd.merge(df, data, on="CMuniqueRowID", how='outer')
        data[f'matchType_{term}'] = data[f'matchType_{term}'].fillna('none')
        data.fillna('', inplace=True)
        dtypes = data.dtypes.to_dict()
        list_cols = []
        for col_name, typ in dtypes.items():
            if typ == 'object' and isinstance(data[col_name].iloc[0], list):
                list_cols.append(col_name)

        # Explode list-type columns
        for col in list_cols:
            data = data.explode(col)

        data = data.astype(str)

        # print(data)
                
        return data.to_dict(orient = "records")

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
        
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")
        
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
    if str.lower(database) == "sociomap":
        driver = connectionSM()
    elif str.lower(database) == "archamap":
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
        password = CM.password_hash(password)
        intendedUse = data.get("intendedUse")
        
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
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
u.role = 'user',
u.intendedUse = $intendedUse
return u.userID as userID
"""

        with driver.session() as session:
            result = session.run(query,firstName = firstName, lastName = lastName, email = email, password = password,username = username,intendedUse = intendedUse)
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
            return f"Error: please contact admin@catmapper.org. Error: {error_message}", 500

@app.route('/admin', methods=['GET'])
def getAdmin():
    """
    Retrieve the 'admin.html' template and return it as a response.

    Returns:
    - Response: A Flask response containing the 'admin.html' template.

    Example:
    ```python
    from flask import Flask

    app = Flask(__name__)

    @app.route('/admin')
    def admin_route():
        return getAdmin()
    ```
    """
    headers = {'Content-Type': 'text/html'}
    return make_response(render_template('admin.html'),200,headers)

@app.route('/admin/edit', methods=['GET','POST'])
def getAdminEdit():
    # will not be documented in swagger at this point
    try:
        if request.method == 'GET':
            data = request.args
        elif request.method == "POST":
            data = request.get_data()  
            data = json.loads(data)
        else: 
            raise Exception("invalid request method")
        database = CM.unlist(data.get('database'))
        fun = CM.unlist(data.get('fun'))
        apikey = CM.unlist(data.get('apikey'))
        if apikey != apikeyEnv:
            raise Exception(f"Error: apikey is invalid: {apikey}")
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception("Database must be 'SocioMap' or 'ArchaMap'")
        result = "Nothing returned"
        # if fun == "getUSESrels":
        #     result = CM.getUSESrels(request,driver)
        if fun == "mergeNodes":
            result = CM.mergeNodes(request,driver)
        elif fun == "addIndexes":
            result = CM.addIndexes(driver)
        elif fun == "updateUses":
            CMID = data.get('CMID') 
            result = CM.updateUses(driver = driver, CMID = CMID)    
        else:
            raise Exception("Function does not exist")
        return result
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        data = str(e)
        return data, 500

@app.route('/dataset', methods=['GET'])
def getDataset():
    # to do: document
    try:
        database = CM.unlist(request.args.get('database'))
        cmid = CM.unlist(request.args.get('cmid'))
        domain = CM.unlist(request.args.get('domain'))
        children = CM.unlist(request.args.get('children'))

        if domain is None:
            domain = "CATEGORY"
      
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception("Database must be 'SocioMap' or 'ArchaMap'")

        # determine if dataset has child datasets
        query = """
        unwind $cmid as cmid
        match (d:DATASET {CMID: cmid})-[:USES]->(c:CATEGORY) return count(c) as n
        """

        session = driver.session()
        count = session.run(query,cmid = cmid)
        count =  [dict(record) for record in count]
        count = count[0].get("n")

        if count == 0:
            query = """
            unwind $cmid as cmid
            match (:DATASET {CMID: cmid})-[:CONTAINS*..5]->(d:DATASET) return d.CMID as CMID
            """
            result = session.run(query,cmid = cmid)
            result = [record["CMID"] for record in result]
            if result is not None:
                cmid = [cmid] + result
        query = """
 unwind $cmid as cmid
 match (a:DATASET)-[r:USES]->(b) 
 where a.CMID = cmid and not isEmpty([i in labels(b) 
 where i in apoc.coll.flatten([$domain],true)]) 
 unwind keys(r) as property with a,r,b, property 
 where not property in ['type','Key','log'] 
 return distinct a.CMName as datasetName, a.CMID as datasetID, 
 b.CMID as CMID, b.CMName as CMName, r.type as type, 
 r.Key as Key, property, r[property] as value
"""

        with driver.session() as session:
            result = session.run(query,cmid = cmid,domain = domain)
            data = [dict(record) for record in result]
            driver.close()
        df = pd.DataFrame(data)
        df = df.pivot_table(index='CMID', columns='property', values='value', aggfunc='first').reset_index()
        dtypes = df.dtypes.to_dict()
        list_cols = []
        for col_name, typ in dtypes.items():
            if typ == 'object' and isinstance(df[col_name].iloc[0], list):
                list_cols.append(col_name)

        # Explode list-type columns
        for col in list_cols:
            df = df.explode(col)
        df = df.astype(str)
        return jsonify(df.to_json(orient='records'))
    
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@app.route('/routines', methods=['GET'])
def routines():
    # this route will not be documented in swagger
    # it is intended for automatic routines only
    try:
        database = CM.unlist(request.args.get('database'))
        fun = CM.unlist(request.args.get('fun'))
        data = CM.unlist(request.args.get('data'))
        if data is None:
            data = False
        elif data.lower() == "true":
            data = True
        else:
            data = False
        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception("Database must be 'SocioMap' or 'ArchaMap'")
        result = "Nothing returned"
        if fun == "addLog":
            result = CM.addLog(driver = driver)
        elif fun == "checkDomains":
            result = CM.checkDomains(data = data,driver = driver)
        elif fun == "updateUses":
            CMID = request.args.get('CMID') 
            result = CM.updateUses(driver = driver, CMID = CMID)    
        else:
            result = "function not found"
        return result
    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@app.route('/CMID', methods=['GET'])
def getCMID():
    try:
        database = request.args.get('database')
        cmid = request.args.get('cmid')

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")   
        
        query1 = """
unwind $cmid as cmid 
match (c {CMID: cmid}) 
unwind keys(c) as nodeProperties  
return id(c) as nodeID, nodeProperties, c[nodeProperties] as nodeValues
"""
        query2 = """
unwind $cmid as cmid 
match (c {CMID: cmid})<-[r:USES]-(d) 
unwind keys(r) as relProperties 
return id(r) as relID, relProperties, r[relProperties] as relValues
"""

        with driver.session() as session:
            result = session.run(query1,cmid = cmid)
            node = [dict(record) for record in result]
            result = session.run(query2,cmid = cmid)
            relations = [dict(record) for record in result]
            driver.close()

        grouped_data = defaultdict(dict)

        for entry in relations:
            rel_id = entry['relID']
            prop = entry['relProperties']
            val = entry['relValues']
            

            if prop in grouped_data[rel_id]:

                if isinstance(grouped_data[rel_id][prop], list):
                    grouped_data[rel_id][prop].extend(val if isinstance(val, list) else [val])
                else:
                    grouped_data[rel_id][prop] = val
            else:
                grouped_data[rel_id][prop] = val


        relations = dict(grouped_data)

        return {"node": node,"relations":relations}

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@app.route('/allDatasets', methods=['GET'])
def getAllDatasets():
    try:
        database = request.args.get('database')

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        elif database == "gisdb":
            driver = connectionGIS()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")   
        
        query = """
match (d:DATASET) 
return id(d) as nodeID, 
d.CMName as CMName, 
d.CMID as CMID, 
d.shortName as shortName, 
d.project as project, 
d.Unit as Unit, 
d.parent as parent, 
d.ApplicableYears as ApplicableYears, 
d.DatasetCitation as DatasetCitation, 
d.District as District, 
d.DatasetLocation as DatasetLocation, 
d.SubNational as SubNational, 
d.DatasetVersion as DatasetVersion, 
d.DatasetScope as DatasetScope, 
d.Subdistrict as Subdistrict, 
d.Note as Note
"""

        with driver.session() as session:
            result = session.run(query)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@app.route('/linkfile', methods=['GET'])
def getLinkFile():
    try:
        database = request.args.get('database')
        datasets = request.args.get('datasets')
        intersection = request.args.get('intersection')
        domain = request.args.get('domain')

        if not isinstance(datasets,list):
            raise Exception("datasets must be a list")

        if not isinstance(domain,str):
            raise Exception("domain must be a string")
        
        if not isinstance(intersection,bool):
            raise Exception("intersection must be a boolean")

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")   
        
        query = f"""
match (c:{domain})<-[r:USES]-(d:DATASET) where d.CMID in $datasets
return distinct d.CMName as DatasetName, r.Key as Key, c.CMName as CMName, c.CMID as CMID, apoc.text.join(r.Name,'; ') as Name order by CMName
"""

        with driver.session() as session:
            result = session.run(query, datasets = datasets)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@app.route('/networknodes', methods=['POST'])
def getnetworknodes():
    try:
    
        data = request.get_data()  
        data = json.loads(data)

        database = CM.unlist(data.get('database'))
        cmid = CM.unlist(data.get('cmid'))
        relation = data.get('relation')
        domains = data.get('domains')

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")  
        
        query = """
        unwind $cmid as cmid 
        unwind $relation as relation 
        match (a)-[r]-(b) 
        where a.CMID = cmid and type(r) = relation and ANY(label IN labels(b) 
        WHERE label IN apoc.coll.flatten([$domains],true)) 
        return b.CMID as CMID, b.CMName as Name order by Name limit 1000
"""

        with driver.session() as session:
            result = session.run(query, cmid = cmid, relation = relation, domains = domains)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

@app.route('/networkDomains', methods=['POST'])
def getnetworkDomains():
    try:
    
        data = request.get_data()  
        data = json.loads(data)

        database = CM.unlist(data.get('database'))
        cmid = CM.unlist(data.get('cmid'))
        relation = data.get('relation')
        domains = data.get('domains')

        if str.lower(database) == "sociomap":
            driver = connectionSM()
        elif str.lower(database) == "archamap":
            driver = connectionAM()
        else:
            raise Exception(f"must specify database as 'SocioMap' or 'ArchaMap', but database is {database}")  
        
        query = """
        unwind $cmid as cmid 
        unwind $relation as relation 
        match (a)-[r]-(b) 
        where a.CMID = cmid and type(r) = relation and ANY(label IN labels(b) 
        WHERE label IN apoc.coll.flatten([$domains],true)) 
        return b.CMID as CMID, b.CMName as Name order by Name limit 1000
"""

        with driver.session() as session:
            result = session.run(query, cmid = cmid, relation = relation, domains = domains)
            data = [dict(record) for record in result]
            driver.close()

        return data

    except Exception as e:
    # In case of an error, return an error response with an appropriate HTTP status code
        result = str(e)
        return result, 500

if __name__== "__main__":
    app.run(debug=True,port=5001)



