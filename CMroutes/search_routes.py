from flask import request, Blueprint, jsonify
import json
from CM import translate, unlist, search

search_bp = Blueprint('search', __name__)
    
@search_bp.route('/search', methods=['GET'])
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
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = request.args.get('domain')
        yearStart = request.args.get('yearStart')
        yearEnd = request.args.get('yearEnd')
        context = request.args.get('context')
        dataset = request.args.get('dataset')
        country = request.args.get('country')
        query = request.args.get('query')

        result = search(
            database,
            term,
            property,
            domain,
            yearStart,
            yearEnd,
            context,
            country,
            query,
            dataset)
        
        return jsonify(result)

    except Exception as e:
        return str(e), 500    

@search_bp.route('/translate', methods=['POST'])
def getTranslate2():
    try:
        data = request.get_data()
        data = json.loads(data)
        database = unlist(data.get("database"))
        property = unlist(data.get("property"))
        if property == "CatMapper ID (CMID)":
            property = "CMID"
        domain = unlist(data.get("domain"))
        key = unlist(data.get("key"))
        term = unlist(data.get("term"))
        country = unlist(data.get('country'))
        context = unlist(data.get('context'))
        dataset = unlist(data.get('dataset'))
        yearStart = unlist(data.get('yearStart'))
        yearEnd = unlist(data.get('yearEnd'))
        query = unlist(data.get("query"))
        table = data.get("table")
        countsamename = data.get("countsamename")
        uniqueRows = data.get("uniqueRows")

        data, desired_order = translate(
            database=database,
            property=property,
            domain=domain,
            key=key,
            term=term,
            country=country,
            context=context,
            dataset=dataset,
            yearStart=yearStart,
            yearEnd=yearEnd,
            query=query,
            table=table,
            countsamename=countsamename,
            uniqueRows=uniqueRows)

        data_dict = data.to_dict(orient='records')

        print(data_dict)

        return jsonify({"file": data_dict, "order": desired_order})

    except Exception as e:
        return str(e), 500
