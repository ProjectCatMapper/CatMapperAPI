''' admin.py '''

############################
# Code for the following admin functions
# 1.  add, edit, delete USES tie properties
# 2.  add, edit, delete NODE properties
# 3.  Merge nodes
# 4.  Move uses ties
# 5.  delete Node
# 6.  delete USES relation
# 7.  create label
############################

from .utils import *
from .metadata import *
from .log import createLog
from .USES import processUSES
from .USES import addCMNameRel, processDATASETs
from .upload import updateProperty
from .keys import invalid_key_format_error
from flask import jsonify
from collections import Counter

# This is a module for admin functions in CatMapper

import re

############################
#section for general use helper functions

_ADMIN_MULTI_VALUE_SEPARATOR = re.compile(r"\s*(?:\|{2,}|,|;)\s*")


def _split_admin_multi_value(value):
    if value is None:
        return []
    if isinstance(value, list):
        values = value
    else:
        values = _ADMIN_MULTI_VALUE_SEPARATOR.split(str(value))
    return [str(val).strip() for val in values if str(val).strip()]


def _editable_node_label(cmid, driver):
    query = """
    MATCH (n {CMID: $cmid})
    RETURN labels(n) AS labels
    """
    rows = getQuery(query=query, driver=driver, params={"cmid": cmid}, type="dict")
    if not rows:
        raise ValueError(f"{cmid} is invalid")

    labels = set(rows[0].get("labels") or [])
    if "DELETED" in labels:
        raise ValueError(f"{cmid} is a deleted node and cannot be edited.")
    if "DATASET" in labels:
        return "DATASET"
    if "CATEGORY" in labels:
        return "CATEGORY"
    if "PROPERTY" in labels:
        return "PROPERTY"
    if "LABEL" in labels:
        return "LABEL"

    raise ValueError(f"{cmid} has no editable node label.")

#Function used by deleteNode to get elementId for either a single or list of CMIDs
#deletenode only needs to operate on a string
def getID(id_value, property, driver):
    """
    Given a node property CMID, returns the internal Neo4j element ID.
    Returns None if no node is found.
    
    Parameters:
    - id_value: str or int, the value to match
    - property: str, property CMID
    - driver: Neo4j driver session or wrapper with a run(query, params) method
    """
    # Ensure id_value is a list of trimmed strings
    if isinstance(id_value, str):
        id_list = [id_value.strip()]
    else:
        id_list = [str(x).strip() for x in id_value]

    property = sanitize_cypher_identifier(property, "property")

    # Construct and execute Cypher query
    query = f"""
        UNWIND $id AS id
        MATCH (a)
        WHERE a.{property} = id
        RETURN elementId(a) AS id
    """
    result = getQuery(query=query, params={"id": id_list}, driver=driver)

    # Return first result if any, else None
    return result[0]["id"] if result else None

def getGroupLabels(CMID,driver):
    cmid = str(CMID or "").strip()
    if not cmid:
        raise ValueError("CMID is required to validate group labels.")

    query = """
    UNWIND $cmid AS cmid
    MATCH (n)
    WHERE n.CMID = cmid
    RETURN labels(n)
    """
    result = getQuery(query=query, params={"cmid": [cmid]}, driver=driver,type="list")

    if not result:
        raise ValueError(f"{cmid} is invalid")

    result = result[0]
    if "CATEGORY" in result:
        result.remove("CATEGORY")

    if not result:
        raise Exception(f"{CMID} has improper labels, unable to verify grouplabels")
        
    result = result[0]

    query = """
    UNWIND $label AS label
    MATCH (m:LABEL)
    WHERE m.CMName = label
    RETURN m.groupLabel AS groupLabel
    """
    result = getQuery(query=query, params={"label": result}, driver=driver)
    if not result or not result[0].get("groupLabel"):
        raise ValueError(f"{cmid} has no group label metadata, unable to validate parent property.")

    result = result[0]['groupLabel']

    return result


def _selected_admin_relation(input_payload, relation_type="USES"):
    index_value = input_payload.get('s1_7')
    relations = input_payload.get('s1_4') or []

    try:
        selected_index = int(index_value) - 1
    except (TypeError, ValueError):
        raise ValueError(f"Invalid {relation_type} tie selection index.")

    if (
        not isinstance(relations, list)
        or selected_index < 0
        or selected_index >= len(relations)
    ):
        raise ValueError(f"Selected {relation_type} tie is invalid or no longer available.")

    selected_relation = relations[selected_index]
    if not isinstance(selected_relation, list) or len(selected_relation) < 3:
        raise ValueError(f"Selected {relation_type} tie payload is invalid.")

    relation_props = selected_relation[1] if isinstance(selected_relation[1], dict) else {}
    dataset_props = selected_relation[2] if isinstance(selected_relation[2], dict) else {}
    dataset_id = dataset_props.get("CMID")
    if not dataset_id:
        raise ValueError(f"Selected {relation_type} tie is missing its dataset CMID.")

    return selected_relation, relation_props, dataset_id


def _resolve_primary_domain_from_labels(labels, driver):
    """
    Resolve a node's primary domain from its labels.

    For CATEGORY-style nodes this uses LABEL.groupLabel mapping.
    For DATASET nodes, primary domain is DATASET.
    """
    labels = [lbl for lbl in (labels or []) if lbl]
    labels_no_structural = [
        lbl for lbl in labels
        if lbl not in {"CATEGORY", "DELETED", "MERGING", "STACK"}
    ]

    if "DATASET" in labels_no_structural:
        return "DATASET"

    candidate_labels = [lbl for lbl in labels_no_structural if lbl != "DATASET"]
    if not candidate_labels:
        raise Exception("Unable to determine a primary domain from node labels.")

    query = """
    UNWIND $labels AS label
    OPTIONAL MATCH (m:LABEL {CMName: label})
    RETURN label, m.groupLabel AS groupLabel
    """
    mapped = getQuery(query=query, driver=driver, params={"labels": candidate_labels}, type="dict")

    primary_domains = set()
    for row in mapped:
        label = row.get("label")
        group_label = row.get("groupLabel")
        if isinstance(group_label, str) and group_label.strip():
            primary_domains.add(group_label.strip())
        elif isinstance(label, str) and label.strip():
            primary_domains.add(label.strip())

    if len(primary_domains) != 1:
        domains = ", ".join(sorted(primary_domains)) if primary_domains else "none"
        raise Exception(f"Unable to determine a unique primary domain (found: {domains}).")

    return list(primary_domains)[0]


def getNodeMergeSummary(cmid, driver):
    """
    Return merge-summary details for a CMID:
    - CMID
    - CMName
    - labels
    - primaryDomain
    """
    cmid = str(cmid or "").strip()
    if not cmid:
        raise Exception("CMID cannot be empty.")

    query = """
    UNWIND $cmid AS cmid
    MATCH (n {CMID: cmid})
    RETURN n.CMID AS CMID, n.CMName AS CMName, labels(n) AS labels
    """
    rows = getQuery(query=query, driver=driver, params={"cmid": [cmid]}, type="dict")
    if not rows:
        raise Exception(f"{cmid} is invalid")

    row = rows[0]
    labels = row.get("labels") or []
    primary_domain = _resolve_primary_domain_from_labels(labels, driver)

    return {
        "CMID": row.get("CMID") or cmid,
        "CMName": row.get("CMName"),
        "labels": labels,
        "primaryDomain": primary_domain,
    }

def validatePropertyCMID(value,proptoChange,validgroupLabel,driver):
    values = _split_admin_multi_value(value)
    if not values:
        raise Exception(f"{proptoChange} requires at least one CMID")

    for val in values:

        validprop = isValidCMID(val, driver)

        if len(validprop) == 0:
            raise Exception(f"{val} is invalid")

        grouplabel = getGroupLabels(val,driver)
        #permits GENERICS as parents of other domains
        if not (proptoChange == "parent" and grouplabel == "GENERIC"):
            if validgroupLabel != grouplabel:
                raise Exception(f"All {proptoChange} CMIDS should be a {validgroupLabel}")

def validate_parent_context_list(driver,parent_context_list):
    """
    parent_context_list: list of JSON strings
    """

    VALID_EVENT_TYPES = {
    "SPLIT",
    "HIERARCHY",
    "SPLITMERGE",
    "MERGED",
    "FOLLOWS",
    }

    CURRENT_YEAR = datetime.now().year

    errors = []

    for idx, raw in enumerate(parent_context_list):
        # here we load the string as a JSON element
        try:
            # use object_pairs_hook to get all key occurrences
            def check_duplicates(pairs):
                keys = [k for k, _ in pairs]
                counts = Counter(keys)
                duplicates = [k for k, v in counts.items() if v > 1]
                if duplicates:
                    raise ValueError(f"Duplicate keys found: {duplicates}")
                return dict(pairs)
            
            pc = json.loads(raw, object_pairs_hook=check_duplicates)
        except ValueError as ve:
            errors.append((idx, str(ve)))
            continue
        except Exception as e:
            errors.append((idx, f"Invalid JSON:{e}"))
            continue

        # 1️⃣ Must be a dict
        if not isinstance(pc, dict):
            errors.append((idx, "parentContext entry is not an object"))
            continue

        # 2️⃣ Must contain ONLY allowed keys
        allowed_keys = {"parent", "eventType", "eventDate"}
        extra_keys = set(pc.keys()) - allowed_keys
        missing_keys = {"parent"} - set(pc.keys())

        if extra_keys:
            errors.append((idx, f"Extra keys found: {extra_keys}"))
            continue

        if missing_keys:
            errors.append((idx, f"Missing required keys: {missing_keys}"))
            continue

        parent = pc.get("parent")
        event_type = pc.get("eventType")
        event_date = pc.get("eventDate")

        # 3️⃣ parent validation
        if not isinstance(parent, str):
            errors.append((idx, "Invalid parent value - parent is not string"))
            continue

        q = """
            unwind $CMID as cmid
            RETURN EXISTS { MATCH (p { CMID: cmid })} AS cmidExists
            """
        result = getQuery(q,driver=driver,params = {"CMID": parent})

        if not result[0]['cmidExists']:
            errors.append((idx, "Parent CMID not in database"))
            continue

        # 4️⃣ eventType validation
        if not isinstance(event_type, str) or event_type not in VALID_EVENT_TYPES:
            errors.append((idx, f"Invalid eventType: {event_type}"))
            continue

        # 5️⃣ eventDate validation
        if event_date not in (None, ""):
            if event_type in (None, ""):
                errors.append((idx, "eventType is required when eventDate exists."))
                continue

            if isinstance(event_date, int):
                event_year = event_date
            elif isinstance(event_date, str) and re.fullmatch(r"-?\d+", event_date.strip()):
                event_year = int(event_date.strip())
            else:
                errors.append((idx, "eventDate must be an integer year string"))
                continue

            if event_year > CURRENT_YEAR:
                errors.append((idx, f"eventDate out of range: {event_date}"))
                continue

    return errors
        
############################
#section for add, edit, delete USES ties
#Function for editing USES tie properties.
def add_edit_delete_USES(database,user,input):
    CMID = (input.get('s1_2') or "").strip()
    USES_property = input.get('s1_8')
    new_property_value = (input.get('s1_3') or "").strip()
    addOrEditNode = input.get('s1_1')
    selected_relation, relation_props, datasetID = _selected_admin_relation(input, "USES")
    key = relation_props.get("Key")
    relID = relation_props.get("id")
    integer_constrained_properties = {
        "yearStart", "yearEnd", "recordStart", 
        "recordEnd", "sampleSize", "yearPublished"
    }

    driver = getDriver(database)

    metaTypes = getPropertiesMetadata(driver)
    metaType = [item['metaType'] for item in metaTypes if item["type"] == "relationship" and item["property"] == USES_property]
    is_list_meta = any(isinstance(mt, str) and "list" in mt.lower() for mt in metaType)
    
    # When adding or editing properties, checks to make sure CMIDs are valid and the labels are correct.
    if addOrEditNode != "delete":
        if USES_property == "categoryType":
            node_summary = getNodeMergeSummary(CMID, driver)
            if node_summary.get("primaryDomain") == "VARIABLE":
                new_property_value = validate_variable_category_type_value(new_property_value)

        if USES_property == "parent":
            groupLabel = getGroupLabels(CMID,driver)
            parent_values = _split_admin_multi_value(new_property_value)
            validatePropertyCMID(parent_values,USES_property,groupLabel,driver)
            new_property_value = parent_values
        else:
            query = """
                    UNWIND $prop as prop
                    MATCH (n:PROPERTY)
                    WHERE n.CMName = prop
                    RETURN n.groupLabel as groupLabel
                    """
            groupLabel = getQuery(query=query, params={"prop": USES_property}, driver=driver)
            groupLabel = groupLabel[0].get('groupLabel') if groupLabel else None
            if groupLabel:
                validatePropertyCMID(new_property_value,USES_property,groupLabel,driver)
        
        if USES_property in integer_constrained_properties:
            try:
                temp_val = float(new_property_value)
                if not temp_val.is_integer():
                    raise ValueError(f"Property '{USES_property}' must be an integer, but received decimal: {new_property_value}")
                new_property_value = int(temp_val)
            except (ValueError, TypeError):
                raise TypeError(f"Property '{USES_property}' requires a valid integer value. Received: {new_property_value}")

        elif USES_property == "populationEstimate":
            try:
                # Validate numeric input, then keep string form for formatter compatibility.
                float(new_property_value)
                new_property_value = str(new_property_value).strip()
            except (ValueError, TypeError):
                raise TypeError(f"Property '{USES_property}' requires a floating-point number. Received: {new_property_value}")

                
    if is_list_meta and not isinstance(new_property_value, list):
        normalized_value = str(new_property_value).strip()
        if "||" in normalized_value:
            new_property_value = [part.strip() for part in normalized_value.split("||") if part.strip()]
        else:
            new_property_value = [normalized_value]
    
    if addOrEditNode == "edit" or addOrEditNode == "add":
        if USES_property == "Key":
            invalid_key_error = invalid_key_format_error([new_property_value], "Key")
            if invalid_key_error:
                raise ValueError(invalid_key_error)

            data = {
                'CMID': CMID,
                'Key': key,
                'datasetID': datasetID,
                'relID': relID,
                "NewKey": new_property_value
            }
            USES_property = ["NewKey"]

            query = """UNWIND $rows AS row
                OPTIONAL MATCH (a:DATASET {CMID: row.datasetID})-[r:USES {Key: row.NewKey}]->(b:CATEGORY {CMID: row.CMID})
                RETURN row.CMID AS CMID, row.datasetID AS datasetID, row.NewKey AS Key, COUNT(r) AS rel_count"""
        
            with driver.session() as session:
                results = session.run(query, rows=data)
                keyExists = [
                    (r["CMID"], r["datasetID"], r["Key"])
                    for r in results.data()
                    if r["rel_count"] >= 1
                ]

                if keyExists:
                    raise ValueError(
                        f"Error:CMID, Key and datasetID triplet already exists for {keyExists}"
                    )
        
        else:
            if USES_property == "parentContext":
                if isinstance(new_property_value, str):
                    if "||" in new_property_value:
                        new_property_value = [part.strip() for part in new_property_value.split("||") if part.strip()]
                    else:
                        new_property_value = [new_property_value]
                result = validate_parent_context_list(driver,new_property_value)
                if result:
                    raise ValueError(
                        f"Error: {result}"
                    )        

            data = {
                    'CMID': CMID,
                    'Key': key,
                    'datasetID': datasetID,
                    'relID': relID,
                    USES_property: new_property_value
                }
            USES_property = [USES_property]
        
        if CMID[1] == "D":
            isDataset = True
        elif CMID[1] == "M":
            isDataset = False
              
        df = pd.DataFrame([data])
        # Arguments coming from admin have already been parsed into lists
        update_result = updateProperty(
            df,
            USES_property,
            isDataset,
            database,
            user,
            updateType="overwrite",
            propertyType="USES",
        )
        if isinstance(update_result, str) and update_result.lower().startswith("error"):
            raise Exception(update_result)
        if isinstance(update_result, dict):
            updated_rows = update_result.get("result")
            if isinstance(updated_rows, list) and len(updated_rows) == 0:
                raise Exception("No USES ties were updated. Verify the selected relation still exists.")
        processUSES(CMID=CMID,database=database,user=user)
    elif addOrEditNode == "delete":
        if relID:
            q = """
                    MATCH ()-[r:USES]-()
                    WHERE elementId(r) = $relID
                    REMOVE r[$USES_property]
                    RETURN elementId(r) as relID
                """
            params = {
                "relID": relID,
                "USES_property": USES_property
            }
        else:
            q = f"""
                    MATCH (a:CATEGORY {{CMID: $CMID}})<-[r:USES {{Key: $key}}]-(d:DATASET {{CMID: $datasetID}})
                    REMOVE r[$USES_property] RETURN elementId(r) as relID
                """
            params = {
                "CMID": CMID,
                "key": key,
                "datasetID": datasetID,
                "USES_property": USES_property
            }
        result = getQuery(q,driver=driver,params = params)
        processUSES(CMID=CMID,database=database,user=user)
        rel_ids = [row["relID"] for row in result] if isinstance(result, list) else []
        if not rel_ids:
            raise Exception("No USES ties were updated. Verify the selected relation still exists.")
        log_message = f"deleted USES property {input.get('s1_8')}"
        createLog(
            id=rel_ids,
            type="relation",
            log=[log_message] * len(rel_ids),
            user=user,
            driver=driver,
        )

    return "done"


def add_edit_delete_CATEGORY_MERGING(database, user, input):
    CMID = (input.get('s1_2') or "").strip()
    MERGING_property = input.get('s1_8')
    new_property_value = (input.get('s1_3') or "").strip()
    addOrEditNode = input.get('s1_1')
    indexValue = input.get('s1_7')
    selected_relations = input.get('s1_4') or []
    allowed_properties = {"stack", "Key"}

    if MERGING_property not in allowed_properties:
        raise ValueError("Only stack and Key can be edited on category MERGING ties.")

    try:
        selected_index = int(indexValue) - 1
    except (ValueError, TypeError):
        raise ValueError("Invalid category MERGING tie selection index.")

    if not isinstance(selected_relations, list) or selected_index < 0 or selected_index >= len(selected_relations):
        raise ValueError("Selected category MERGING tie is invalid or no longer available.")

    selected_relation = selected_relations[selected_index]
    if not isinstance(selected_relation, list) or len(selected_relation) < 2:
        raise ValueError("Selected category MERGING tie payload is invalid.")

    relID = selected_relation[1].get("id")
    safe_rel_id = sanitize_cypher_element_id(relID, "relationship elementId")
    driver = getDriver(database)

    if addOrEditNode == "edit" or addOrEditNode == "add":
        q = """
            MATCH (:DATASET)-[r:MERGING]->(:CATEGORY)
            WHERE elementId(r) = $relID
            SET r[$prop] = $value
            RETURN elementId(r) as relID
        """
        result = getQuery(q, driver=driver, params={"relID": safe_rel_id, "prop": MERGING_property, "value": new_property_value})
        rel_ids = [row["relID"] for row in result] if isinstance(result, list) else []
        if not rel_ids:
            raise Exception("No category MERGING ties were updated. Verify the selected relation still exists.")
        createLog(
            id=rel_ids,
            type="relation",
            log=[f"updated category MERGING property {MERGING_property} to {new_property_value} for {CMID}"] * len(rel_ids),
            user=user,
            driver=driver,
        )
    elif addOrEditNode == "delete":
        q = """
            MATCH (:DATASET)-[r:MERGING]->(:CATEGORY)
            WHERE elementId(r) = $relID
            REMOVE r[$prop]
            RETURN elementId(r) as relID
        """
        result = getQuery(q, driver=driver, params={"relID": safe_rel_id, "prop": MERGING_property})
        rel_ids = [row["relID"] for row in result] if isinstance(result, list) else []
        if not rel_ids:
            raise Exception("No category MERGING ties were updated. Verify the selected relation still exists.")
        createLog(
            id=rel_ids,
            type="relation",
            log=[f"deleted category MERGING property {MERGING_property} for {CMID}"] * len(rel_ids),
            user=user,
            driver=driver,
        )
    else:
        raise ValueError("Action must be add, edit, or delete.")

    return "done"


def moveCATEGORYMERGINGties(database, user, input):
    driver = getDriver(database)
    CMID_from = (input.get('s1_2') or "").strip()
    CMID_to = (input.get('s1_3') or "").strip()
    selected_relation = input.get('s1_7')

    if CMID_from == CMID_to:
        raise ValueError("Both CMIDs cannot be the same.")

    try:
        selected_relation = json.loads(selected_relation)
    except Exception:
        raise ValueError("Invalid category MERGING tie payload.")

    rel_id = selected_relation[1].get("id")
    safe_rel_id = sanitize_cypher_element_id(rel_id, "relationship elementId")

    query_move_rel = """
        MATCH (from:DATASET)-[r:MERGING]->(oldTo:CATEGORY {CMID: $CMID_from})
        WHERE elementId(r) = $rel_id
        MATCH (newTo:CATEGORY {CMID: $CMID_to})
        WITH from, oldTo, r, newTo, properties(r) AS relProps
        CREATE (from)-[newR:MERGING]->(newTo)
        SET newR = relProps
        WITH r, newR, oldTo
        DELETE r
        RETURN elementId(newR) AS relID, oldTo.CMID AS oldToCMID
    """
    move_result = getQuery(
        query_move_rel,
        driver,
        params={"CMID_from": CMID_from, "rel_id": safe_rel_id, "CMID_to": CMID_to},
    )

    if not move_result:
        raise ValueError("No category MERGING tie was moved. Verify the selected tie and destination CMID.")

    new_rel_id = move_result[0]["relID"]
    old_to_cmid = move_result[0]["oldToCMID"]
    createLog(
        id=[new_rel_id],
        type="relation",
        log=[f"moved category MERGING tie from {old_to_cmid} to {CMID_to}"],
        user=user,
        driver=driver,
    )

    return "done"

############################
#section for add, edit, delete node ties
#Function that allows editing Node properties.
def add_edit_delete_Node(database,user,input):
    changeNodeID = input.get('s1_2')
    changeNodeProperty = input.get('s1_7')
    changeNodeValue = (input.get('s1_3') or "").strip()
    addOrEditNode = input.get('s1_1')

    driver = getDriver(database)

    metaTypes = getPropertiesMetadata(driver)
    property_meta_types = [
        item.get("metaType")
        for item in metaTypes
        if item.get("type") == "node" and item.get("property") == changeNodeProperty
    ]
    is_list_meta = any(
        isinstance(meta_type, str) and "list" in meta_type.lower()
        for meta_type in property_meta_types
    )
    list_value = None

    if is_list_meta:
        list_value = _split_admin_multi_value(changeNodeValue)

    if changeNodeProperty == "parent":
        list_value = _split_admin_multi_value(changeNodeValue)
        validatePropertyCMID(list_value,changeNodeProperty,"DATASET",driver)
                
    if changeNodeProperty == "District":
        list_value = _split_admin_multi_value(changeNodeValue)
        validatePropertyCMID(list_value,changeNodeProperty,"AREA",driver)
    
    if changeNodeProperty == "glottocode":
        node_summary = getNodeMergeSummary(changeNodeID, driver)
        if "LANGUOID" not in node_summary.get("labels", []):
            raise Exception("Only nodes with a LANGUOID label can have a glottocode property")
    

    if not changeNodeID or not addOrEditNode:
        return "CMID is empty or choice of add/edit/delete is empty"
    
    label = _editable_node_label(changeNodeID, driver)

    # Get prior value
    priorValQuery = f"""
        MATCH (a {{CMID: '{changeNodeID}'}})
        RETURN a.{changeNodeProperty} AS val
    """
    #priorVal = CMCypherQuery(con=con, query=priorValQuery)
    priorVal = getQuery(priorValQuery,driver=driver,type='list')

    if addOrEditNode == "delete":
        if label == "DATASET" and changeNodeProperty in ["District", "parent"]:
            if changeNodeProperty == "District":
                q = f"""
                    MATCH (a {{CMID: '{changeNodeID}'}})<-[r:AREA_OF]-(c:AREA)
                    DELETE r
                """
                #CMCypherQuery(con=con, query=q)
                getQuery(q,driver=driver)
            if changeNodeProperty == "parent":
                q = f"""
                    MATCH (a {{CMID: '{changeNodeID}'}})<-[r:CONTAINS]-(c:DATASET)
                    DELETE r
                """
                #CMCypherQuery(con=con, query=q)
                getQuery(q,driver=driver)

        q = f"""
            MATCH (a {{CMID: '{changeNodeID}'}})
            SET a.{changeNodeProperty} = NULL
        """
        #CMCypherQuery(con=con, query=q)
        getQuery(q,driver=driver)

    else:  # edit or add
        if label == "DATASET" and changeNodeProperty in ["District", "parent"]:
            q = f"""
                MATCH (a {{CMID: '{changeNodeID}'}})
                SET a.{changeNodeProperty} = $id
            """
            #CMCypherQuery(con=con, query=q, parameters={'id': changeNodeValue})
            getQuery(q,driver=driver,params={"id": list_value or _split_admin_multi_value(changeNodeValue)})

            processDATASETs(database,CMID=changeNodeID,user=user)
        else:
            q = f"""
                MATCH (a {{CMID: '{changeNodeID}'}})
                SET a.{changeNodeProperty} = $id
            """
            #CMCypherQuery(con=con, query=q, parameters={'id': changeNodeValue})
            getQuery(q,driver=driver,params={"id": list_value if is_list_meta else changeNodeValue})

            if changeNodeProperty == "CMName":
                try:
                    #CMaddCMNameRel(CMID=changeNodeID, user=user, con=con)
                    addCMNameRel(database,CMID=changeNodeID)
                except Exception as e:
                    print(f"CMaddCMNameRel failed: {e}")

    new_val = "NULL" if addOrEditNode == "delete" else changeNodeValue
    log_msg = f"updated CMID {changeNodeID} {changeNodeProperty} from {priorVal} to {new_val}"
    #CMlog(id=changeNodeID, type="node", log=log_msg, user=user, con=con)
    return "updated successfully"

############################
#section for merging nodes
# when a node is merged, this finds all instances of that CMID in other
# USES tie properties and edits them to reflect the change
def replaceProperty(cmid, property, old, new, database, datasetID = None, Key = None):
    """
    Replace a specified property value in relationships for a given CMID.

    Parameters:
    - cmid: The CMID for which the property replacement should occur.
    - datasetID: The ID of the dataset to be used.
    - Key: The key to identify the relationship.
    - property: The name of the property to be replaced.
    - old: The old value to be replaced.
    - new: The new value to replace the old value.
    - driver: Neo4j driver for database interaction.

    Returns:
    - str: A completion message indicating the success of the property replacement.

    Raises:
    - Exception: In case of any unexpected errors during property replacement.
    """
    try:
        driver = getDriver(database)
        if datasetID is None and Key is None:
            query = f"""
            unwind $cmid as cmid
            match (:CATEGORY {{CMID: cmid}})<-[r:USES]-(:DATASET) 
            where not r.{property} is null
            with r, [i in r.{property} | case when i = $old then $new else i end] as prop
            set r.{property} = prop
            """
        else:
            if len(cmid) > 0:
                raise Exception("cmid must be a single value, not a list")
            query = f"""
            match (:CATEGORY {{CMID: $cmid}})<-[r:USES {{Key: $key}}]-(:DATASET {{CMID: $datasetID}}) 
            where not r.{property} is null
            with r, [i in r.{property} | case when i = $old then $new else i end] as prop
            set r.{property} = prop
            """
        getQuery(query, driver, params={
            "cmid": cmid,
            "datasetID": datasetID,
            "key": Key,
            "old": old,
            "new": new
        })
        return f"Completed {cmid} property {property}"
    except Exception as e:
        return str(e), 500

#Main function for merging nodes
def mergeNodes(keepcmid,deletecmid,user,database):
    """
    Merges nodes in a Neo4j database based on specified CMIDs.

    Parameters:
    - request: Flask request object containing 'keepcmid' and 'deletecmid' as query parameters.
    - driver: Neo4j driver for database interaction.

    Returns:
    - str: A completion message indicating the success of the operation.

    Raises:
    - Exception: If invalid CMIDs are provided or in case of any unexpected errors.
    """
    try:

        if keepcmid == deletecmid:
            raise Exception(f"keepcmid and deletecmid cannot be the same")
        
        driver = getDriver(database)

        results = [f"Started Combining {deletecmid} into {keepcmid}"]

        keep_summary = getNodeMergeSummary(keepcmid, driver)
        delete_summary = getNodeMergeSummary(deletecmid, driver)
        results = results + [
            "checking if keepcmid is valid", [keep_summary.get("CMID")],
            "checking if deletecmid is valid", [delete_summary.get("CMID")],
        ]

        keep_label = keep_summary["primaryDomain"]
        delete_label = delete_summary["primaryDomain"]
        if keep_label != delete_label:
            raise Exception(
                f"Primary domain mismatch. "
                f"Keep {keepcmid} ({keep_summary.get('CMName')}) is {keep_label}; "
                f"Delete {deletecmid} ({delete_summary.get('CMName')}) is {delete_label}."
            )
        
        results = results + [addCMNameRel(database, keepcmid)]
        results = results + [addCMNameRel(database, deletecmid)]

        # get EC relID
        query = """
        unwind $cmid as cmid match (c:CATEGORY {CMID: cmid})<-[r:USES]-(d:DATASET {CMID: "SD11"}) return elementId(r) as relID
            """

        relID = getQuery(query, driver, params = {"cmid": keepcmid}, type = "list")

        results = results + ["relID to keep"]
        results = results + relID

        # replace the CMID in the USES relationships
        contextProps = getQuery(
            "match (m:PROPERTY) where m.relationship is not null return m.CMName as property", driver, type = "list")
        contextProps.append("parentContext") 

        cmids = getQuery(
            "match (:CATEGORY {CMID: $deletecmid})-[rel]->(c:CATEGORY) return c.CMID as cmid", driver, params={
                "deletecmid": deletecmid
            },
            type="list"
        )

        for property in contextProps:
            results = results + [f"updating {property} with new CMID"]
            replaceProperty(cmid=cmids, property=property,
                            old=deletecmid, new=keepcmid, database=database)

        # determine the merge label from the resolved primary domain
        if len(keepcmid) > 1 and keepcmid[1] == "D":
            domain = "DATASET"
        elif keep_label == "VARIABLE":
            domain = "VARIABLE"
        else:
            domain = "CATEGORY"
        domain = sanitize_cypher_identifier(domain, "domain")
        
        if domain == "CATEGORY":
            query = f"""
            MATCH (c:DATASET)-[e1:MERGING]->(k:CATEGORY {{CMID: $keepcmid}})
            MATCH (c)-[e2:MERGING]->(d:CATEGORY {{CMID: $deletecmid}})
            WHERE e1.stack     = e2.stack
            AND e1.Key       = e2.Key

            WITH e2

            DELETE e2
            """

            getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid)

            query = f"""
            MATCH (c:DATASET)-[e1:MERGING]->(k:CATEGORY {{CMID: $keepcmid}})
            MATCH (c)-[e2:MERGING]->(d:CATEGORY {{CMID: $deletecmid}})
            WHERE e1.stack     = e2.stack
            AND e1.Key       = e2.Key

            WITH  e2

            DELETE e2
            """

            getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid)
        
        # combine the nodes

        query = f"""
        match (a:{domain} {{CMID: $keepcmid}})
        match (b:{domain} {{CMID: $deletecmid}})
        WITH collect(a) + collect(b) AS nodes
        CALL apoc.refactor.mergeNodes(nodes,{{properties: {{
        CMID:'discard',
        CMName:'discard',
        `.*`: 'combine'}} }})
        YIELD node
        return node.CMID as CMID
        """

        merged = getQuery(query, driver, keepcmid=keepcmid, deletecmid=deletecmid, type = "list")
        if not keepcmid in merged:
            raise Exception(f"Failed to merge {deletecmid} into {keepcmid}")

        # create deleted node and "IS" relationship to remaining node
        # Preserve the old CMName on the DELETED marker node.
        delete_cmname = delete_summary.get("CMName")
        query = f"""
        unwind $keepcmid as keepcmid 
        unwind $deletecmid as deletecmid 
        match (new:{domain} {{CMID: keepcmid}}) 
        create (del:DELETED {{CMID: deletecmid}}) 
        set del.CMName = $deletecmname
        with new, del
        create (del)-[:IS]->(new)
        return elementId(del) as delID
        """

        delID = getQuery(
            query=query,
            driver=driver,
            keepcmid=keepcmid,
            deletecmid=deletecmid,
            deletecmname=delete_cmname,
            type="list",
        )

        createLog(id=delID, type="node",
                  log=f"deleted {deletecmid} and merged into {keepcmid}", user=user, driver=driver)

        # combine EC USES ties

        if len(relID) > 0:
            # this query does a merge with errors such as turning lists into strings
            query = """
            unwind $cmid as cmid 
            match (:DATASET {CMID: "SD11"})-[r:USES]->({CMID: cmid}) 
            with collect(r) as rels

            WITH CASE
                    WHEN elementId(head(rels)) IN $relID THEN rels
                    ELSE reverse(rels)
                END AS relIDfirst

            MATCH (p:PROPERTY) 
            WHERE p.type = "relationship" 
                AND p.metaType = "list" 
                AND p.CMName <> "language"
            WITH relIDfirst, collect(p.CMName) AS listProps

            MATCH (p:PROPERTY) 
            WHERE p.type = "relationship" 
                AND p.metaType = "string" 
            WITH relIDfirst, listProps, collect(p.CMName) AS stringProps

            WITH relIDfirst,
                [prop IN listProps | [prop,'combine']] +
                    [prop IN stringProps | [prop,'retain']] +
                    [['language','retain']] AS allProps,listProps+['language'] AS listPropsPluslanguage
            
            WITH relIDfirst, apoc.map.fromPairs(allProps) AS props,listPropsPluslanguage

            call apoc.refactor.mergeRelationships(relIDfirst,{properties: props,singleElementAsArray: listPropsPluslanguage}) yield rel 
            
            RETURN count(*) AS mergedCount
            """

            getQuery(query = query, driver = driver, cmid=keepcmid, relID=relID)

        # need to update USES ties

        id = getQuery(
            "unwind $keepcmid as cmid match (n {CMID: cmid}) return elementId(n) as id", driver = driver, keepcmid=keepcmid, type = "list")
        results = results + ["id is:", id]
        createLog(id=id, type="node",
                  log=f"merged {deletecmid} into {keepcmid}", user=user, driver=driver)

        processUSES(database = database, CMID=keepcmid, user="0")

        results = results + \
            [f"Completed combining {deletecmid} into {keepcmid}"]
        
        return results

    except Exception as e:
        return str(e), 500
    
############################
#section for moving USES ties

def USESLogText(relid, driver):
    """
    Creates a custom log text for the uses tie that is to be moved.

    Parameters:
    - relid: str - relationshipID
    - driver: Neo4j session
    
    Returns:
    - pandas DataFrame with columns: logtext
    """
    query = """
    UNWIND $relid AS relid
    MATCH (a)-[r:USES]->(b)
    WHERE elementId(r) = relid
    RETURN coalesce(a.CMName,'NA') + '-' + type(r) + '-' + coalesce(r.Key,'') + '->' + coalesce(b.CMName,'NA') AS logtext
    """
    
    result = getQuery(query,driver, params={"relid": relid})
    logtext = result[0]["logtext"]
    
    return logtext

#When moving USES tie U from dataset D from category node A to B, if U defines contextual children from A to C and there are multiple uses ties from D to A, 
#this creates ambiguity in whether C should be a child of A or B.
#This function detects that issue and leads to the user being prompted to make decisions about this ambiguity.
def check_ambiguous_ties_moveUSESties(driver,CMID_from,CMID_to,rel_id):
    
    try:
        #checks to see if CMID is valid
        validCMID_to = isValidCMID(CMID_to, driver)

        if CMID_from == CMID_to:
            raise Exception(f"Both CMIDs cannot be the same.")

        if len(validCMID_to) == 0:
            raise Exception(f"{CMID_to} is invalid")
        
        # gets labels of uses tie using relID, then gets groupLabel of the label and returns it.
        query = """
                MATCH ()-[r:USES]->()
                WHERE elementId(r) = $relID
                WITH r.label AS label
                MATCH (m:LABEL {CMName: label})
                RETURN m.groupLabel as groupLabel
                """

        uses_label = getQuery(query,driver,params = {'relID': rel_id})

        if uses_label:
            uses_label = uses_label[0]['groupLabel']
        else:
            return "No label found for this USES tie."
        
        #checks to see if uses tie labels is consistent with label of destination node
        to_label = getGroupLabels(CMID_to,driver)
        #from_label = getGroupLabels(uses_label,driver)

        if to_label != uses_label:
            raise Exception(f"The CMIDs are not of the same group label.")
        
        # 1. Get dataset CMID linked to the relID
        query_dataset = """
        UNWIND $relID AS relID
        MATCH (d:DATASET)-[r:USES]->(:CATEGORY)
        WHERE elementId(r) = relID
        RETURN d.CMID AS datasetID
        """
        dataset_df = getQuery(query_dataset,driver,params = {'relID': rel_id})

        if dataset_df:
            dataset = dataset_df[0]['datasetID']
        else:
            return "No Dataset found for this USES tie."
        
        #checks if there are multiple uses ties from the same Dataset d to the from node p
        query_check_for_multiple_uses_ties = """
        UNWIND $fromCMID AS fromCMID
        UNWIND $dataset AS dataset
        MATCH (p:CATEGORY {CMID: fromCMID})<-[r:USES]-(d:DATASET {CMID: dataset})
        Return count(r) as uses_count
        """

        uses_count = getQuery(query_check_for_multiple_uses_ties,driver,params= {'fromCMID': [CMID_from], 'dataset': [dataset]})
        uses_count = uses_count[0]['uses_count']

        # do any contextual children of fromNode A have a USES tie from D that includes A as a property
        query = """
            UNWIND $fromCMID AS fromCMID
            UNWIND $dataset AS dataset
            MATCH (p:PROPERTY)
            WHERE p.relationship IS NOT NULL
            WITH collect(p.CMName) AS prop_CMNames,fromCMID,dataset

            MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET {CMID: dataset})
            WHERE any(k IN prop_CMNames WHERE fromCMID IN r[k])
            RETURN c.CMID as CMID, r.Key as Key
            """

        child_USES_check = getQuery(query,driver,params= {'fromCMID': [CMID_from], 'dataset': [dataset]})

        if uses_count > 1 and child_USES_check:
            return jsonify({
                "status" : "True",
                "dataset": dataset,
                "child_USES_check": child_USES_check
            })
        else:
            return jsonify({"status" : "False",
                            "dataset": dataset,
                            "child_USES_check": child_USES_check})
    except Exception as e:
        return {"error": str(e)},500

# table data includes user decisions about ambiguous parents
#Function that moves uses tie from one category node to another category node
def moveUSESties(database,user,input,dataset,tabledata):
    driver = getDriver(database)
    CMID_from = input.get('s1_2').strip()
    CMID_to = input.get('s1_3').strip()
    USES_property = json.loads(input.get('s1_7'))
    rel_id = USES_property[1]["id"]
    # only need to revise operation if user wants to keep some parent-child ties with the FROM node.
    USES_to_change = [row for row in tabledata if row['optionA'] != 'From']
    
    if len(USES_to_change) > 0:

        try:
            query = """
                MATCH (d1)-[r:USES]->(c1)
                WHERE c1.CMID = $CMID_from AND elementId(r) = $rel_id

                WITH c1, d1, r.Key as Key, d1.CMID as datasetID

                OPTIONAL MATCH (s:STACK)-[:MERGING]->(d1)
                WHERE d1.CMID = datasetID

                WITH c1, d1, Key, datasetID, s.CMID as stackID

                OPTIONAL MATCH (d1)-[e:MERGING]->(c1)
                WHERE e.stack = stackID AND e.Key = Key

                WITH c1, d1, e, $CMID_to AS c2CMID
                WHERE e IS NOT NULL

                MATCH (c2:CATEGORY)
                WHERE c2.CMID = c2CMID
                CREATE (d1)-[newEq:MERGING {stack: e.stack, Key: e.Key}]->(c2)
                DELETE e
                RETURN 'Moved category merging tie', c1.CMID AS oldCategory, c2.CMID AS newCategory
                """
                    
            result = getQuery(query,driver,params = {"CMID_from": CMID_from, "rel_id": rel_id, "CMID_to": CMID_to})
            
        except Exception as e:
            return "Error occurred while moving category merging ties."
        
        try:
            query_update_parents = """
            UNWIND $changes AS change
            MATCH (c {CMID: change.cmid})<-[r:USES {Key: change.Key}]-(d:DATASET {CMID: $dataset})
            WITH c, d, r, $old AS old, $new AS new

            WITH c, d, r, old, new, [x IN r.parentContext WHERE x IS NOT NULL | 
                CASE 
                    WHEN apoc.convert.fromJsonMap(x).parent = old 
                    THEN apoc.convert.toJson(apoc.map.setKey(apoc.convert.fromJsonMap(x), 'parent', new))
                    ELSE x 
                END
            ] AS updatedParentContext
            SET r.parentContext = updatedParentContext

            WITH c, d, r, old, new

            MATCH (p:PROPERTY) WHERE p.relationship IS NOT NULL
            WITH c, d, r,old,new, collect(p.CMName) AS prop_CMNames

            UNWIND prop_CMNames AS propName
            WITH c, d, r, old, new, propName
            WHERE r[propName] IS NOT NULL
            SET r[propName] = [element IN r[propName] | CASE WHEN element = old THEN new ELSE element END]

            RETURN c.CMID AS CMID, r, d.CMID AS datasetID
            """
            
            result = getQuery(query_update_parents,driver,params = {
                'changes': [{"cmid": row['CMID'], "Key": row['Key']} for row in USES_to_change],
                'old': CMID_from,
                'new': CMID_to,
                'dataset': dataset,
            })

            print("completed moving props")

            processUSES(CMID=[row['CMID']for row in USES_to_change], database=database)
        except Exception as e:
            return "Error occurred while moving USES ties."
    
    # Move the relationship itself
    # Fetch relationship details for log
    try:
        logtext = USESLogText(rel_id,driver)
        
        log_msg = f"moved relationship {logtext} from {CMID_from} to {CMID_to}"

        safe_rel_id = sanitize_cypher_element_id(rel_id, "relationship elementId")
        query_move_rel = """
        MATCH ()-[r:USES]->(from)
        WHERE from.CMID = $CMID_from AND elementId(r) = $rel_id
        MATCH (to)
        WHERE to.CMID = $CMID_to
        CALL apoc.refactor.to(r, to) YIELD input, output
        RETURN elementId(output) AS relID
        """
        rel_id_df = getQuery(
            query_move_rel,
            driver,
            params={"CMID_from": CMID_from, "rel_id": safe_rel_id, "CMID_to": CMID_to},
        )
        new_rel_id = rel_id_df[0]['relID'] if rel_id_df else None

        #Logging
        from_node_id = getID(CMID_from, "CMID", driver)
        to_node_id = getID(CMID_to, "CMID", driver)

        #CMlog(id=from_node_id, type_="node", log=log_msg, user=user, con=con)
        #CMlog(id=to_node_id, type_="node", log=log_msg, user=user, con=con)

    
        if new_rel_id is not None:
            createLog(id=new_rel_id, type="relation", log=log_msg, user=user, driver=driver)
    except Exception as e:
        return f"Warning while logging relation: {e}"
            
    # Final updates and notifications
    print("move completed: updating USES ties")
    processUSES(CMID=[CMID_from, CMID_to], database=database)

    print("Completed USES ties update")

    return "done"

############################
#section for deleting a Node

# Function used by deleteNode to get the label of the node to differentiate b/w DATASET or not
# only returns CATGEORY or DATASET
def getLabel(CMID, driver, filter=True):
    # Run Cypher query to get labels
    query = """
        UNWIND $CMID AS cmid 
        MATCH (a) WHERE a.CMID = cmid 
        UNWIND labels(a) AS label 
        RETURN label
    """
    result = getQuery(query=query, params={"CMID": CMID}, driver=driver)
    
    # Sort labels alphabetically
    labels = sorted([row["label"] for row in result])

    if filter:
        # Get groupLabel metadata
        grpLabels = getLabelsMetadata(driver=driver)
        grpLabels = list(set(row["groupLabel"] for row in grpLabels if row["groupLabel"] is not None))

        # Filter out group labels
        resultF = [label for label in labels if label not in grpLabels]
        if resultF:
            labels = resultF

        # Filter out "CATEGORY"
        resultF = [label for label in labels if label != "CATEGORY"]
        if resultF:
            labels = resultF

    return labels

# Helper function which does the delete operation for delete Node
def deleteID(id_value, driver, type="node"):
    # Validate and coerce input to integer or list of integers
    if isinstance(id_value, str):
        id_list = [id_value]
    elif isinstance(id_value, list):
        try:
            id_list = [str(i) for i in id_value]
        except (ValueError, TypeError):
            raise ValueError("id should be an string or list of strings")
    else:
        raise ValueError("id should be an string or list of strings")

    if type not in ["relationship", "node"]:
        raise ValueError("type should be 'relationship' or 'node'")

    count_deleted = 0
    queries = []

    if type == "relationship":
        for id in id_list:
            safe_id = sanitize_cypher_element_id(id, "relationship elementId")
            queries.append((
                "MATCH ()-[r]->() WHERE elementId(r) = $id DELETE r RETURN count(*) AS count",
                {"id": safe_id},
            ))
    else:  # type == "node"
        for id in id_list:
            safe_id = sanitize_cypher_element_id(id, "node elementId")
            queries.append((
                "MATCH (a) WHERE elementId(a) = $id DETACH DELETE a RETURN count(*) AS count",
                {"id": safe_id},
            ))

    for query, params in queries:
        result = getQuery(query, driver=driver, params=params)
        if result and "count" in result[0]:
            count_deleted += result[0]["count"]

    return f"Deleted {count_deleted} of type {type}"

# Function that deletes node, cleans up properties with that CMID, and re-assigns the node the DELETED label
def deleteNode(database,user,input):
    driver = getDriver(database)

    try:
        label = getLabel(input.get('s1_2'),driver,filter=True)
        
        # If you delete a dataset node, need to remove it’s CMID from all parent properties 
        # in other dataset nodes and Dataset in USES ties
        if "DATASET" in label:

            query = """
                    unwind $cmid as cmid
                    MATCH (:DATASET {CMID: cmid})-[r:MERGING]->(:CATEGORY)
                    RETURN DISTINCT r.stack AS stackID
                    """

            result = getQuery(query, driver=driver, params={"cmid": input.get('s1_2')})

            if result:
                raise ValueError(f"this dataset can't be deleted, because it is used by an existing stack for a merge(stackID={result}).")

            # if a stack node is being deleted, remove category merging ties scoped to that stack.
            if "STACK" in label:
                query = """
                    unwind $cmid as cmid
                    MATCH (:DATASET)-[r:MERGING]->(:CATEGORY)
                    WHERE r.stack = $cmid
                    DELETE r
                    """

                getQuery(query, driver=driver, params={"cmid": input.get('s1_2')})

            ids_query = """
                MATCH (:DATASET)-[r:USES]->(:CATEGORY)
                WHERE $cmid IN r.Dataset
                WITH r, [i IN r.Dataset WHERE NOT i = $cmid] AS prop
                SET r.Dataset = prop
                RETURN elementId(r) AS ids
            """

            ids = getQuery(ids_query,driver=driver, params={"cmid": input.get('s1_2')})

            if len(ids) > 0:

                cleanup_query = """
                    UNWIND $ids AS id
                    MATCH (:DATASET)-[r:USES]->(:CATEGORY)
                    WHERE elementId(r) = id AND size(r.Dataset) = 0
                    SET r.Dataset = NULL
                """
                getQuery(cleanup_query,driver=driver,params={"ids": [row['ids'] for row in ids]})
                createLog(id=[row['ids'] for row in ids], type="relation",
                      log=f"removed reference to deleted node {input.get('s1_2')} from Dataset property",
                      user=user, driver=driver)
            
            # removing CMID for deleted node from parent property in dataset nodes
            datasetIDs_query = """
                MATCH (d:DATASET)
                WHERE $cmid IN d.parent
                RETURN d.CMID AS ids
            """
            datasetIDs = getQuery(datasetIDs_query,driver=driver, params={"cmid": input.get('s1_2')})

            if len(datasetIDs) > 0:
                for prop in ["parent"]:
                    safe_prop = sanitize_cypher_identifier(prop, "property")
                    ids_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id
                        WITH d, [i IN d.{safe_prop} WHERE NOT i = $cmid] AS p
                        SET d.{safe_prop} = p
                        RETURN d.CMID AS ids
                    """
                    ids = getQuery(ids_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs], "cmid": input.get('s1_2')})
                    nullify_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id AND size(d.{safe_prop}) = 0
                        SET d.{safe_prop} = NULL
                    """
                    getQuery(nullify_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs]})
                    createLog(id=[row['ids'] for row in ids], type="node",
                          log=f"removed reference to deleted node {input.get('s1_2')} from {prop}",
                          user=user, driver=driver)

        # If you delete a category node, need to remove it’s CMID from all properties (including parentContext) 
        # in all USES ties (district, country, parent, language, culture….) and 
        # from dataset nodes (District)
        else:
            props = getPropertiesMetadata(driver=driver)
            props = list(set([p['property'] for p in props if p['relationship'] is not None] + ["parentContext"]))

            query = """
                    MATCH (c:CATEGORY)<-[r:USES]-(d:DATASET)
                    WHERE c.CMID = $cmid

                    WITH c.CMID as cmid, r.Key as Key, d.CMID as datasetID

                    MATCH (:DATASET {CMID: datasetID})-[e:MERGING]->(:CATEGORY {CMID: cmid})
                    WHERE e.Key = Key
                    
                    RETURN DISTINCT
                    e.stack as stackID
                    """

            result = getQuery(query, driver=driver, params={"cmid": input.get('s1_2')})

            if result:
                raise ValueError(f"This category can't be deleted, because it is used by an existing stack for a merge(stackID={result}).")

            print("1")

            rels_query = """
                UNWIND $keys AS key
                MATCH (d:DATASET)-[r:USES]->(c:CATEGORY)
                WITH key, d, c, $cmid AS cmid, r
                WHERE r[key] IS NOT NULL AND (
                    toString(cmid) IN r[key] OR 
                    (r.parentContext IS NOT NULL AND ANY(i IN r.parentContext WHERE i CONTAINS '\"parent\":\"' + cmid))
                )
                RETURN elementId(r) AS id, r[key] AS val, cmid, key
            """
            rels = getQuery(rels_query, driver = driver, params={"keys": props, "cmid": input.get('s1_2')})

            datasetIDs_query = f"""
                MATCH (d:DATASET)
                WHERE $cmid IN d.District
                RETURN d.CMID AS ids
            """
            datasetIDs = getQuery(datasetIDs_query,driver=driver,params = {"cmid": input.get("s1_2")})

            # removing CMID for deleted node from District property in dataset nodes
            if len(datasetIDs) > 0:
                for prop in ["District"]:
                    safe_prop = sanitize_cypher_identifier(prop, "property")
                    ids_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id
                        WITH d, [i IN d.{safe_prop} WHERE NOT i = $cmid] AS p
                        SET d.{safe_prop} = p
                        RETURN d.CMID AS ids
                    """
                    ids = getQuery(ids_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs], "cmid": input.get('s1_2')})
                    nullify_query = f"""
                        UNWIND $ids AS id
                        MATCH (d:DATASET) WHERE d.CMID = id AND size(d.{safe_prop}) = 0
                        SET d.{safe_prop} = NULL
                    """
                    getQuery(nullify_query, driver=driver, params={"ids": [row['ids'] for row in datasetIDs]})
                    createLog(id=[row['ids'] for row in ids], type="node",
                          log=f"removed reference to deleted node {input.get('s1_2')} from {prop}",
                          user=user, driver=driver)
                                            
            # getting all the affected relationships and extracting the safe data and setting it back
            if len(rels) > 0:

                sepRels = []
                for row in rels:
                    vals = row['val']
                    if isinstance(vals, list):
                        vals = vals  # already a list
                    else:
                        vals = re.split(r' \|\|', vals)  # split string

                    for val in vals:
                        val = val.strip()
                        if input.get('s1_2') not in val:
                            sepRels.append({"id": row["id"], "key": row["key"], "val": val})
                    # for val in re.split(r' \|\|', row['val']):
                    # #for val in row['val']:
                    #     val = val.strip()
                    #     if input.get('s1_2') not in val:
                    #         sepRels.append({"id": row["id"], "key": row["key"], "val": val})
                                
                # if there's saved data, it is set back before removing the purely unsaved data
                if len(sepRels) > 0:
                    grouped = {}
                    for r in sepRels:
                        grouped.setdefault((r['id'], r['key']), []).append(r['val'])

                    for (id_val, key), vals in grouped.items():
                        safe_id_val = sanitize_cypher_element_id(id_val, "relationship elementId")
                        safe_key = sanitize_cypher_identifier(key, "property")
                        set_query = f"""
                            MATCH (:DATASET)-[r:USES]->(:CATEGORY) WHERE elementId(r) = $id
                            SET r.{safe_key} = $vals
                        """
                        getQuery(set_query,driver=driver, params={"id": safe_id_val, "vals": vals})
                        nullify_empty_query = f"""
                            MATCH (:DATASET)-[r:USES]->(:CATEGORY) WHERE elementId(r) = $id AND size(r.{safe_key}) = 0
                            SET r.{safe_key} = NULL
                        """
                        getQuery(nullify_empty_query,driver=driver, params={"id": safe_id_val})
                # removing the purely unsaved data
                else:
                    for row in rels:
                        safe_row_id = sanitize_cypher_element_id(row["id"], "relationship elementId")
                        safe_row_key = sanitize_cypher_identifier(row["key"], "property")
                        nullify_query = f"""
                            MATCH (:DATASET)-[r:USES]->(:CATEGORY) WHERE elementId(r) = $id
                            SET r.{safe_row_key} = NULL
                        """
                        getQuery(nullify_query,driver=driver, params={"id": safe_row_id})

                createLog(id=[row["id"] for row in rels], type="relation",
                      log=f"removed reference to deleted node {input.get('s1_2')}",
                      user=user, driver=driver)
                
        nodeID = getID(input.get('s1_2'), "CMID", driver)
        safe_node_id = sanitize_cypher_element_id(nodeID, "node elementId")
        create_deleted_query = """
            MATCH (n) WHERE elementId(n) = $nodeID
            CREATE (n2:DELETED)
            SET n2.CMID = n.CMID, n2.CMName = n.CMName, n2.log = n.log
            RETURN elementId(n2) AS nodeID
        """
        deletedID = getQuery(create_deleted_query,driver=driver, params={"nodeID": safe_node_id})
        print(deletedID)
        print("Stgae 3")
        createLog(id=[deletedID[0]['nodeID']], type="node",
              log=[f"deleted node {input.get('s1_2')}"],
              user=user, driver=driver)

        deleteID(nodeID,driver,type="node")
        print("deleted node")
        return "done"

    except Exception as e:
        print(f"error deleting node: {str(e)}")

############################
#section for deleting a USES tie
def deleteUSES(database,user,input):
    driver = getDriver(database)
    CMID = input.get('s1_2')
    USES_property = json.loads(input.get('s1_7'))
    rel_id = sanitize_cypher_element_id(USES_property[1]["id"], "relationship elementId")

    q = """MATCH (a)-[r:USES]->(d:DATASET)
            WHERE elementId(r) = $id

            WITH d.CMID as datasetID,a.CMID as CMID, r.Key as Key
            OPTIONAL MATCH (d)-[e:MERGING]->(a)
            WHERE e.Key = Key
            WITH CMID, Key, datasetID, e.stack as stackID

            WHERE e IS NOT NULL

            RETURN 
            CMID,
            datasetID,
            Key,
            stackID
            """
    
    result = getQuery(q, driver=driver, params={"id": rel_id})

    if result:
        raise ValueError(f"There is a stack that uses this USES tie for a merging template. It is not possible to delete this USES tie using admin functions.(stackID = {result})")

    q = "MATCH ()-[r]->() WHERE elementId(r) = $id DELETE r RETURN count(*) AS count"
    result = getQuery(q, driver=driver, params={"id": rel_id})

    processUSES(database,CMID)

    return "done"


def deleteCATEGORYMERGING(database, user, input):
    driver = getDriver(database)
    selected_tie = input.get('s1_7')

    try:
        selected_tie = json.loads(selected_tie)
    except Exception:
        raise ValueError("Invalid category MERGING tie payload.")

    rel_id = sanitize_cypher_element_id(selected_tie[1]["id"], "relationship elementId")
    q = "MATCH (:DATASET)-[r:MERGING]->(:CATEGORY) WHERE elementId(r) = $id DELETE r RETURN count(*) AS count"
    result = getQuery(q, driver=driver, params={"id": rel_id})

    deleted_count = result[0]["count"] if result and "count" in result[0] else 0
    if deleted_count == 0:
        raise ValueError("No category MERGING tie was deleted. Verify the selected relation still exists.")

    return "done"

############################
#section for creating a new label
def createLabel(database,user,input):
    driver = getDriver(database)

    label_name = str(input.get('s1_2', "")).strip()
    group_label_input = str(input.get('s1_7', "")).strip()
    relationship_value = str(input.get('s1_3', "")).strip()
    description = str(input.get('s1_4', "")).strip()
    display_name = str(input.get('s1_5', "")).strip()
    color = str(input.get('s1_6', "")).strip() or "#404040"

    q = "MATCH (n:LABEL) WHERE n.CMName = $label_name RETURN n.CMName as CMName"

    result = getQuery(q,driver=driver, params={"label_name": label_name})

    #returns error if label name already exists
    if result != []:
        return "Label name already exists"
    
    q = 'MATCH (n:LABEL) WHERE n.CMID STARTS WITH "CL" WITH n, toInteger(replace(n.CMID, "CL", "")) AS numericID RETURN numericID ORDER BY numericID DESC LIMIT 1'

    result = getQuery(q,driver=driver)

    CMID = "CL" + str(result[0]['numericID']+1)

    if group_label_input == "NA":
        grouplabel = label_name
        displayOrder = 4
    else:
        grouplabel = group_label_input
        displayOrder = 100

    create_params = {
        "CMID": CMID,
        "CMName": label_name,
        "groupLabel": grouplabel,
        "description": description,
        "displayName": display_name,
        "color": color,
        "label": display_name,
        "displayOrder": str(displayOrder),
    }

    if relationship_value != "":
        create_params["relationship"] = relationship_value
        q = """
        CREATE (n:METADATA:LABEL {
            CMID: $CMID,
            CMName: $CMName,
            groupLabel: $groupLabel,
            relationship: $relationship,
            description: $description,
            displayName: $displayName,
            color: $color,
            label: $label,
            displayOrder: $displayOrder,
            public: "TRUE"
        })
        """
    else:
        q = """
        CREATE (n:METADATA:LABEL {
            CMID: $CMID,
            CMName: $CMName,
            groupLabel: $groupLabel,
            description: $description,
            displayName: $displayName,
            color: $color,
            label: $label,
            displayOrder: $displayOrder,
            public: "TRUE"
        })
        """

    result = getQuery(q,driver=driver, params=create_params)

    # create index after creation

    q =    """UNWIND $label_name as label_name
            match (d:METADATA:LABEL)
            where (d.public = true or tolower(d.public) = "true") AND d.CMName= label_name
            with d.label as l
            call apoc.cypher.runSchema('CREATE FULLTEXT INDEX ' + l + ' IF NOT EXISTS FOR (n:' + l + ') ON EACH [n.names]',{}) yield value return count(*)"""

    result = getQuery(q,driver=driver,params={"label_name": display_name})

    return "done"


def _uses_value_key(value):
    if isinstance(value, list):
        return tuple(_uses_value_key(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted((key, _uses_value_key(val)) for key, val in value.items()))
    return value


def _dedupe_uses_values(values):
    deduped = []
    seen = set()
    for value in values:
        key = repr(_uses_value_key(value))
        if key not in seen:
            seen.add(key)
            deduped.append(value)
    return deduped


def _format_uses_conflict_value(value):
    if value is None:
        return "NULL"
    return repr(value)


def _uses_conflict_details(conflicts, CMID, Key, datasetID):
    return {
        "CMID": CMID,
        "Key": Key,
        "datasetID": datasetID,
        "conflicts": conflicts,
    }


def _format_uses_conflict_message(error_details):
    row_prefix = (
        "Cannot merge duplicate USES ties for "
        f"CMID {error_details['CMID']}, "
        f"Key {error_details['Key']}, "
        f"datasetID {error_details['datasetID']}."
    )
    conflict_parts = []
    for conflict in error_details.get("conflicts", []):
        values = "; ".join(
            f"{item['relID']}: {_format_uses_conflict_value(item['value'])}"
            for item in conflict.get("values", [])
        )
        conflict_parts.append(f"property {conflict.get('property')} has values [{values}]")
    if conflict_parts:
        return f"{row_prefix} Conflicting scalar properties: " + "; ".join(conflict_parts)
    return row_prefix


def _merge_uses_relationship_properties(relationships, CMID, Key, datasetID):
    props_by_name = {}
    for rel in relationships:
        rel_id = rel.get("relID")
        props = rel.get("props") or {}
        for prop, value in props.items():
            props_by_name.setdefault(prop, []).append({
                "relID": rel_id,
                "value": value,
            })

    merged_props = {}
    conflicts = []
    for prop, entries in props_by_name.items():
        non_null_entries = [entry for entry in entries if entry["value"] is not None]
        if not non_null_entries:
            continue

        values = [entry["value"] for entry in non_null_entries]
        has_list = any(isinstance(value, list) for value in values)
        if has_list:
            combined_values = []
            for value in values:
                if isinstance(value, list):
                    combined_values.extend([item for item in value if item is not None])
                else:
                    combined_values.append(value)
            merged_props[prop] = _dedupe_uses_values(combined_values)
            continue

        unique_values = _dedupe_uses_values(values)
        if len(unique_values) > 1:
            conflicts.append({
                "property": prop,
                "values": [
                    {
                        "relID": entry["relID"],
                        "value": entry["value"],
                    }
                    for entry in non_null_entries
                ],
            })
        else:
            merged_props[prop] = unique_values[0]

    if conflicts:
        error_details = _uses_conflict_details(conflicts, CMID, Key, datasetID)
        error = ValueError(_format_uses_conflict_message(error_details))
        error.details = error_details
        raise error

    return merged_props


def mergeUSESties(database, CMID, Key, datasetID):
    """
    Merge all `USES` relationships between a CATEGORY node and a DATASET node 
    in a Neo4j database while respecting constraints on which properties can 
    or cannot be combined.

    Parameters
    ----------
    database : str
        Name of the Neo4j database to connect to.
    CMID : str
        The unique identifier of the CATEGORY node.
    Key : str
        The key value identifying the specific USES relationships to merge.
    datasetID : str
        The unique identifier of the DATASET node.

    Returns
    -------
    str
        Success message confirming that the USES relationships were merged.

    Raises
    ------
    Exception
        If multiple distinct values are found for a non-combinable property, 
        or if no relationships are found to merge.
    """

    # Obtain a Neo4j driver connection for the specified database.
    driver = getDriver(database)

    existing_query = """
    MATCH (:DATASET {CMID: $datasetID})-[r:USES {Key: $key}]->(:CATEGORY {CMID: $cmid})
    WITH r
    ORDER BY elementId(r)
    RETURN elementId(r) AS relID, properties(r) AS props
    """
    relationships = getQuery(
        existing_query,
        driver=driver,
        params={
            "cmid": CMID,
            "key": Key,
            "datasetID": datasetID
        }
    )

    if len(relationships) < 2:
        raise Exception(
            f"No duplicate USES ties found to merge for CMID {CMID} with Key {Key} in Dataset {datasetID}"
        )

    merged_props = _merge_uses_relationship_properties(relationships, CMID, Key, datasetID)

    merge_query = """
    MATCH (:DATASET {CMID: $datasetID})-[r:USES {Key: $key}]->(:CATEGORY {CMID: $cmid})
    WITH r
    ORDER BY elementId(r)
    WITH collect(r) AS rels
    WITH rels[0] AS keep, rels[1..] AS discard, size(rels) AS originalCount
    SET keep = $mergedProps
    FOREACH (rel IN discard | DELETE rel)
    RETURN elementId(keep) AS relID, originalCount, 1 AS mergedCount
    """
    result = getQuery(
        merge_query,
        driver=driver,
        params={
            "cmid": CMID,
            "key": Key,
            "datasetID": datasetID,
            "mergedProps": merged_props
        }
    )

    return {
        "CMID": CMID,
        "Key": Key,
        "datasetID": datasetID,
        "originalCount": result[0].get("originalCount", len(relationships)),
        "mergedCount": result[0].get("mergedCount", 1),
        "relID": result[0].get("relID"),
    }





############################
#section for potentially deprecating functions
