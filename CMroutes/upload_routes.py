from flask import Blueprint, request, jsonify
import pandas as pd
import json
import os
from CM import input_Nodes_Uses, unlist

upload_bp = Blueprint('upload', __name__)

@upload_bp.route("/uploadInputNodes", methods=['GET', 'POST'])
def upload_API():
    try:
        data = request.get_data()
        data = json.loads(data)
        df = data.get("df")
        database = unlist(data.get("database"))
        formData = unlist(data.get("formData"))
        label = formData.get("subdomain") or formData.get("domain")
        label_upper = str(label).upper() if label is not None else ""
        if label_upper == "ANY DOMAIN":
            label = "CATEGORY"
        if label_upper == "AREA":
            label = "DISTRICT"
        datasetID = formData["datasetID"]
        CMName = formData["cmNameColumn"]
        Name = formData["categoryNamesColumn"]
        altNames = formData["alternateCategoryNamesColumn"]
        CMID = formData["cmidColumn"]
        Key = formData["keyColumn"]

        optionalProperties = data.get("allContext")

        if data.get("addoptions")["district"] == False:
            addDistrict = False
        else:
            addDistrict = True

        if data.get("addoptions")["recordyear"] == False:
            addRecordYear = False
        else:
            addRecordYear = True

        user = data.get("user")
        mergingType = data.get("mergingType")

        if data.get("so") == "standard":

            uploadOption = data.get("ao")

            dfpd = pd.DataFrame(df)
            required = ["CMName", "Name", "CMID",
                        "label", "altNames", "Key", "datasetID"]
            key_cols = {}
            for key in required:
                if key in dfpd.columns.to_list():
                    key_cols[key] = key
                else:
                    key_cols[key] = None

            response, desired_order = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption=uploadOption,
                formatKey=False,
                optionalProperties=optionalProperties,
                user=user,
                addDistrict=addDistrict,
                addRecordYear=addRecordYear,
                mergingType=mergingType,
                geocode=False,
                batchSize=1000)
        else:

            if not label:
                raise Exception("Must specify a domain")
            df = pd.DataFrame(df)
            df['label'] = label
            df['datasetID'] = datasetID
            if not Name in df.columns:
                df['Name'] = df[CMName]
                Name = "Name"
            if not CMID in df.columns:
                df['CMID'] = ""
                CMID = "CMID"
            df.rename(columns={CMName: "CMName", CMID: "CMID", Name: "Name",
                      Key: "Key", altNames: "altNames"}, inplace=True)
            df = df.to_dict(orient='records')
            # return {"Name":Name, "CMID":CMID,"altNames":altNames,"Key":Key,"user":user,"overwriteProperties":overwriteProperties,"updateProperties":updateProperties,"addDistrict":addDistrict,"addRecordYear":addRecordYear}
            response, desired_order = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption="add_uses",
                formatKey=True,
                optionalProperties=optionalProperties,
                user=user,
                addDistrict=False,
                addRecordYear=False,
                geocode=False,
                batchSize=1000)

        if isinstance(response, pd.DataFrame):
            n = len(response)
            response_dict = response.to_dict(orient='records')
            return jsonify({"message": f"Upload completed for {n} row(s)", "file": response_dict, "order": desired_order})
        # else:
        #     return "Error!! Check your file."

    except Exception as e:
        log_file = f'log/{user}uploadProgress.txt'
        full_log = []
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                full_log = file.readlines()
        else:
            full_log.append("Log file not found.")

        response_data = {
            "error": f"Upload error - {str(e)}",
            "full_log": full_log
        }

        return json.dumps(response_data), 500
