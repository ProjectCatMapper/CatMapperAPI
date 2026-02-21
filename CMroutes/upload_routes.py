from flask import Blueprint, request, jsonify
import pandas as pd
import json
import os
from CM import input_Nodes_Uses, unlist
from .auth_utils import verify_request_auth, classify_auth_error_status

upload_bp = Blueprint('upload', __name__)

@upload_bp.route("/uploadInputNodes", methods=['GET', 'POST'])
def upload_API():
    acting_user = "unknown"
    try:
        data = request.get_json(silent=True)
        if data is None:
            raw = request.get_data(as_text=True)
            data = json.loads(raw) if raw else {}
        if not isinstance(data, dict):
            raise Exception("Invalid payload")

        credentials = unlist(data.get("cred"))
        claims = verify_request_auth(credentials=credentials, req=request)
        acting_user = claims.get("userid") or "unknown"
        requested_user = data.get("user")
        if requested_user is not None and str(requested_user).strip():
            if str(requested_user).strip() != str(acting_user):
                raise Exception("User does not match authenticated API key/token owner")

        df = data.get("df")
        database = unlist(data.get("database"))
        formData = unlist(data.get("formData"))
        if not isinstance(formData, dict):
            raise Exception("Invalid formData")
        label = formData.get("subdomain") or formData.get("domain")
        label_upper = str(label).upper() if label is not None else ""
        if label_upper == "ANY DOMAIN":
            label = "CATEGORY"
        if label_upper == "AREA":
            label = "DISTRICT"
        datasetID = formData["datasetID"]
        CMName = formData["cmNameColumn"]
        Name = formData["categoryNamesColumn"]
        altNamesColumns = formData.get("alternateCategoryNamesColumns", [])
        if not isinstance(altNamesColumns, list):
            altNamesColumns = [altNamesColumns] if altNamesColumns else []
        altNamesColumns = [col for col in altNamesColumns if col]
        altNames = formData.get("alternateCategoryNamesColumn", "")
        CMID = formData["cmidColumn"]
        Key = formData["keyColumn"]

        optionalProperties = data.get("allContext") or []

        addoptions = data.get("addoptions") or {}
        addDistrict = bool(addoptions.get("district"))
        addRecordYear = bool(addoptions.get("recordyear"))

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
                user=acting_user,
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

            if altNamesColumns:
                existing_alt_cols = [col for col in altNamesColumns if col in df.columns]
                if existing_alt_cols:
                    def _combine_alt_names(row):
                        values = []
                        for col in existing_alt_cols:
                            raw = row.get(col)
                            if pd.isna(raw):
                                continue
                            text = str(raw).strip()
                            if text:
                                values.append(text)
                        return ";".join(values)
                    df["altNames"] = df.apply(_combine_alt_names, axis=1)
            elif altNames and altNames in df.columns:
                df.rename(columns={altNames: "altNames"}, inplace=True)

            df.rename(columns={CMName: "CMName", CMID: "CMID", Name: "Name", Key: "Key"}, inplace=True)
            df = df.to_dict(orient='records')
            # return {"Name":Name, "CMID":CMID,"altNames":altNames,"Key":Key,"user":user,"overwriteProperties":overwriteProperties,"updateProperties":updateProperties,"addDistrict":addDistrict,"addRecordYear":addRecordYear}
            response, desired_order = input_Nodes_Uses(
                dataset=df,
                database=database,
                uploadOption="add_uses",
                formatKey=True,
                optionalProperties=optionalProperties,
                user=acting_user,
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
        error_message = str(e)
        log_file = f'log/{acting_user}uploadProgress.txt'
        full_log = []
        if os.path.exists(log_file):
            with open(log_file, 'r') as file:
                full_log = file.readlines()
        else:
            full_log.append("Log file not found.")

        response_data = {
            "error": f"Upload error - {error_message}",
            "full_log": full_log
        }

        status_code = classify_auth_error_status(error_message) or 500
        return jsonify(response_data), status_code
