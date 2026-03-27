"""merge.py"""

from .utils import *
from .keys import *
from .search import *
import pandas as pd
from flask import jsonify
import os
import re
from datetime import datetime
import hashlib
import base64
import zipfile
import numpy as np


def generate_unique_hash():
    now = datetime.utcnow().isoformat()
    return base64.urlsafe_b64encode(
        hashlib.sha256(now.encode()).digest()
    ).decode()[:16]

def split_vars_values(s):
    if pd.isna(s):
        return pd.Series([None, None])
    # Split by semicolon first
    pairs = [p.strip() for p in s.split(";")]
    variables = []
    values = []
    for pair in pairs:
        if ": " in pair:
            var, val = pair.split(": ", 1)
            variables.append(var.strip())
            values.append(val.strip())
    return pd.Series(["; ".join(variables), "; ".join(values)])


def get_dataset_name_map(driver, dataset_ids):
    if not dataset_ids:
        return {}

    query = """
    UNWIND $dataset_ids AS cmid
    MATCH (d:DATASET {CMID: cmid})
    RETURN d.CMID AS datasetID, d.CMName AS datasetName
    """
    rows = getQuery(query, driver=driver, params={"dataset_ids": dataset_ids})

    if isinstance(rows, pd.DataFrame):
        records = rows.to_dict(orient="records")
    else:
        records = rows or []

    return {
        row.get("datasetID"): row.get("datasetName", "")
        for row in records
        if row.get("datasetID")
    }


def _build_extended_wide_frame(matches, dataset_choices, intersection):
    merge_how = "inner" if intersection else "outer"
    wide = None

    for dataset_id in dataset_choices:
        dataset_rows = matches[matches["datasetID"] == dataset_id].copy()
        dataset_cols = ["LCA_CMID", "LCA_CMName"]
        rename_map = {}

        if "Key" in dataset_rows.columns:
            rename_map["Key"] = f"Key_{dataset_id}"
            dataset_cols.append(f"Key_{dataset_id}")
        if "Name" in dataset_rows.columns:
            rename_map["Name"] = f"Name_{dataset_id}"
            dataset_cols.append(f"Name_{dataset_id}")
        if "tie" in dataset_rows.columns:
            rename_map["tie"] = f"tie_{dataset_id}"
            dataset_cols.append(f"tie_{dataset_id}")

        if dataset_rows.empty:
            dataset_frame = pd.DataFrame(columns=dataset_cols)
        else:
            dataset_frame = dataset_rows.rename(columns=rename_map)[dataset_cols].drop_duplicates()

        if wide is None:
            wide = dataset_frame
        else:
            wide = pd.merge(
                wide,
                dataset_frame,
                on=["LCA_CMID", "LCA_CMName"],
                how=merge_how,
            )

    if wide is None:
        return pd.DataFrame()

    return wide.drop_duplicates()


def _select_best_extended_rows(result, dataset_choices, ncontains, intersection):
    if result.empty:
        return result

    total_datasets = len(dataset_choices)
    key_cols = [f"Key_{dataset_id}" for dataset_id in dataset_choices if f"Key_{dataset_id}" in result.columns]
    tie_cols = [f"tie_{dataset_id}" for dataset_id in dataset_choices if f"tie_{dataset_id}" in result.columns]

    result = result.copy()

    if key_cols:
        present_keys = result[key_cols].notna()
        for key_col in key_cols:
            present_keys[key_col] = present_keys[key_col] & result[key_col].astype(str).str.strip().ne("")
        matched_count = present_keys.sum(axis=1)
    else:
        matched_count = pd.Series(0, index=result.index)

    if tie_cols:
        tie_values = result[tie_cols].apply(pd.to_numeric, errors="coerce")
        tie_sum = tie_values.sum(axis=1, skipna=True)
        max_tie = tie_values.max(axis=1, skipna=True).fillna(0)
    else:
        tie_sum = pd.Series(0, index=result.index)
        max_tie = pd.Series(0, index=result.index)

    infinity = 1000
    result["_matchedDatasetCount"] = matched_count
    result["nTie"] = tie_sum + (total_datasets - matched_count) * infinity

    # Keep unmatched rows for key coverage, but enforce tie radius when all datasets are matched.
    result = result[(result["_matchedDatasetCount"] < total_datasets) | (max_tie <= ncontains)]

    if intersection:
        result = result[result["_matchedDatasetCount"] == total_datasets]

    if result.empty:
        return result

    rows_to_keep = np.zeros(len(result), dtype=int)
    for key_col in key_cols:
        key_series = result[key_col].fillna("").astype(str).str.strip()
        has_key = key_series.ne("")
        if not has_key.any():
            continue

        candidates = result[has_key].copy()
        candidates["_row_idx"] = candidates.index
        best_rows = (
            candidates.sort_values(
                by=[key_col, "_matchedDatasetCount", "nTie", "LCA_CMID"],
                ascending=[True, False, True, True],
                kind="mergesort",
            )
            .drop_duplicates(subset=[key_col], keep="first")
        )
        rows_to_keep[result.index.isin(best_rows["_row_idx"])] = 1

    if rows_to_keep.any():
        result = result[rows_to_keep == 1]

    return result.drop(columns=["_matchedDatasetCount"], errors="ignore")


def _discover_crossdomain_of_relationship(driver, source_domain, target_domain):
    source_domain = sanitize_cypher_identifier(source_domain, "sourceDomain")
    target_domain = sanitize_cypher_identifier(target_domain, "targetDomain")
    query = f"""
    MATCH (s:{source_domain})-[r]-(t:{target_domain})
    WHERE type(r) ENDS WITH '_OF'
    RETURN DISTINCT type(r) AS relType
    ORDER BY relType
    """
    rows = getQuery(query, driver=driver) or []
    rel_types = [row.get("relType") for row in rows if isinstance(row, dict) and row.get("relType")]

    if not rel_types:
        raise ValueError(
            f"No *_OF relationship exists between {source_domain} and {target_domain}. "
            "Cross-domain merge requires one *_OF tie plus CONTAINS ties."
        )
    if len(rel_types) > 1:
        raise ValueError(
            f"Multiple *_OF relationships found between {source_domain} and {target_domain}: "
            f"{', '.join(sorted(rel_types))}. Please resolve metadata before running cross-domain merge."
        )

    return sanitize_cypher_identifier(rel_types[0], "crossDomainRelationship")


def _get_crossdomain_matches(
    driver,
    dataset_choices,
    source_domain,
    target_domain,
    return_domain,
    max_hops,
    of_relationship,
):
    source_domain = sanitize_cypher_identifier(source_domain, "sourceDomain")
    target_domain = sanitize_cypher_identifier(target_domain, "targetDomain")
    return_domain = sanitize_cypher_identifier(return_domain, "returnDomain")
    of_relationship = sanitize_cypher_identifier(of_relationship, "ofRelationship")

    if max_hops < 1 or max_hops > 6:
        raise ValueError("maxHops must be between 1 and 6")

    query = f"""
    UNWIND $datasets AS dataset
    MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(src:{source_domain})
    CALL {{
        WITH src
        RETURN src AS srcExpanded, 0 AS sourceTie
        UNION
        WITH src
        MATCH p=(src)-[rc:CONTAINS*1..{max_hops}]-(srcExpanded:{source_domain})
        WHERE isEmpty([rel IN rc WHERE rel.generic = true])
        RETURN srcExpanded, length(p) AS sourceTie
    }}
    MATCH (srcExpanded)-[:{of_relationship}]-(tgt:{target_domain})
    CALL {{
        WITH tgt
        MATCH (outNode:{return_domain})
        WHERE elementId(outNode) = elementId(tgt)
        RETURN outNode, 0 AS targetTie
        UNION
        WITH tgt
        MATCH p2=(tgt)-[tc:CONTAINS*1..{max_hops}]-(outNode:{return_domain})
        WHERE isEmpty([rel IN tc WHERE rel.generic = true])
        RETURN outNode, length(p2) AS targetTie
    }}
    RETURN DISTINCT
        d.CMID AS datasetID,
        src.CMID AS sourceCMID,
        src.CMName AS sourceCMName,
        srcExpanded.CMID AS sourceExpandedCMID,
        srcExpanded.CMName AS sourceExpandedCMName,
        tgt.CMID AS targetCMID,
        tgt.CMName AS targetCMName,
        outNode.CMID AS CMID,
        outNode.CMName AS CMName,
        r.Key AS Key,
        apoc.text.join(apoc.coll.toSet(r.Name), '; ') AS Name,
        sourceTie,
        targetTie,
        sourceTie + targetTie + 1 AS tie
    """
    matches = getQuery(query, driver=driver, params={"datasets": dataset_choices}, type="df")
    if not isinstance(matches, pd.DataFrame):
        matches = pd.DataFrame(matches or [])
    return matches


def _normalize_selected_key_variables(selectedKeyvariables):
    selectedKeyvariables = selectedKeyvariables or {}
    return {
        str(k).strip(): str(v).strip()
        for k, v in selectedKeyvariables.items()
        if str(k).strip() and str(v).strip()
    }


def _filter_long_crossdomain_by_selected_keyvariables(matches, selectedKeyvariables):
    if matches.empty:
        return matches
    selectedKeyvariables = _normalize_selected_key_variables(selectedKeyvariables)
    if not selectedKeyvariables:
        return matches

    result = matches.copy()
    for dataset_id, prefix in selectedKeyvariables.items():
        mask = result["datasetID"].astype(str).str.strip().eq(dataset_id)
        if mask.any():
            key_mask = result["Key"].fillna("").astype(str).str.startswith(prefix, na=False)
            result = result[~mask | key_mask]
    return result


def _select_best_crossdomain_rows(matches):
    if matches.empty:
        return matches

    sort_cols = [col for col in ["datasetID", "sourceCMID", "Key", "tie", "sourceTie", "targetTie", "CMID"] if col in matches.columns]
    if sort_cols:
        matches = matches.sort_values(by=sort_cols, kind="mergesort")

    dedupe_cols = [col for col in ["datasetID", "sourceCMID", "Key"] if col in matches.columns]
    if dedupe_cols:
        matches = matches.drop_duplicates(subset=dedupe_cols, keep="first")

    return matches


def _apply_crossdomain_anchor_and_intersection(matches, dataset_choices, primary_dataset, intersection):
    if matches.empty:
        return matches

    primary = matches[matches["datasetID"] == primary_dataset].copy()
    if primary.empty:
        return pd.DataFrame()

    anchor_nodes = set(primary["CMID"].dropna().astype(str))
    anchored = matches[matches["CMID"].astype(str).isin(anchor_nodes)].copy()

    if intersection:
        expected = len(dataset_choices)
        coverage = anchored.groupby("CMID")["datasetID"].nunique()
        keep_nodes = coverage[coverage == expected].index
        anchored = anchored[anchored["CMID"].isin(keep_nodes)]

    return anchored


def _build_crossdomain_wide_frame(matches, dataset_choices, primary_dataset, intersection):
    if matches.empty:
        return pd.DataFrame()

    primary = matches[matches["datasetID"] == primary_dataset].copy()
    if primary.empty:
        return pd.DataFrame()

    base = primary.rename(
        columns={
            "Key": f"Key_{primary_dataset}",
            "Name": f"Name_{primary_dataset}",
            "tie": f"tie_{primary_dataset}",
            "sourceCMID": f"sourceCMID_{primary_dataset}",
            "sourceCMName": f"sourceCMName_{primary_dataset}",
        }
    )
    keep_cols = ["CMID", "CMName", f"Key_{primary_dataset}", f"Name_{primary_dataset}", f"tie_{primary_dataset}", f"sourceCMID_{primary_dataset}", f"sourceCMName_{primary_dataset}"]
    keep_cols = [c for c in keep_cols if c in base.columns]
    wide = base[keep_cols].drop_duplicates()

    for dataset_id in dataset_choices:
        if dataset_id == primary_dataset:
            continue
        rows = matches[matches["datasetID"] == dataset_id].copy()
        rows = rows.rename(
            columns={
                "Key": f"Key_{dataset_id}",
                "Name": f"Name_{dataset_id}",
                "tie": f"tie_{dataset_id}",
                "sourceCMID": f"sourceCMID_{dataset_id}",
                "sourceCMName": f"sourceCMName_{dataset_id}",
            }
        )
        cols = ["CMID", "CMName", f"Key_{dataset_id}", f"Name_{dataset_id}", f"tie_{dataset_id}", f"sourceCMID_{dataset_id}", f"sourceCMName_{dataset_id}"]
        cols = [c for c in cols if c in rows.columns]
        frame = rows[cols].drop_duplicates()
        merge_how = "inner" if intersection else "left"
        wide = pd.merge(wide, frame, on=["CMID", "CMName"], how=merge_how)

    tie_cols = [f"tie_{dataset_id}" for dataset_id in dataset_choices if f"tie_{dataset_id}" in wide.columns]
    key_cols = [f"Key_{dataset_id}" for dataset_id in dataset_choices if f"Key_{dataset_id}" in wide.columns]
    if tie_cols:
        tie_vals = wide[tie_cols].apply(pd.to_numeric, errors="coerce")
        if key_cols:
            present = wide[key_cols].fillna("").astype(str).apply(lambda col: col.str.strip().ne(""))
            matched_count = present.sum(axis=1)
        else:
            matched_count = pd.Series(0, index=wide.index)
        wide["nTie"] = tie_vals.sum(axis=1, skipna=True) + (len(dataset_choices) - matched_count) * 1000
    else:
        wide["nTie"] = 0

    cols = ["CMID", "CMName", "nTie"] + [col for col in wide.columns if col not in ["CMID", "CMName", "nTie"]]
    return wide[cols].drop_duplicates()


def _filter_wide_crossdomain_by_selected_keyvariables(result, selectedKeyvariables):
    selectedKeyvariables = _normalize_selected_key_variables(selectedKeyvariables)
    if not selectedKeyvariables:
        return result
    filtered = result.copy()
    for dataset_id, prefix in selectedKeyvariables.items():
        col = f"Key_{dataset_id}"
        if col in filtered.columns:
            filtered = filtered[filtered[col].fillna("").astype(str).str.startswith(prefix, na=False)]
    return filtered

# joins two datasets that have previously been translated into CatMapper’s database.Each dataset must include two columns: datasetiD and the Key pointing to a category.
# It returns a single spreadsheet with: 1) datasetIDs, 2) data columns from the original dataset (renamed with _left and _right suffixes if overlapping.  Rows with keys pointing to the same category are aligned in the output spreadsheet.
# When keys point to a CatMapper category, standardized identifiers are also returned (CMID, CMName).
# database = "ArchaMap"
# joinLeft = pd.read_excel("tmp/joinLeft.xlsx")
# joinRight = pd.read_excel("tmp/joinRight.xlsx")
def joinDatasets(database, joinLeft, joinRight, domain="CATEGORY"):
    try:

        # ensure dataframes
        joinLeft = pd.DataFrame(joinLeft)
        joinRight = pd.DataFrame(joinRight)

        if 'datasetID' not in joinLeft.columns:
            raise ValueError(
                "The 'datasetID' column is missing from the first DataFrame.")

        if 'datasetID' not in joinRight.columns:
            raise ValueError(
                "The 'datasetID' column is missing from the second DataFrame.")
        driver = getDriver(database)
        domain = validate_domain_label(domain, driver=driver)

        # Drop 'CMID' and 'CMName' only if they exist in the columns
        joinLeft.drop(
            columns=[col for col in ['CMID', 'CMName'] if col in joinLeft.columns], inplace=True)
        joinRight.drop(
            columns=[col for col in ['CMID', 'CMName'] if col in joinRight.columns], inplace=True)

        datasetID_left = joinLeft['datasetID'].unique()[0]
        datasetID_right = joinRight['datasetID'].unique()[0]

        # Query keys for left dataset
        match_query = f"""
        UNWIND $datasetID AS id
        MATCH (d:DATASET {{CMID: id}})-[r:USES]->(:{domain})
        WITH d, split(r.Key, '; ') AS Key
        WITH d, [i IN Key | trim(split(i, ': ')[0])] AS Key
        RETURN DISTINCT d.CMID AS datasetID, Key
        """
        match_left = getQuery(match_query, driver, {"datasetID": datasetID_left}, type = "df")
        
        if match_left.empty:
            raise ValueError(
                "No categories found in first DataFrame for domain: " + domain)

        # Query keys for right dataset

        match_right = getQuery(match_query, driver, {"datasetID": datasetID_right}, type = "df")

        if match_right.empty:
            raise ValueError(
                "No categories found in second DataFrame for domain: " + domain)

        left_keys = match_left['Key'].explode(
        ).unique() if 'Key' in match_left else []
        right_keys = match_right['Key'].explode(
        ).unique() if 'Key' in match_right else []

        # Check for available columns
        found_left_keys = [key for key in joinLeft.columns if key in left_keys]
        found_right_keys = [
            key for key in joinRight.columns if key in right_keys]

        # Throw an error only if none of the keys are found
        if not found_left_keys:
            print(
                {"error": "Cannot continue with merge: no matching required columns found in first DataFrame"})
        if not found_right_keys:
            print(
                {"error": "Cannot continue with merge: no matching required columns found in second DataFrame"})

        # Convert only the found columns to string type
        joinLeft[found_left_keys] = joinLeft[found_left_keys].astype(
            str, errors='ignore')
        joinRight[found_right_keys] = joinRight[found_right_keys].astype(
            str, errors='ignore')

        merge_left = joinLeft[['datasetID'] + found_left_keys].copy()
        merge_left = createKey(merge_left, cols=found_left_keys).rename(
            columns={'Key': 'term', 'datasetID': 'dataset'})
        translate_left = translate(database=database, property="Key", domain=domain, term="term", table=merge_left,
                                   key='false', country=None, context=None, dataset='dataset', yearStart=None, yearEnd=None, query='false', uniqueRows=False, countsamename=False)
        if not 'CMID_term' in translate_left[0].columns:
            raise ValueError(
                f"Translation failed: 'CMID_term' not found in translation results for first dataset. Check domain ({domain}) and keys.")
         # rename columns to remove _term suffix
        translate_left = translate_left[0].rename(
            columns=lambda x: x.replace('_term', ''))
        merge_left = translate_left[
            ['term', 'CMID', 'CMName', 'dataset']
            ].merge(merge_left, on=['term', 'dataset']).drop(
            columns='term').rename(columns={'dataset': 'datasetID'}).drop_duplicates()

        # merge right
        merge_right = joinRight[['datasetID'] + found_right_keys].copy()
        merge_right = createKey(merge_right, cols=found_right_keys).rename(
            columns={'Key': 'term', 'datasetID': 'dataset'})
        translate_right = translate(
            database=database,
            property="Key",
            domain="CATEGORY",
            term="term",
            table=merge_right,
            key='false',
            country=None,
            context=None,
            dataset='dataset',
            yearStart=None,
            yearEnd=None,
            query='false',
            uniqueRows=False,
            countsamename=False
        )
        if not 'CMID_term' in translate_right[0].columns:
            raise ValueError(
                "Translation failed: 'CMID_term' not found in translation results for second dataset. Check domain and keys.")
        translate_right = translate_right[0].rename(
            columns=lambda x: x.replace('_term', ''))
        merge_right = (
            translate_right[['term', 'CMID', 'CMName', 'dataset']]
            .merge(merge_right, on=['term', 'dataset'])
            .drop(columns='term')
            .rename(columns={'dataset': 'datasetID'})
            .drop_duplicates()
        )

        # Final joining
        # Identify overlapping columns between merge_left and merge_right, excluding CMID and CMName
        overlapping_columns = [
            col for col in merge_left.columns if col in merge_right.columns and col not in ['CMID', 'CMName']]

        # Perform the first merge between merge_left and merge_right with suffixes for overlapping columns
        link_file = merge_left.merge(
            merge_right,
            on=['CMID', 'CMName'],
            how='outer',
            suffixes=('_'+datasetID_left, '_'+datasetID_right)
        )
        
        # Rename datasetID in joinLeft and joinRight for consistent merging
        joinLeft.rename(columns={'datasetID': 'datasetID_' + datasetID_left}, inplace=True)
        joinRight.rename(columns={'datasetID': 'datasetID_' + datasetID_right}, inplace=True)

        # Merge link_file with joinLeft without adding further suffixes for overlapping columns
        link_file = link_file.merge(
            joinLeft,
            left_on=['datasetID_' + datasetID_left] + found_left_keys,
            right_on=['datasetID_' + datasetID_left] + found_left_keys,
            how='outer',
            # Prevents adding additional _x suffixes
            suffixes=('_'+datasetID_left, '_'+datasetID_right)
        )

        # Merge link_file with joinRight without adding further suffixes for overlapping columns
        link_file = link_file.merge(
            joinRight,
            left_on=['datasetID_' + datasetID_right] + found_right_keys,
            right_on=['datasetID_' + datasetID_right] + found_right_keys,
            how='outer',
            suffixes=('_'+datasetID_left, '_'+datasetID_right)
        )

        # Final clean-up to drop duplicates and sort by specified columns
        link_file = link_file.drop_duplicates().sort_values(
            by=['datasetID_' + datasetID_left, 'datasetID_' + datasetID_right, 'CMName', 'CMID'])

        # replace NaN with empty string
        link_file = link_file.fillna("")

        desired_order = ['CMID', 'CMName',
                         'datasetID_' + datasetID_left, 'datasetID_' + datasetID_right]
        remaining_cols = [
            col for col in link_file.columns if col not in desired_order]
        link_file = link_file[desired_order + remaining_cols]

        return link_file.to_dict(orient='records'), 200

    except Exception as e:
        try:
            status_code = 400 if isinstance(e, ValueError) else 500
            return {"error": str(e)}, status_code
        except:
            return {"Error": "Unable to process error"}, 500


def proposeMerge(
    dataset_choices,
    category_label,
    criteria,
    database,
    intersection,
    selectedKeyvariables,
    ncontains=2,
    resultFormat="key-to-key",
    source_domain=None,
    target_domain=None,
    return_domain=None,
    primary_dataset=None,
    max_hops=3,
):

    try:
        driver = getDriver(database)
        criteria = str(criteria or "").lower()

        if len(dataset_choices) < 1:
            return jsonify({"message": "Please select more options"}), 400

        if criteria == "crossdomain":
            source_domain = validate_domain_label(source_domain, driver=driver)
            target_domain = validate_domain_label(target_domain, driver=driver)
            return_domain = validate_domain_label(return_domain or target_domain, driver=driver)
            primary_dataset = str(primary_dataset or "").strip()
            if primary_dataset == "":
                raise ValueError("primaryDataset is required for crossdomain merges")
            if primary_dataset not in dataset_choices:
                raise ValueError("primaryDataset must be one of datasetChoices")
            try:
                max_hops = int(max_hops)
            except Exception:
                raise ValueError("maxHops must be an integer")
            if max_hops < 1 or max_hops > 6:
                raise ValueError("maxHops must be between 1 and 6")

            of_relationship = _discover_crossdomain_of_relationship(
                driver=driver,
                source_domain=source_domain,
                target_domain=target_domain,
            )

            matches = _get_crossdomain_matches(
                driver=driver,
                dataset_choices=dataset_choices,
                source_domain=source_domain,
                target_domain=target_domain,
                return_domain=return_domain,
                max_hops=max_hops,
                of_relationship=of_relationship,
            )
            if matches.empty:
                return jsonify({"message": "No data found"}), 404

            matches = _select_best_crossdomain_rows(matches)
            matches = _filter_long_crossdomain_by_selected_keyvariables(matches, selectedKeyvariables)
            matches = _apply_crossdomain_anchor_and_intersection(
                matches=matches,
                dataset_choices=dataset_choices,
                primary_dataset=primary_dataset,
                intersection=intersection,
            )
            if matches.empty:
                return jsonify({"message": "No cross-domain matches found"}), 404

            dataset_name_map = get_dataset_name_map(driver, dataset_choices)

            if resultFormat == "key-to-category":
                result = matches.copy()
                result["datasetCMName"] = result["datasetID"].map(dataset_name_map).fillna("")
                result["relationshipType"] = of_relationship
                preferred_cols = [
                    "CMID",
                    "CMName",
                    "datasetID",
                    "datasetCMName",
                    "Key",
                    "Name",
                    "tie",
                    "sourceCMID",
                    "sourceCMName",
                    "targetCMID",
                    "targetCMName",
                    "relationshipType",
                ]
                cols = [c for c in preferred_cols if c in result.columns] + [c for c in result.columns if c not in preferred_cols]
                result = result[cols].fillna("")
                return result.to_dict(orient="records")

            if resultFormat == "category-to-category":
                matches = matches.groupby(["datasetID", "CMID", "CMName"], as_index=False).agg({
                    "Key": lambda x: " || ".join(sorted(set([str(i) for i in x if str(i).strip()]))),
                    "Name": lambda x: " || ".join(sorted(set([str(i) for i in x if str(i).strip()]))),
                    "tie": "min",
                    "sourceCMID": lambda x: " || ".join(sorted(set([str(i) for i in x if str(i).strip()]))),
                    "sourceCMName": lambda x: " || ".join(sorted(set([str(i) for i in x if str(i).strip()]))),
                })

            result = _build_crossdomain_wide_frame(
                matches=matches,
                dataset_choices=dataset_choices,
                primary_dataset=primary_dataset,
                intersection=intersection,
            )
            if result.empty:
                return jsonify({"message": "No cross-domain matches found"}), 404

            result = _filter_wide_crossdomain_by_selected_keyvariables(result, selectedKeyvariables)
            if result.empty:
                return jsonify({"message": "No cross-domain matches found"}), 404

            result["nTie"] = pd.to_numeric(result["nTie"], errors="coerce").fillna(0).astype(int)
            for dataset_id in dataset_choices:
                result[f"datasetCMName_{dataset_id}"] = dataset_name_map.get(dataset_id, "")
            result["relationshipType"] = of_relationship

            if resultFormat == "key-to-key":
                for col in result.filter(like="Key_").columns:
                    parsed = result[col].apply(split_vars_values)
                    var_series = parsed[0]
                    val_series = parsed[1]
                    has_var_values = var_series.fillna("").astype(str).str.strip().ne("").any()
                    has_val_values = val_series.fillna("").astype(str).str.strip().ne("").any()
                    if has_var_values or has_val_values:
                        result[f"variable_{col}"] = var_series.fillna("")
                        result[f"value_{col}"] = val_series.fillna("")

            result = result.fillna("")
            return result.to_dict(orient="records")

        category_label = validate_domain_label(category_label, driver=driver)

        if criteria == "standard":

            query = f"""
                            UNWIND $datasets AS dataset
                            MATCH (c:{category_label})<-[r:USES]-(d:DATASET {{CMID: dataset}})
                            RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMName AS CMName, c.CMID AS CMID,
                                            apoc.text.join(apoc.coll.toSet(r.Name), '; ') AS Name
                            ORDER BY CMName
                    """

            merged = getQuery(query, driver=driver, params={
                              'datasets': dataset_choices}, type = "df")

            if merged.empty:
                return jsonify({"message": "No data found"}), 404
            
            if resultFormat == "key-to-category":
                cols = ['CMID', 'CMName', 'datasetID', 'Key', 'Name']
                result = merged[cols].copy()
                result = result.fillna("")
                return result.to_dict(orient='records')
                
            # filter to have one row per category
            if resultFormat =="category-to-category":
                merged = merged.groupby(['datasetID', 'CMName', 'CMID']).agg({
                    'Key': lambda x: list(x),
                    'Name': lambda x: list(x)
                }).reset_index()
                for col in merged.columns:
                    if merged[col].apply(lambda x: isinstance(x, list)).any():
                        merged[col] = merged[col].apply(lambda x: ' || '.join(map(str, x)) if isinstance(x, list) else x)
                        
            dataset_list = []
            for dataset in dataset_choices:
                tmp = merged[merged['datasetID'] == dataset].copy()
                dataset_list.append(tmp)
            merged_df = dataset_list[0]
            merged_df.rename(columns={"Key": f"Key_{merged_df['datasetID'].iloc[0]}", "Name": f"Name_{merged_df['datasetID'].iloc[0]}"}, inplace=True)
            merged_df.drop(columns=["datasetID"], inplace=True)

            for dataset in dataset_list[1:]:
                dataset.rename(columns={"Key": f"Key_{dataset['datasetID'].iloc[0]}", "Name": f"Name_{dataset['datasetID'].iloc[0]}"}, inplace=True)
                dataset.drop(columns=["datasetID"], inplace=True)
                merged_df = pd.merge(merged_df, dataset, on=["CMName", "CMID"], how="outer")
                
            # reorder columns
            cols = merged_df.columns.tolist()
            cols = ["CMID", "CMName"] + [col for col in cols if col not in ["CMID", "CMName"]]
            merged_df = merged_df[cols]

            # filter keys if intersection is off
            if intersection:
                for col in merged_df.columns:
                    if 'Key_' in col:
                        merged_df = merged_df[merged_df[col].notna()]

            merged = merged_df.fillna("")
            merged = merged.to_dict(orient='records')
            return merged

        elif criteria == "extended":
            query = generate_cypher_query(unlist(category_label), ncontains)
            dataset_name_map = get_dataset_name_map(driver, dataset_choices)

            matches = getQuery(query, driver, {"datasets": dataset_choices}, type="df")

            if matches.empty:
                return jsonify({"message": "No data found"}), 404
            
            if resultFormat == "key-to-category":
                cols = ['LCA_CMID', 'LCA_CMName', 'datasetID', 'tie', 'Key', 'Name']
                result = matches[cols].copy()
                result["datasetCMName"] = result["datasetID"].map(dataset_name_map).fillna("")
                result = result.fillna("")
                return result.to_dict(orient='records')
            
            if resultFormat == "category-to-category":
                matches = matches.groupby(['datasetID', 'LCA_CMName', 'LCA_CMID']).agg({
                    'Key': lambda x: list(x),
                    'Name': lambda x: list(x),
                    'tie': 'min',
                }).reset_index()
                for col in matches.columns:
                    if matches[col].apply(lambda x: isinstance(x, list)).any():
                        matches[col] = matches[col].apply(lambda x: ' || '.join(map(str, x)) if isinstance(x, list) else x)

            result = _build_extended_wide_frame(matches, dataset_choices, intersection)

            if result.empty:
                return jsonify({"message": "No common ancestors found"}), 404

            selectedKeyvariables = selectedKeyvariables or {}
            selectedKeyvariables = {
                f"Key_{k.strip()}": str(v).strip()
                for k, v in selectedKeyvariables.items()
                if str(v).strip()
            }

            for col, prefix in selectedKeyvariables.items():
                if col in result.columns:
                    result = result[result[col].fillna("").astype(str).str.startswith(prefix, na=False)]

            result = _select_best_extended_rows(
                result=result,
                dataset_choices=dataset_choices,
                ncontains=ncontains,
                intersection=intersection,
            )

            if result.empty:
                return jsonify({"message": "No common ancestors found"}), 404

            result["nTie"] = pd.to_numeric(result["nTie"], errors="coerce").fillna(0).astype(int)

            cols = ["LCA_CMID", "LCA_CMName", "nTie"] + \
                [col for col in result.columns if col not in [
                    "LCA_CMID", "LCA_CMName", "nTie"]]
            result = result[cols].copy()
            for dataset_id in dataset_choices:
                result[f"datasetCMName_{dataset_id}"] = dataset_name_map.get(dataset_id, "")
            result = result.fillna("")

            for col in result.filter(like="Key_").columns:
                parsed = result[col].apply(split_vars_values)
                var_series = parsed[0]
                val_series = parsed[1]
                has_var_values = var_series.fillna("").astype(str).str.strip().ne("").any()
                has_val_values = val_series.fillna("").astype(str).str.strip().ne("").any()
                if has_var_values or has_val_values:
                    result[f"variable_{col}"] = var_series.fillna("")
                    result[f"value_{col}"] = val_series.fillna("")

            return result.to_dict(orient='records')

        else:
            raise Exception("Invalid criteria")

    except Exception as e:
        try:
            status_code = 400 if isinstance(e, ValueError) else 500
            return {"error": str(e)}, status_code
        except:
            return {"Error": "Unable to process error"}, 500


def generate_cypher_query(domain, nContains):
    if not isinstance(domain, str):
        raise ValueError("domain must be a string")
    domain = sanitize_cypher_identifier(domain, "domain")
    if nContains < 1:
        raise ValueError("nContains must be at least 1")
    elif nContains > 4:
        raise ValueError("nContains must be at most 4")
    base_query = f"""
    UNWIND $datasets AS dataset
    MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{domain})
    RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
    c.CMID as LCA_CMID, c.CMName as LCA_CMName,
    apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, 0 as tie
    """

    union_queries = []
    for i in range(1, nContains + 1):
        union_query = f"""
        UNION ALL
        UNWIND $datasets AS dataset
        MATCH (d:DATASET {{CMID: dataset}})-[r:USES]->(c:{domain})
        MATCH (c)<-[rc:CONTAINS*{i}]-(p:{domain})
        WHERE isEmpty([i in rc WHERE i.generic = true])
        RETURN DISTINCT d.CMID AS datasetID, r.Key AS Key, c.CMID AS CMID, c.CMName AS CMName,
        p.CMID as LCA_CMID, p.CMName as LCA_CMName,
        apoc.text.join(apoc.coll.toSet(r.Name), "; ") AS Name, {i} as tie
        """
        union_queries.append(union_query)

    full_query = base_query + "\n".join(union_queries)
    return full_query

def load_r_syntax_template(filename, replacements):
    """ Reads R syntax template and replaces placeholders """
    try:
        with open(filename, "r") as file:
            content = file.read()
        for key, value in replacements.items():
            content = content.replace(key, value)
        return content
    except FileNotFoundError:
        print(f"Error: {filename} not found. Ensure the file exists.")
        return None


def zip_output_files(files, dirpath, zip_filename="output.zip"):
    """ Zip all generated files into a single archive """
    zip_path = os.path.join(dirpath, zip_filename)

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file in files:
            if os.path.exists(file):  # Ensure file exists before adding
                # Store without full path
                zipf.write(file, os.path.basename(file))
                print(f"Added to ZIP: {file}")
            else:
                print(
                    f"Warning: {file} does not exist and was not added to the ZIP.")

    print(f"ZIP file created: {zip_path}")
    return zip_path


def getMergingTemplate(datasetID, database):
    try:
        driver = getDriver(database)

        query = """
            RETURN
            "" as mergingID,
            "" as mergingCMName,
            "" as mergingShortName,
            "" as mergingCitation,
            "" as stackID,
            "" as datasetID,
            "" as datasetName,
            "Please enter the working directory as the first filepath" as filePath
            UNION ALL 
            MATCH (m:DATASET {CMID: $datasetID})-[:MERGING]->(s:DATASET)-[:MERGING]->(d:DATASET)
            RETURN
            m.CMID as mergingID,
            m.CMName as mergingCMName,
            m.shortName as mergingShortName,
            m.DatasetCitation as mergingCitation,
            s.CMID as stackID,
            d.CMID as datasetID,
            d.CMName as datasetName,
            "" as filePath
            """
        data = getQuery(query, driver=driver, params={
            "datasetID": datasetID})

        if data[0].get("error"):
            return jsonify({"message": "No data found"}), 404

        desired_order = [
            "mergingID",
            "mergingCMName",
            "mergingShortName",
            "mergingCitation",
            "stackID",
            "datasetID",
            "datasetName",
            "filePath"
        ]

        template = [
            {key: item.get(key, "") for key in desired_order}
            for item in data
        ]

        return jsonify(template)

    except Exception as e:
        try:
            return {"error": str(e)}, 500
        except:
            return {"Error": "Unable to process error"}, 500

# template = pd.read_excel("tmp/BecomingHopiMergingTemplate.xlsx")
# database = "ArchaMap"
# syntax = "R"
# dirpath = None
# download=True
def createSyntax(template, database="SocioMap",
                 syntax="R", dirpath=None, download=True):
    driver = getDriver(database)

    try:
        template = pd.DataFrame(template)
    except Exception as e:
        raise ValueError("Template must be a pandas DataFrame.") from e

    if template.empty:
        raise ValueError("Template DataFrame is empty.")

    required_cols = ["mergingID", "datasetID", "filePath"]
    for col in required_cols:
        if col not in template.columns:
            raise ValueError(f"Required column '{col}' is missing from the template.")

    # Use the first non-empty filePath as the R working directory.
    filepaths = template["filePath"].fillna("").astype(str).str.strip()
    wd = next((p for p in filepaths if p), "")
    if not wd:
        raise ValueError("Template must include at least one non-empty filePath value.")

    if re.match(r"^[a-zA-Z]:\\\\", wd) or "\\" in wd:
        print("Detected Windows path. Converting to compatible format...")
        wd = wd.replace("\\", "/")
        wd = wd.replace(" ", "\\ ")

    # Keep only actionable merge rows.
    template["mergingID"] = template["mergingID"].fillna("").astype(str).str.strip()
    template["datasetID"] = template["datasetID"].fillna("").astype(str).str.strip()
    template = template[(template["mergingID"] != "") & (template["datasetID"] != "")]
    if template.empty:
        raise ValueError("Template has no actionable rows after filtering blank mergingID/datasetID values.")

    if "stackID" not in template.columns:
        query = """
        UNWIND $rows as row
        MATCH (m:DATASET {CMID: row.mergingID})-[rs:MERGING]->(s:DATASET)-[rm:MERGING]->(d:DATASET {CMID: row.datasetID})
        RETURN
        m.CMID as mergingID, s.CMID as stackID, d.CMID as datasetID
        """
        stacks = getQuery(
            query,
            driver=driver,
            params={"rows": template.to_dict(orient='records')},
            type="df",
        )
        if stacks.empty:
            raise ValueError(
                "Could not retrieve stackIDs from the database. Please ensure mergingID and datasetID are correct."
            )
        template = pd.merge(template, stacks, on=["mergingID", "datasetID"], how="left")

    template = template[["mergingID", "stackID", "datasetID", "filePath"]].copy()

    if dirpath is None:
        dirpath = "./tmp"
    os.makedirs(dirpath, exist_ok=True)

    # verify CMIDs
    cols = ["mergingID", "stackID", "datasetID"]
    cmids = list(set(template[cols].values.flatten().tolist()))
    check = getQuery(
        """
        UNWIND $CMIDs as cmid
        MATCH (a:DATASET {CMID: cmid})
        RETURN a.CMID as CMID, a.CMName as CMName
        """,
        driver=driver,
        params={"CMIDs": cmids},
        type="df",
    )

    missing = sorted(set(cmids) - set(check["CMID"].tolist()))
    if missing:
        raise ValueError(
            "Error: One or more CMIDs not found in the database\nMissing CMIDs: "
            + ", ".join(missing)
        )

    variable_query = """
        UNWIND $rows as row
        MATCH (m:DATASET {CMID: row.mergingID})-[:MERGING]->(s:DATASET {CMID: row.stackID})-[rsv:MERGING]->(v:VARIABLE)<-[rdv:MERGING]-(d:DATASET {CMID: row.datasetID})
        WHERE rdv.stack = s.CMID
        OPTIONAL MATCH (v)<-[ru:USES]-(d)
        RETURN DISTINCT
        m.CMID as mergingID,
        m.CMName as mergingName,
        s.CMID as stackID,
        s.CMName as stackName,
        d.CMID as datasetID,
        d.CMName as datasetName,
        rsv.varName as varName,
        v.CMID as variableID,
        rsv.stackTransform as stackTransform,
        rsv.summaryStatistic as summaryStatistic,
        rdv.datasetTransform as datasetTransform,
        ru.Key as variableKey
    """
    variables = getQuery(
        variable_query,
        driver=driver,
        params={"rows": template.to_dict(orient='records')},
        type="df",
    )

    if variables.empty:
        raise ValueError("No merging variable mappings found for the provided template rows.")

    # Build data.xlsx payload from variable mappings.
    data = variables.copy()
    data["Key"] = data["variableKey"].fillna("").astype(str)
    data["transform"] = (
        data["datasetTransform"].fillna("").astype(str).str.strip().replace("", np.nan)
        .fillna(data["stackTransform"].fillna("").astype(str).str.strip())
        .replace("", np.nan)
    )

    key_pairs = data[["datasetID", "Key"]].drop_duplicates().copy()
    key_pairs["Key2"] = key_pairs["Key"].str.split("; ")
    key_pairs = key_pairs.explode("Key2").reset_index(drop=True)
    parsed = key_pairs["Key2"].fillna("").astype(str).str.split(r"\s*(?:==|:)\s*", n=1, regex=True, expand=True)
    key_pairs["variable"] = parsed[0]
    key_pairs["value"] = parsed[1]
    key_pairs = key_pairs.drop(columns=["Key2"])

    data = pd.merge(data, key_pairs, on=["datasetID", "Key"], how="left")
    data["variable"] = data["variable"].fillna("").astype(str).str.lower()
    data = pd.merge(data, template[["datasetID", "filePath"]], on="datasetID", how="left")
    data = data.astype(str).replace("None", np.nan)
    data.to_excel(os.path.join(dirpath, "data.xlsx"), index=False)

    domain = validate_domain_label("CATEGORY", driver=driver)
    cat_query = f"""
        UNWIND $rows as row
        MATCH (d:DATASET {{CMID: row.datasetID}})-[ru:USES]->(c:{domain})
        OPTIONAL MATCH (c)-[:EQUIVALENT]->(e:{domain})
        RETURN
        d.CMID as datasetID,
        ru.Key as Key,
        c.CMID as CMID,
        c.CMName as CMName,
        e.CMID as equivalentCMID,
        e.CMName as equivalentCMName
    """
    categories = getQuery(
        cat_query,
        driver=driver,
        params={"rows": template.to_dict(orient='records')},
        type="df",
    )

    if categories.empty:
        categories = pd.DataFrame(
            columns=["datasetID", "Key", "CMID", "CMName", "equivalentCMID", "equivalentCMName"]
        )
    else:
        category_keys = categories[["datasetID", "Key"]].drop_duplicates().copy()
        category_keys["Key2"] = category_keys["Key"].str.split("; ")
        category_keys = category_keys.explode("Key2").reset_index(drop=True)
        parsed_keys = category_keys["Key2"].fillna("").astype(str).str.split(r"\s*(?:==|:)\s*", n=1, regex=True, expand=True)
        category_keys["variable"] = parsed_keys[0]
        category_keys["value"] = parsed_keys[1]
        category_keys = category_keys.drop(columns=["Key2"])

        categories = pd.merge(categories, category_keys, on=["datasetID", "Key"], how="left")
        categories = categories.drop_duplicates(subset=["datasetID", "Key", "CMID", "variable", "value"])
        categories["variable"] = categories["variable"].fillna("").astype(str).str.lower()
        categories = categories.astype(str).replace("None", np.nan)
    categories.to_excel(os.path.join(dirpath, "categories.xlsx"), index=False)

    r_syntax_template = "syntax/R_syntax.txt"
    replacements = {
        "${f}": "\n".join(data["transform"].dropna().astype(str).tolist()),
        "${wd}": wd,
        "${database}": database,
    }

    if syntax == "R":
        r_syntax = load_r_syntax_template(r_syntax_template, replacements)
        with open(os.path.join(dirpath, "syntax.R"), "w") as f:
            f.write(r_syntax)
    else:
        raise ValueError("Invalid syntax type. Only 'R' is supported.")

    files = [
        os.path.join(dirpath, "data.xlsx"),
        os.path.join(dirpath, "categories.xlsx"),
        os.path.join(dirpath, "syntax.R"),
    ]

    if download is True:
        hash_id = generate_unique_hash()
        zip_filename = f"merged_output_{hash_id}.zip"
    else:
        hash_id = ""
        zip_filename = "merged_output.zip"
    zip_path = zip_output_files(files, dirpath, zip_filename)

    return {"zip": zip_path, "hash": hash_id}
