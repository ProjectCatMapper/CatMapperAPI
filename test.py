import os
import pandas as pd
import re
from CM.utils import *
from CM.keys import *
import zipfile
import numpy as np

database = "SocioMap"
syntax = "R"
dirpath = "./tmp"
template = pd.read_csv("55-template.csv")

class createSyntax:
    def __init__(self, database, template, syntax="R", dirpath=None):
        self.syntax = syntax
        self.template = template
        self.database = database
        self.driver = getDriver(database)
        self.dirpath = dirpath if dirpath else os.path.abspath("./tmp")
        os.makedirs(self.dirpath, exist_ok=True)
        self.files = []
    
    def run_query(self, query, parameters=None):
        return getQuery(query, driver=self.driver, params=parameters, type = "df")

    def transform_variables_r(self, variables):
        variables["transform"] = variables["transform"].str.replace("~", "!", regex=True)
        variables["transform"] = variables["transform"].str.replace("=", "==", regex=True)
        variables["transform"] = variables["transform"].str.replace("!==", "!=", regex=True)
        variables["transform"] = variables["transform"].str.replace("concat", "paste0", regex=True)
        variables["transform"] = variables["transform"].str.replace(r',0\)', ',na.rm = True', regex=True)
        variables["transform"] = variables["transform"].str.replace("in", "%in%", regex=True)
        variables["transform"] = variables["transform"].str.replace("na.rm == T", "na.rm = True", regex=True)
        variables["transform"] = variables["transform"].str.replace("== as.numeric", "= as.numeric", regex=True)
        return variables
    
    def load_r_syntax_template(self, filename, replacements):
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
        
    def zip_output_files(self, zip_filename="output.zip"):
        """ Zip all generated files into a single archive """
        zip_path = os.path.join(self.dirpath, zip_filename)

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file in self.files:
                if os.path.exists(file):  # Ensure file exists before adding
                    zipf.write(file, os.path.basename(file))  # Store without full path
                    print(f"Added to ZIP: {file}")
                else:
                    print(f"Warning: {file} does not exist and was not added to the ZIP.")

        print(f"ZIP file created: {zip_path}")
        return zip_path
        
    def process(self):
        if "filePath" not in self.template.columns:
            raise ValueError("Must upload a list of datasets with the filePath column before generating syntax.")
        
        wd = self.template.iloc[0]["filePath"]

        if re.match(r"^[a-zA-Z]:\\\\", wd) or "\\" in wd:
            print("Detected Windows path. Converting to compatible format...")
            
            # Convert backslashes to forward slashes
            wd = wd.replace("\\", "/")
            
            # Ensure proper formatting (R escape sequences)
            wd = wd.replace(" ", "\\ ")  # Escape spaces if needed

        self.template = self.template.iloc[1:]

        # verify CMIDs
        cols = ["mergingID","stackID","datasetID"]
        cols = [col for col in cols if col in self.template.columns]
        CMIDs = list(set(self.template[cols].values.flatten().tolist()))

        check = self.run_query(
            """
            UNWIND $CMIDs as cmid
            match (a:DATASET {CMID: cmid})
            return a.CMID as CMID, a.CMName as CMName
            """,
            parameters={"CMIDs": CMIDs}
        )

        missing = set(CMIDs) - set(check["CMID"].tolist())
        missing = [str(m) + "\n" for m in missing]

        if len(check) != len(CMIDs):
            raise ValueError("Error: One or more CMIDs not found in the database\nMissing CMIDs: ", missing)
        else:
            print("All CMIDs found in the database.")
        self.template = self.template.drop(columns=["datasetName"])
        self.template = pd.merge(self.template, check, how="left", left_on="mergingID", right_on="CMID")
        self.template = self.template.rename(columns={"CMName": "mergingName"})
        self.template = pd.merge(self.template, check, how="left", left_on="stackID", right_on="CMID")
        self.template = self.template.rename(columns={"CMName": "stackName"})
        self.template = pd.merge(self.template, check, how="left", left_on="datasetID", right_on="CMID")
        self.template = self.template.rename(columns={"CMName": "datasetName"})
        self.template = self.template.drop(columns=["CMID_x", "CMID_y", "CMID"])
        cols = ["mergingID","mergingName", "stackID", "stackName", "datasetID","datasetName", "filePath"]
        self.template = self.template[cols]
        
        variable_list = []
        for s in self.template["stackID"].unique():
            print("getting variables for stackID: ", s)
            if self.syntax == "R":
                result =  self.run_query(
                """
                unwind $stackID as id
                match (v:VARIABLE)<-[r:MERGING]-(s:DATASET {CMID: id}) 
                where not r.varName is null 
                return distinct head($mergingID) as mergingID, s.CMID as stackID,
                v.CMID as variableCMID, 
                r.varName as varName, r.transform as transform, 
                r.Rtransform as Rtransform, r.Rfunction as Rfunction, 
                r.summaryStatistic as summaryStatistic
                """,
                parameters={"stackID": s, "mergingID": self.template["mergingID"].tolist()}
            )
            else:
                raise ValueError("Error: syntax not yet supported.")
                # variables =  self.run_query(
                #         """
                #         unwind $stackIDs as id
                #         match (s:DATASET {CMID: id})-[:MERGING]->(d2:DATASET) 
                #         return head($mergingID) as mergingID, 
                #         s.datasetID as stackID, d2.datasetID as datasetID, 
                #         d2.datasetName as datasetName order by datasetName
                #         """,
                #         parameters={"stackIDs": self.template["stackID"].tolist(), "mergingID": self.template["mergingID"].tolist()}
                #     )
            result = result.explode('varName', ignore_index=True)
            if "transform" in result.columns:
                print("transforming variables")

                if "Rtransform" in result.columns:
                    result['transform'] = result['Rtransform'].combine_first(result['transform'])

                CMshort = "SM" if self.database == "SocioMap" else "AM"

                # Fill NaNs with empty strings
                result['transform'] = result['transform'].fillna('')

                # Safely apply regex to extract CMIDs
                result['CMID'] = result['transform'].apply(
                    lambda x: list(set(re.findall(f'{CMshort}\\d+', x))) if x else []
                )

                # Replace empty lists with [NaN] to retain rows upon explosion
                result['CMID'] = result['CMID'].apply(lambda x: x if x else [np.nan])

                # Explode, preserving all rows
                result = result.explode('CMID', ignore_index=True)

                variable_list.append(result)

            variables = pd.concat(variable_list, ignore_index=True)

            # Convert list-type columns to tuples (safe for hashing)
            for col in variables.columns:
                if variables[col].apply(lambda x: isinstance(x, list)).any():
                    variables[col] = variables[col].apply(lambda x: tuple(x) if isinstance(x, list) else x)

            # Now drop duplicates safely
            variables = variables.drop_duplicates(ignore_index=True)


        if variables.empty:
            raise ValueError("Error: No variables retrieved from Neo4j.")

        # Check if "summaryStatistic" is a column in the variables DataFrame
        if "summaryStatistic" in variables.columns:
            print("Getting summary statistic")

            # Select unique (mergingID, stackID) pairs where mergingID is not empty
            temp_stacks = (
                self.template[['mergingID', 'stackID']]
                .drop_duplicates()
                .query("mergingID != ''")
            )

            agg_by_list = []

            query = """
            UNWIND $rows as row
            MATCH (m:DATASET {CMID: row.mergingID})-[r:MERGING]->(s:DATASET {CMID: row.stackID})
            return s.CMID as stackID, r.aggBy as aggBy
            """
            tempStacks = self.template[['mergingID', 'stackID']].drop_duplicates()
            aggBy = self.run_query(query, parameters={"rows": tempStacks.to_dict(orient='records')})
            if aggBy is not None and not aggBy.empty:
                variables = pd.merge(variables, aggBy, on='stackID', how='left')

            # Combine all query results into one DataFrame
            agg_by = pd.concat(agg_by_list, ignore_index=True) if agg_by_list else pd.DataFrame()

            # Left join with variables on "stackID"
            if not agg_by.empty:
                variables = variables.merge(agg_by, on='stackID', how='left')

        categories = self.run_query(
            """
            UNWIND $CMID AS cmid
            MATCH (:DATASET {CMID: cmid})-[r:USES]->(c:CATEGORY) 
            WHERE NOT 'VARIABLE' IN labels(c)
            RETURN DISTINCT c.CMID AS CMID, c.CMName AS CMName, r.Key AS Key, apoc.text.join(r.Dataset, " || ") AS datasetID
            """,
            parameters={"CMID": self.template["mergingID"].tolist()}
        )



        # Debugging step: Check if categories dataframe is empty
        if categories.empty:
            raise ValueError("Error: No categories retrieved from Neo4j.")
        else:
            print("Processing categories")
            categories = extract_key(categories, col="Key")
            categories = categories.drop(columns=["Key"])
            categories = categories.drop_duplicates()
            categories = categories.reset_index()
            categories = categories.melt(
                id_vars=['index', 'CMID', 'CMName', 'datasetID'],
                var_name='variable',
                value_name='value'
            )
            categories = categories.dropna(subset=["value"])
            # make "variable" lowercase
            categories["variable"] = categories["variable"].str.lower()
            # convert all columns to string
            categories = categories.astype(str)
            categories.replace("None", np.nan, inplace=True)
            categories.to_csv(os.path.join(self.dirpath, "mergingCategories.csv"), index=False, na_rep='')

        metadata = self.run_query(
            """
            UNWIND $id AS id
            MATCH (d:DATASET {CMID: id})
            RETURN DISTINCT d.CMID AS datasetID, d.ApplicableYears AS year, apoc.text.join(d.District," || ") AS District
            """,
            parameters={"id": self.template["datasetID"].tolist()}
        )

        if not metadata.empty:
            metadata = metadata.astype(str)
            metadata.replace("None", np.nan, inplace=True)
            metadata.to_csv(os.path.join(self.dirpath, "metadata.csv"), index=False, na_rep='')
            print("metadata.csv saved successfully!")
        else:
            print("Warning: No metadata found, metadata.csv not created.")

        stack_vars_list = []

        print("Getting stack variables")
        print(self.template['stackID'].unique())


        for s in self.template['stackID'].unique():
            print("Getting stack variables for stackID: ", s)
            ds = self.template[self.template['stackID'] == s]

            query = (
                "unwind $variableIDs as vid "
                "unwind $datasetIDs as did "
                "match (d:DATASET {CMID: did})-[r:USES]->(v:VARIABLE {CMID: vid}) "
                "return distinct r.Key as Key, v.CMID as variableCMID, d.CMID as datasetID"
            )
            print(variables.columns)
            parameters = {
                "variableIDs": variables['CMID'].dropna().unique().tolist(),
                "datasetIDs": ds['datasetID'].dropna().unique().tolist()
            }

            dfvar = self.run_query(query, parameters)

            if dfvar is not None and not dfvar.empty:
                dfvar['Key'] = dfvar['Key'].str.replace(r"variable:\s*", "", regex=True).str.lower()
                dfvar['stackID'] = s
                stack_vars_list.append(dfvar)

        if len(stack_vars_list) > 0:
            stack_vars = pd.concat(stack_vars_list, ignore_index=True)
            stack_vars = stack_vars.drop_duplicates()
            stack_vars = stack_vars.astype(str)
            stack_vars.replace("None", np.nan, inplace=True)
            stack_vars.to_csv(os.path.join(self.dirpath, "stackVariables.csv"), index=False, na_rep='')
            print("stackVariables.csv saved successfully!")
        else:
            print("Warning: No stack variables found, stackVariables.csv not created.")   

        dataset_variables_list = []

        for idx, row in self.template.iterrows():
            print("Getting individual dataset variables for stackID: ", row.stackID)
            tmp_vars = (
                variables[variables['transform'].isna()]
                .query("stackID == @row.stackID")
                .loc[:, ['varName', 'variableCMID']]
                .drop_duplicates()
            )

            if not tmp_vars.empty:
                rows = pd.DataFrame({
                    'vid': tmp_vars['variableCMID'],
                    'id': row['datasetID']
                })

                try:
                    query = (
                        "unwind $rows as row "
                        "match (v:VARIABLE {CMID: row.vid})<-[r:USES]-(d:DATASET {CMID: row.id}) "
                        "return v.CMID as variableCMID, tolower(replace(r.Key,'variable: ','')) as transform"
                    )

                    tmp_result = self.run_query(query, parameters={"rows": rows.to_dict(orient='records')})

                    if not tmp_result.empty:
                        tmp_result = (
                            tmp_result
                            .merge(tmp_vars, on='variableCMID', how='left')
                            .assign(datasetID=row['datasetID'])
                        )
                except Exception as e:
                    print("Error getting individual dataset variables")
                    tmp_result = pd.DataFrame()
            else:
                tmp_result = pd.DataFrame()

            dataset_variables_list.append(tmp_result)

        # Combine dataset-specific variables
        dataset_variables = pd.concat(dataset_variables_list, ignore_index=True)

        # Filter and tag stack-level variables
        stack_vars = (
            variables[variables['transform'].notna()]
            .copy()
        )
        stack_vars['varLevel'] = "stack"
        stack_vars['variableCMID'] = stack_vars['variableCMID'].astype(str)
        stack_vars = stack_vars.astype(str)

        # Tag dataset-level variables
        dataset_variables['varLevel'] = "dataset"
        dataset_variables = dataset_variables.astype(str)

        # Combine and finalize
        variables = (
            pd.concat([stack_vars, dataset_variables], ignore_index=True)
            .drop(columns=['CMID'], errors='ignore')
            .drop_duplicates()
            .astype(str)
        )
                
        # Save files if syntax is R
        if self.syntax == "R":
            self.template = self.template.astype(str)
            self.template.replace("None", np.nan, inplace=True)
            self.template.to_csv(os.path.join(self.dirpath, "template.csv"), index=False, na_rep='')
            if self.syntax == "R":
                variables = self.transform_variables_r(variables)
            variables = variables.drop_duplicates()
            variables = variables.astype(str)
            variables.replace("None", np.nan, inplace=True)
            variables.to_csv(os.path.join(self.dirpath, "variables.csv"), index=False, na_rep='')
            
            # Create R syntax file
            r_syntax_template = "syntax/Rsyntax.txt"  # Ensure this file exists in the working directory
            replacements = {
                "${f}": "\n".join(variables['transform'].dropna()),  # Functions applied
                "${wd}": wd,  # Working directory
                "${database}": self.database  # Database name
            }
            
            r_syntax = self.load_r_syntax_template(r_syntax_template, replacements)
            if r_syntax:
                with open(os.path.join(self.dirpath, "syntax.R"), "w") as f:
                    f.write(r_syntax)
                
                self.files.extend([
                    os.path.join(self.dirpath, "template.csv"),
                    os.path.join(self.dirpath, "variables.csv"),
                    os.path.join(self.dirpath, "syntax.R"),
                    os.path.join(self.dirpath, "mergingCategories.csv"),
                    os.path.join(self.dirpath, "metadata.csv")
                ])

                zip_path = self.zip_output_files("merged_output.zip")  # Zip them
                return zip_path
            else:
                raise ValueError("Error: R syntax not found.")
        else:
            raise ValueError("Error: syntax not yet supported.")


syntax = createSyntax(database, template, syntax, dirpath)

output_zip = syntax.process()

print(output_zip)

from CM.utils import *
from CM.keys import *
import geopandas as gpd
import pandas as pd
from shapely.validation import make_valid
import json
from shapely.geometry import mapping

file_path = "GIS/language_map_valid.geojson"

# Read the file as a GeoDataFrame
gis = gpd.read_file(file_path)

def validate_geometry(gdf):
    # Ensure geometries are valid
    gdf["geometry"] = gdf["geometry"].apply(make_valid)
    return gdf

# Validate the geometries

gis = validate_geometry(gis)
gis['geojson'] = gis.geometry.apply(lambda geom: json.dumps(mapping(geom)))


# Save the valid geometries back to a file (if needed)
gis.to_file("GIS/language_map_valid_fixed.geojson", driver="GeoJSON")

gis = gis.reset_index()

gis.rename(columns={"index": "Feature_ID"}, inplace=True)

gis = gis[["Feature_ID", "geojson"]]

gis = createKey(gis, "Feature_ID")

driver = getDriver("SocioMap")
datasetID = "SD486991"

query = """
MATCH (d:DATASET {CMID: $datasetID})-[r:USES]->(c:CATEGORY)
RETURN c.CMID AS CMID, c.CMName AS CMName, r.Key as Key, d.CMID as datasetID
"""

matches = getQuery(query, driver, params={"datasetID": datasetID}, type= "df")

result = pd.merge(gis, matches, how="left", on="Key")

# make sure all keys are matching
if result["CMID"].isna().any():
    raise ValueError("Error: Some keys do not match.")

new_id = getAvailableID(new_id="geomID", label="CATEGORY", n=len(result), database = "gisdb")
result["geomID"] = new_id
log = "Uploaded a new GeoJSON file"
user = "1"
from datetime import datetime
logQ = f"{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')} user {user}: {log}"
result["log"] = logQ
database = "SocioMap"
origins = []
for key in list(result["Key"].values):
    origins.append({"database": database, "datasetID": datasetID, "Key": key})

result['origin'] = origins
# convert all columns to string
result = result.astype(str)

query = """
UNWIND $rows as row
MERGE (g:GEOMETRY {origin: row.origin})
set g.geomID = row.geomID, g.geometry = row.geojson, g.log = row.log
RETURN g.geomID as geomID
"""
gisdriver = getDriver("gisdb")
df = getQuery(query, gisdriver, params={"rows": result.to_dict(orient='records')}, type = "df")
print(df)

upload = result[['geomID', 'Key']]
# rename geomID to geoPolygon
upload.rename(columns={"geomID": "geoPolygon"}, inplace=True)
upload = pd.merge(upload, matches, how="left", on="Key")

upload.to_excel("upload_USES.xlsx", index=False)




count = 1

while count > 0:
    query = """
    MATCH (g:GEOMETRY)
        WHERE g.origin contains ">"
        return count(*) as count
    """
    count = getQuery(query,gisdriver, type = "list")[0]
    print(count)
   
    upload_query = """
    MATCH (g:GEOMETRY)
    WHERE g.origin contains ">"
    WITH g, [i IN split(g.origin, ">") | trim(i)] AS origins limit 100
    WITH g,
        // Modify the first element and keep the rest
        '{"database": ' + origins[0] + ", " + replace(origins[1],"Dataset",'"datasetName"') + ', "Key": ' + origins[2] + "}" AS updatedOrigins
    set g.origin = updatedOrigins
    """ 
    getQuery(upload_query, gisdriver, type = "list")

import CM

# database = request.args.get('database')
# term = request.args.get('term')
# property = request.args.get('property')
# domain = request.args.get('domain')
# yearStart = request.args.get('yearStart')
# yearEnd = request.args.get('yearEnd')
# context = request.args.get('context')
# country = request.args.get('country')
# limit = request.args.get('limit')
# query = request.args.get('query')

database = "SocioMap"
term = "DHS Bangladesh 2015"
property = "Name"
domain = "DATASET"
yearStart = None
yearEnd = None
context = None
country = None
limit = None
query = 'false'

if domain == "ANY DOMAIN":
    domain = "CATEGORY"
if domain == "AREA":
    domain = "DISTRICT"

driver = CM.getDriver(database)

if term:
    term= term.strip()

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
    limit = 10000

if property is None and term is not None:
    raise Exception("Must specify a property (e.g., Name, CMID, or Key)")

# Define the Cypher query

# if no term specified
if term is None:
    qStart = f"match (a:{domain}) with a, '' as matching, 0 as score" 
elif property == "Key":
        qStart = f"""
call db.index.fulltext.queryRelationships('keys','"' + custom.escapeText($term) + '"') yield relationship
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
with a, [a.CMName, a.shortName, a.DatasetCitation] as nameList
with a, nameList,  collect([i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText($term))]) as scores
with a, nameList, scores, apoc.coll.min(scores) as score
with a, nameList[apoc.coll.indexOf(scores,scores)] as matching, score
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
with a, [a.CMName, a.shortName, a.DatasetCitation] as nameList
with a, nameList,  collect([i in nameList | apoc.text.levenshteinDistance(custom.cleanText(i),custom.cleanText($term))]) as scores
with a, nameList, scores, apoc.coll.min(scores) as score
with a, nameList[apoc.coll.indexOf(scores,scores)] as matching, score
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
with a, years, [i in apoc.coll.toSet(apoc.coll.flatten(collect(coalesce(yearMatch,"")),true))) | toInteger(i)] as yearMatch 
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
    driver = CM.getQuery(cypher_query, driver, params={"term": term, "context": context, "country": country, "yearStart": yearStart, "yearEnd": yearEnd})
    
# return jsonify(data)

print(cypher_query)