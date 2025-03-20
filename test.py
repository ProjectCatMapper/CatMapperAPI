import os
import pandas as pd
import re
from CM.utils import *
from CM.keys import *
import zipfile

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

    def transform_variables(self, variables):
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
            print("Detected Windows path. Converting to R-compatible format...")
            
            # Convert backslashes to forward slashes
            wd = wd.replace("\\", "/")
            
            # Ensure proper formatting (R escape sequences)
            wd = wd.replace(" ", "\\ ")  # Escape spaces if needed

        self.template = self.template.iloc[1:]
        
        variables =  self.run_query(
                """
                UNWIND $stack AS s
                MATCH (v:VARIABLE)<-[r:MERGING]-(:DATASET {CMID: s})
                WHERE NOT r.varName IS NULL 
                RETURN DISTINCT v.CMID AS variableCMID, apoc.text.join(apoc.coll.flatten([r.varName],true), " || ") AS varName, r.transform AS transform
                """,
                parameters={"stack": self.template["stackID"].tolist()}
            )

        # Debugging Step: Print variables DataFrame before proceeding
        print("Debug: Variables DataFrame columns:", variables.columns)
        print("Debug: Variables DataFrame shape:", variables.shape)

        if variables.empty:
            raise ValueError("Error: No variables retrieved from Neo4j.")
        
        if "transform" in variables.columns:
            variables = self.transform_variables(variables)

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
            print("Warning: No categories found for merging template.")
        else:
            categories.to_csv(os.path.join(self.dirpath, "mergingCategories.csv"), index=False)

        metadata = self.run_query(
            """
            UNWIND $id AS id
            MATCH (d:DATASET {CMID: id})
            RETURN DISTINCT d.CMID AS datasetID, d.ApplicableYears AS year, apoc.text.join(d.District," || ") AS District
            """,
            parameters={"id": self.template["datasetID"].tolist()}
        )

        if not metadata.empty:
            metadata.to_csv(os.path.join(self.dirpath, "metadata.csv"), index=False)
            print("metadata.csv saved successfully!")
        else:
            print("Warning: No metadata found, metadata.csv not created.")
                
        # Save files if syntax is R
        if self.syntax == "R":
            self.template.to_csv(os.path.join(self.dirpath, "template.csv"), index=False)
            variables.to_csv(os.path.join(self.dirpath, "variables.csv"), index=False)
            
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

file_path = "GIS/language_map_valid.geojson"

# Read the file as a GeoDataFrame
gis = gpd.read_file(file_path)

def validate_geometry(gdf):
    # Ensure geometries are valid
    gdf["geometry"] = gdf["geometry"].apply(make_valid)
    return gdf

# Validate the geometries

gis = validate_geometry(gis)

# Save the valid geometries back to a file (if needed)
gis.to_file("GIS/language_map_valid_fixed.geojson", driver="GeoJSON")

gis

driver = getDriver("SocioMap")
datasetID = "SD486991"

query = """
MATCH (d:DATASET {CMID: $datasetID})-[r:USES]->(c:CATEGORY)
RETURN c.CMID AS CMID, c.CMName AS CMName, r.Key as Key
"""

matches = getQuery(query, driver, params={"datasetID": datasetID})