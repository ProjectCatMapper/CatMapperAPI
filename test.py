from CM.utils import *
from CM.translate import *
import pandas as pd
from CM.upload import *

df = pd.read_excel("AF2LanguagesPopEst.xlsx")
df.columns

input_Nodes_Uses(dataset = df,
                  database = "SocioMap",
                    CMName = None, 
                    Name = "Name",
                 CMID="CMID",
                 altNames=None,
                 Key='Key',
                 formatKey=False,
                 datasetID='datasetID',
                 label='label',
                 uniqueID=None,
                 uniqueProperty=None, 
                 nodeContext=None, 
                 linkContext=['sampleSize','recordStart', 'recordEnd', 'populationEstimate'],
                 user=1,
                 overwriteProperties=False,
                 updateProperties=False,
                 addDistrict=False,
                 addRecordYear=False,
                 geocode=False,
                 batchSize=1000,
                    )

df = pd.read_excel("UploadDatasetTest.xlsx")
df
createNodes(df,"SocioMap","1")

from CM.utils import *
from CM.translate import *
import pandas as pd

df = pd.read_excel("translate.xlsx")
df
df.columns

database = "SocioMap"
property = "Name"
domain = "CATEGORY"
key = None
term = "Name"
country = None
context = None
dataset = "datasetID"
yearStart = None
yearEnd = None
query = None
table = df.to_dict('records')

result = translate(
        database = database,
        property = property,
        domain = domain,
        key = key,
        term = term,
        country = country,
        context = context,
        dataset = dataset,
        yearStart = yearStart,
        yearEnd = yearEnd,
        query = query,
        table = table)
print(result)
df = result

# Initialize matchType column with None
df['matchType'] = None

# Count occurrences of CMID and CMuniqueRowID for each row
cmid_counts = df['CMID'].value_counts()
df['CMuniqueRowID'] = df['CMuniqueRowID'].astype(str)
cmunique_counts = df['CMuniqueRowID'].apply(lambda x: tuple(x)).value_counts()

# Helper to assign match types
def determine_match_type(row):
        cmid = row['CMID']
        cmunique = tuple(row['CMuniqueRowID'])
        matching_distance = row['matchingDistance']

        # Check conditions for match type
        if matching_distance == 0 and cmid_counts[cmid] == 1 and cmunique_counts[cmunique] == 1:
                return 'exact match'
        elif matching_distance > 0 and cmid_counts[cmid] == 1 and cmunique_counts[cmunique] == 1:
                return 'fuzzy match'
        elif cmunique_counts[cmunique] > 1:
                return 'one-to-many'
        elif cmid_counts[cmid] > 1:
                return 'many-to-one'


# Apply the function to each row
df['matchType'] = df.apply(determine_match_type, axis=1)

df
from CM.utils import *
from CM.translate import *
import pandas as pd
df = pd.read_excel("_Matched_2024-11-08.xlsx")
df = addMatchResults(df)
df