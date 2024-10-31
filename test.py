from CM.utils import *
from CM.translate import *
import pandas as pd
from CM.upload import *

df = pd.read_excel("nodes.xlsx")
df

input_Nodes_Uses(dataset = df,
                  database = "SocioMap",
                    CMName = "CMName", 
                    Name = "Name",
                 CMID=None,
                 altNames=None,
                 Key='Key',
                 formatKey=False,
                 datasetID='datasetID',
                 label='label',
                 uniqueID=None,
                 uniqueProperty=None, 
                 nodeContext=None, 
                 linkContext=None,
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

results = result

cols = ['term', 'CMID', 'matchingDistance', 'rcountry','rcontext']
cols = [col for col in cols if col in results.columns]
df = results[cols].drop_duplicates()

# Group by 'term' and count occurrences
df['n'] = df.groupby('term')['term'].transform('count')

# Determine the match type
conditions = [
        df['CMID'].isna(),
        (df['n'] > 1) & df['CMID'].notna(),
        df['matchingDistance'] > 0,
        True
]
choices = [
        'none',
        'one-to-many',
        'fuzzy match',
        'exact match'
]
df['matchType'] = np.select(conditions, choices, default=np.nan)

# Group by 'CMID' and count occurrences
df['n'] = df.groupby('CMID')['CMID'].transform('count')

# Adjust match type for many-to-one scenarios
df.loc[(df['matchType'] == 'one-to-many') & (df['matchType'] != 'none') & (df['n'] > 1), 'matchType'] = 'many-to-one'

# Drop the 'n' and 'matchingDistance' columns
df = df.drop(columns=['n', 'matchingDistance'])

# Join the original results with the new matchType information
results = pd.merge(results, df, on=['CMID', 'term'], how='left')