from CM.utils import *
from CM.translate import *
import pandas as pd

df = pd.read_excel('matchType.xlsx')

result = translate(database = "SocioMap",
        property = "Name",
        domain = "CATEGORY",
        key = None,
        term = "Name",
        country = None, 
        context  = None,
        dataset = None,
        yearStart = None, 
        yearEnd = None,
        query = None,
        table = df,
        uniqueRows = False)

print(result)

matches = addMatchResults(result)

print(matches)

df = result

df['matchType'] = None

# Count occurrences of each CMID within each CMuniqueCategoryID
cmid_counts_per_category = df.groupby(['CMuniqueCategoryID', 'CMID']).size()

# Count occurrences of CMuniqueRowID within each CMuniqueCategoryID
cmunique_counts = df.groupby('CMuniqueCategoryID')['CMuniqueRowID'].transform('size')

# Helper to assign match types
def determine_match_type(row):
    cmid = row['CMID']
    matching_distance = row['matchingDistance']
    category_id = row['CMuniqueCategoryID']
    
    # Check the count of the current CMID within the specific CMuniqueCategoryID
    cmid_count_in_category = cmid_counts_per_category.get((category_id, cmid), 0)
    cmunique_count = cmunique_counts.get(category_id, 0)

    print(category_id)
    print(cmid)
    print(cmid_count_in_category)
    print(cmunique_count)
    
    # Determine match type based on conditions
    if matching_distance == 0 and cmid_count_in_category == 1 and cmunique_count == 1:
        return 'exact match'
    elif matching_distance > 0 and cmid_count_in_category == 1 and cmunique_count == 1:
        return 'fuzzy match'
    elif cmid_count_in_category > 1:
        return 'many-to-one'
    elif cmunique_count > 1:
        return 'one-to-many'
    return None

# Apply the function to each row
df['matchType'] = df.apply(determine_match_type, axis=1)
