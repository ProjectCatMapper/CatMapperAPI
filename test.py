from CM.upload import *
import pandas as pd

df = pd.read_excel("TBDDHSEthnicityUpload11_11_2024.xlsx")

result = combine_properties(df, ["CMID","Key","datasetID"])

result['recordStart'].unique()

result.to_excel("tmp.xlsx", index=False)