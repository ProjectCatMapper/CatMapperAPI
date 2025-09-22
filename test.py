import CM
import pandas as pd
database = "ArchaMap"
user = "1"
df = pd.read_excel("new CMID.xlsx")
for CMID in df['CMID']:
    input = {
    "s1_1": "edit",
    "s1_2": CMID
    }
    CM.deleteNode(database, user, input)