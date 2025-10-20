from CM import translate
import pandas as pd
example_Data = pd.DataFrame([{"period": "Archaic", "country":"AM22269"}, {"period": "Classical"}, {"period": "Hellenistic"}, {"period": "Roman"},{"period": "Archaic"}])
database = "ArchaMap"
property = "Name"
domain = "PERIOD"
key = "false"
term = "period"
country = ""
context = ""
dataset = ""
yearStart = ""
yearEnd = ""
query = ""
table = example_Data
countsamename = ""
uniqueRows = True

result = translate(database, property, domain, key, term, country, context, dataset, yearStart, yearEnd, query, table, countsamename, uniqueRows)

result[0]["period"]

result[0].to_excel("test_output.xlsx", index=False)