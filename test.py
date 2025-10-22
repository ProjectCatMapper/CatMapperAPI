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



data = {"database":"ArchaMap","property":"Name","domain":"PERIOD","key":"false","term":"Phase","country":[""],"context":[""],"dataset":[""],"yearStart":-4000,"yearEnd":2024,"table":[{"Name":"Colinas Serrated","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Colinas Serrated","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Colinas Serrated","culture":"AM23935","Phase":"Soho","country":"AM21933","datasetID":"AD936"},{"Name":"Colinas Serrated","culture":"AM23935","Phase":"Civano","country":"AM21933","datasetID":"AD936"},{"Name":"Estrella Side-notched","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Estrella Side-notched","culture":"AM23935","Phase":"Gila Butte","country":"AM21933","datasetID":"AD936"},{"Name":"Estrella Side-notched","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Gatlin Side-notched","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Gatlin Side-notched","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Sauceda Side-notched","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Sauceda Side-notched","culture":"AM23935","Phase":"Snaketown","country":"AM21933","datasetID":"AD936"},{"Name":"Sauceda Side-notched","culture":"AM23935","Phase":"Gila Butte","country":"AM21933","datasetID":"AD936"},{"Name":"Sauceda Side-notched","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Sauceda Side-notched","culture":"AM23935","Phase":"Civano","country":"AM21933","datasetID":"AD936"},{"Name":"Salado Side-notched","culture":"AM23935","Phase":"Civano","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Stemmed","culture":"AM23935","Phase":"Snaketown","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Stemmed","culture":"AM23935","Phase":"Gila Butte","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Stemmed","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Sonoran Side-notched","culture":"AM23935","Phase":None,"country":"AM21933","datasetID":"AD936"},{"Name":"Citrus Side-notched","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Citrus Side-notched","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Solares Corner-notched","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Solares Corner-notched","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Solares Corner-notched","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Serrated","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Serrated","culture":"AM23935","Phase":"Soho","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Serrated","culture":"AM23935","Phase":"Snaketown","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Serrated","culture":"AM23935","Phase":"Gila Butte","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Serrated","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Barbed","culture":"AM23935","Phase":"Gila Butte","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Barbed","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Barbed","culture":"AM23935","Phase":"Snaketown","country":"AM21933","datasetID":"AD936"},{"Name":"Snaketown Barbed","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Santan Barbed","culture":"AM23935","Phase":"Sacaton","country":"AM21933","datasetID":"AD936"},{"Name":"Santan Barbed","culture":"AM23935","Phase":"Gila Butte","country":"AM21933","datasetID":"AD936"},{"Name":"Santan Barbed","culture":"AM23935","Phase":"Santa Cruz","country":"AM21933","datasetID":"AD936"}],"query":"false","countsamename":False,"uniqueRows":True}
