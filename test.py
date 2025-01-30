from CM.utils import *
from CM.upload import *
import pandas as pd

df = pd.read_excel("NewEthnicitiesLOC_D.xlsx")

df = df[["Name","Key","CMID","country","parent","language"]]

result = input_Nodes_Uses(dataset = df,
                     database = "SocioMap",
                 CMName=None,
                 Name="Name",
                 CMID="CMID",
                 altNames=None,
                 Key="Key",
                 formatKey=False,
                 datasetID="datasetID",
                 label="label",
                 uniqueID=None,
                 uniqueProperty=None, 
                 nodeContext=None, 
                 linkContext=["country","parent","religion","language"],
                 user="1",
                 overwriteProperties=False,
                 updateProperties=False,
                 addDistrict=False,
                 addRecordYear=False,
                 geocode=False,
                 batchSize=1000)

