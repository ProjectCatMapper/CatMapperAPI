import CM
import pandas as pd
import time
import re

# data = [{"CMName":"test-1","label":"ETHNICITY"}]
# df = pd.DataFrame(data)
# df
# database = "SocioMap"
# user = "1"
# CM.createNodes(df,database,user)

# data = [{"to":"SM466731","Name":"test-1","from":"SD11","Key":"test-1","geoCoords":"yep","yearStart":2011,"label":"ETHNICITY"}]
# df = pd.DataFrame(data)
# df
# database = "SocioMap"
# user = "1"
# CM.createUSES(df,database,user)


data = [{"to":"SM466731","Name":"test-1a","from":"SD11","Key":"test-1","geoCoords":"testing now","yearStart":2013,"label":"ETHNICITY"}]
df = pd.DataFrame(data)
df
database = "SocioMap"
user = "1"
CM.updateProperty(df,database,user,updateType = "update")
