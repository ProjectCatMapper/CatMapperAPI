import CM
driver = CM.getDriver("SocioMap")
CMID = "SM462091"
properties = CM.getPropertiesMetadata(driver = driver)
properties = [item for item in properties if item.get('relationship') is not None] 

for property, relationship in zip([item['property'] for item in properties if 'property' in item], [item['relationship'] for item in properties if 'relationship' in item]):
    print(f"{property} {relationship} {CMID}")
            
    CM.fixUsesRels(CMID=CMID, property=property, relationship=relationship, driver = driver)