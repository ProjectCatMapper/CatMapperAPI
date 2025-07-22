from CM import getDriver, getQuery


def getDomains(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    domains = getQuery("MATCH (n:LABEL) where n.public = 'TRUE' and not n.label = 'CATEGORY' and n.groupLabel=n.label RETURN  n.groupLabel AS domain order by n.displayOrder,domain", driver, type="list")

    return domains

def getSubdomains(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    #subdomains = getQuery("MATCH (n:LABEL) where n.public = 'TRUE' and not n.label = 'CATEGORY' WITH DISTINCT n.groupLabel AS domain, n.label as label order by domain, label WITH domain, collect(label) AS subdomains RETURN domain, [domain] + [x IN subdomains WHERE x <> domain] AS subdomains", driver, type="dict")

    subdomains = getQuery("MATCH (n:LABEL) WHERE n.public = 'TRUE' AND NOT n.label = 'CATEGORY' WITH DISTINCT n.groupLabel AS domain, n.label AS label, n.displayOrder AS displayOrder ORDER BY domain, label WITH domain, collect(label) AS subdomains, min(displayOrder) AS displayOrder RETURN domain,[domain] + [x IN subdomains WHERE x <> domain] AS subdomains ORDER BY displayOrder, domain",driver,type="dict")

    return subdomains

def getDomainDescriptions(database):
    driver = getDriver(database)
    if not driver:
        return "Database connection failed."

    descriptions = getQuery("MATCH (n:LABEL) where n.label = n.groupLabel and n.public = 'TRUE' and not n.label = 'CATEGORY' RETURN DISTINCT n.label AS label, n.description AS description order by label", driver, type="dict")

    return descriptions