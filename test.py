from CM import *
import pandas as pd
database = "ArchaMap"
data = reportChanges(database, return_type = "info")
data
# data_USES = processUSES(database, CMID = "AM1", detailed = False)
# data_USES
# data_Dataset = processDATASETs(database)
# data_Dataset

database = "ArchaMap"
dateStart = None
dateEnd = None
action = "default"
user = None

with app.app_context():
    sendEmail(mail, subject=f"Routines for {database} - {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}", recipients=["bischrob@gmail.com"], body="<br>".join(info), sender=config['MAIL']['mail_default'], attachments=files)