from CM.utils import *
from CM.keys import *
from CM.translate import *
import pandas as pd
from flask import jsonify

category_label = "ETHNICITY"
ncontains = 2
dataset_choices = ['SD21','SD14']
query = generate_cypher_query(unlist(category_label),ncontains)
print(query)

