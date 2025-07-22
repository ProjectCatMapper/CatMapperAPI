#!/bin/bash

source /opt/conda/etc/profile.d/conda.sh
conda run -n global_api_env --no-capture-output which python

conda run -n global_api_env --no-capture-output python -c "import pandas; print(pandas.__version__)"