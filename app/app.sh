#!/bin/bash
service ssh restart 

bash start-services.sh

SPARK_DEFAULTS_CONF="/usr/local/spark/conf/spark-defaults.conf"
if [ -f "$SPARK_DEFAULTS_CONF" ]; then
  echo "Commenting out spark.yarn.jars in $SPARK_DEFAULTS_CONF (if present)..."
  sed -i '/^[^#]*spark.yarn.jars/ s/^/#/' "$SPARK_DEFAULTS_CONF"
else
  echo "Warning: $SPARK_DEFAULTS_CONF not found, cannot modify."
fi

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt  

venv-pack -o .venv.tar.gz

bash prepare_data.sh


bash index.sh

# Run the ranker
bash search.sh "this is a query!"