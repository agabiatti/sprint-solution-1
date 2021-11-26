#!/bin/bash

echo "Start Pipeline"

echo "Get 2021 data"
pip3 install -r api/requirements.txt
python3 api/main.py

echo "Clear Data"
docker exec -it jupyter-spark hdfs dfs -rm -r /raw_data

echo "Drop Database"
docker exec -it jupyter-spark chmod +x drop/drop.sh
docker exec -it jupyter-spark sh drop/drop.sh

echo "Import to HDFS"
docker exec -it jupyter-spark hdfs dfs -mkdir /raw_data
docker exec -it jupyter-spark hdfs dfs -put -f ./cartola/* /raw_data

echo "Create Ingestion"
docker exec -it jupyter-spark chmod +x create_ingestion/ingestion.sh
docker exec -it jupyter-spark sh create_ingestion/ingestion.sh

echo "Create Consumption"
docker exec -it jupyter-spark chmod +x create_consumption/consumption.sh
docker exec -it jupyter-spark sh create_consumption/consumption.sh
