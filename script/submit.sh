docker exec -d spark-master spark-submit \
    /opt/src/transform/bronze/streamTables.py;

docker exec -d spark-master spark-submit \
    /opt/src/transform/silver/batchTables.py;

docker exec -it spark-master spark-submit /opt/src/transform/gold/unitRelation.py;

