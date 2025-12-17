#!/bin/bash

# Submit bronze job in background
docker exec -d spark-master spark-submit \
    /opt/src/transform/bronze/participantUnit.py &

# # Wait a bit for bronze to start
# sleep 10

# # Submit silver job in background
# docker exec -d spark-master spark-submit \
#     /opt/src/transform/silver/summoner.py &

# echo "Both jobs submitted to Spark cluster"
# echo "Check status at: http://localhost:8080"
# echo "Press Ctrl+C to stop monitoring, jobs will continue running"