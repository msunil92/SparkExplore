SparkExplore

Steps:
1. run SparkRedisApp

2. docker run -d -p 6379:6379 --name redis redis
3. docker exec -it redis bash
4. redis-cli -> to set and get variables
5. redis-cli monitor -> to monitor ingestions


to add data:

xadd users * language 2 count 30
xadd users * language 4 count 43

