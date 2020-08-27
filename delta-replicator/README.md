# Scripts for Delta Replicator

## Summary
This script is used to automate the process of creating database/tables and inserting/updating events to SQL Server database periodically.

## Prerequisite
1. Instanll JVM

## Variables
1. host : hostname or ip of the SQL Server database
2. user : username of the SQL Server database
3. password : password of the SQL Server database
4. timezone : timezone setting of SQL Server database (default: PST)
5. port : port number of the SQL Server database (default: 1433)
6. numOfTables : num of tables to create (default: 100)
7. rowSizeBytes : size of each insert row in bytes (default: 5000)
8. durationMins : duration to keep running the script in minutes (default: 5)
9. batchSize : number of rows to insert per table per interval (default: 1)
10. updateFactor : the factor between number of insert and update events (default: 1). For example, if the factor is set to 0.5 and batch size is 10, it means that this script will generate 10 insert events and 5 update events per interval.

(note that, the default interval is 1s, if you want to change, you will need to update variable 'PERIOD_IN_SECONDS' in the java code)

## How to execute
```
./testDP.sh --host localhost --user root --password pwd --timezone PST --port 1433 --numOfTables 100 --rowSizeByte
s 10000 --durationMins 15 --batchSize 1 --updateFactor 1.0
```
