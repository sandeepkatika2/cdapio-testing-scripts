#!/bin/bash

# Transform long options to short ones
for arg in "$@"; do
  shift
  case "$arg" in
    "--help") set -- "$@" "-h" ;;
    "--host") set -- "$@" "-o" ;;
    "--user")   set -- "$@" "-u" ;;
    "--password")   set -- "$@" "-p" ;;
   	"--timezone")   set -- "$@" "-z" ;;
	"--numOfTables")   set -- "$@" "-n" ;;
	"--rowSizeBytes")   set -- "$@" "-s" ;;
	"--durationMins")   set -- "$@" "-d" ;;
    "--app")   set -- "$@" "-a" ;;
    *)        set -- "$@" "$arg"
  esac
done

# Default behavior
host="localhost"
user="root"
password="password"
timezone="PST"
port=3306
num_of_tables=1
row_size=100
terminate_duration=10
app="MySQLTest"

# Parse short options
while getopts "o":"u":"p":"z":"n":"s":"d":"a":"h": opt
do
  case "$opt" in
    "o") host=${OPTARG} ;;
	"u") user=${OPTARG} ;;
	"p") password=${OPTARG} ;;
	"z") timezone=${OPTARG} ;;
	"n") num_of_tables=${OPTARG} ;;
	"s") row_size=${OPTARG} ;;
	"d") terminate_duration=${OPTARG} ;;
    "a") app=${OPTARG} ;;
	"h") echo "Example: ./testDeltaReplicator.sh --host localhost --user root --password pwd --timezone PST --port 3306 --numOfTables 100 --rowSizeBytes 10000 --durationMins 15"; exit 0 ;;
    "?") echo "Invalid Option(s)" >&2; exit 1 ;;
  esac
done

echo "========================================================"
echo "host : $host"
echo "user : $user"
echo "password : $password"
echo "timzeone : $timezone"
echo "port : $port"
echo "num of tables : $num_of_tables"
echo "row size in bytes : $row_size"
echo "terminate duration in minutes : $terminate_duration"
echo "app name : $app"
echo "========================================================"

# comments
CP=$PWD/jdbc/mysql-connector-java-8.0.19.jar

printf "\n\n**Starting Delta Replicator MySQL Testing**\n"
#echo $CP

printf "\n\n==Stop Current Delta Replicator==\n"
curl -XPOST localhost:11015/v3/namespaces/default/apps/$app/workers/DeltaWorker/stop

# Compile current java files
javac -cp .:$CP -d code/output code/*.java

printf "\n\n==Create Database and Tables==\n"
java -cp ./code/output:$CP TableCreator $host $user $password $timezone $port $num_of_tables

printf "\n\n==Start Current Delta Replicator==\n"
rm -rf /tmp/Replicator
curl -XPOST localhost:11015/v3/namespaces/default/apps/$app/workers/DeltaWorker/start

printf "\n\n==Waiting For 1min To Get All Tables Snapshotted==\n"
sleep 60

printf "\n\n==Start Inserting Rows into Table(s)==\n"
now=$(date)
printf "Current Time:%s\n" "$now"
java -cp ./code/output:$CP LoadGenerator $host $user $password $timezone $port $num_of_tables $row_size $terminate_duration