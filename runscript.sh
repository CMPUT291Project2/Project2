#!/bin/bash
db_type=$1

# Create tmp/edrick_db directory
mkdir -p tmp/edrick_db

echo "------------- Running CMPUT 291 Project 2 -------------"
java -jar project2.jar $db_type
echo "------------- Exiting CMPUT 291 Project 2 -------------"

# Delete tmp folder
rm -rf tmp/
