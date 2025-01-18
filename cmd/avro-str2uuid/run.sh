#!/bin/sh

genavro(){
	export ENV_SCHEMA_FILENAME=./sample.d/sample.avsc
	cat ./sample.d/sample.jsonl |
		json2avrows |
		cat > ./sample.d/sample.avro
}

#genavro

export ENV_SCHEMA_FILENAME=./sample.d/output.avsc

export ENV_TARGET_COL_NAME=name
export ENV_STR2UUID_MAP_CSV_FILENAME=./sample.d/name2uuid.csv

export ENV_UUID_TO_STRING=false

cat ./sample.d/sample.avro |
	./avro-str2uuid |
	rs-avro2jsons
