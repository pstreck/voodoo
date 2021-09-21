#!/bin/bash

DATABASE=voodoo
CARDS_COLLECTION=cards
DECKS_COLLECTION=decks
DATA_DIR=./data/voodoo
CARDS_EXTRACT_CSV=cards_extract.csv
DECKS_EXTRACT_JSON=decks_extract.json

if (( $# < 3)); then
  printf "usage: ${0##*/} <mongo_host> <mongo_username> <mongo_password>\n"
  exit
fi

mongo_host=$1
mongo_username=$2
mongo_password=$3

mkdir -p ${DATA_DIR}

printf "extracting cards to ${DATA_DIR}/${CARDS_EXTRACT_CSV}\n"
rm -f ${DATA_DIR}/${CARDS_EXTRACT_CSV}
mongoexport -h ${mongo_host} --authenticationDatabase=admin -d ${DATABASE} -c ${CARDS_COLLECTION} -u ${mongo_username} -p ${mongo_password} --type=csv -f name,voodooId -o ${DATA_DIR}/${CARDS_EXTRACT_CSV}

printf "extracting decks to ${DATA_DIR}/${DECKS_EXTRACT_JSON}\n"
rm -f ${DATA_DIR}/${DECKS_EXTRACT_JSON}
mongoexport -h ${mongo_host} --authenticationDatabase=admin -d ${DATABASE} -c ${DECKS_COLLECTION} -u ${mongo_username} -p ${mongo_password} --type=json -o ${DATA_DIR}/${DECKS_EXTRACT_JSON}
