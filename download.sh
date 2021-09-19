#!/bin/bash

DATA_DIR=./data
MTGJSON_DATA_DIR=${DATA_DIR}/mtg_json
MTGO_DECKLIST_CACHE_DIR=${DATA_DIR}/mtgo_decklist_cache
ATOMIC_CARDS_JSON_URL=https://mtgjson.com/api/v5/AtomicCards.json
SET_LIST_JSON_URL=https://mtgjson.com/api/v5/SetList.json
MTGO_DECKLIST_CACHE_GIT_URI=https://github.com/Badaro/MTGODecklistCache.git

mkdir -p ${DATA_DIR}
rm -f ${MTGJSON_DATA_DIR}/*.json
rm -rf ${MTGO_DECKLIST_CACHE_DIR}

wget -P ${MTGJSON_DATA_DIR} ${ATOMIC_CARDS_JSON_URL}
wget -P ${MTGJSON_DATA_DIR} ${SET_LIST_JSON_URL}
git clone ${MTGO_DECKLIST_CACHE_GIT_URI} ${MTGO_DECKLIST_CACHE_DIR}
