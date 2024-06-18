#!/bin/bash

API_URL="https://covid-api.com/api/regions"
OUTPUT_JSON="regions_data.json"

echo "Fetching data from the API..."
curl -s $API_URL -o $OUTPUT_JSON

echo "Data has been saved to $OUTPUT_JSON"