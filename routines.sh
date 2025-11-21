#!/bin/bash

api_url="https://api.catmapper.org/runRoutines/all"

response=$(curl -v -ks --max-time 600000 -H "Content-Type: application/json" "$api_url")

echo "API Response for runRoutines:"
echo "$response"