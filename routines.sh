#!/bin/bash


api_url="https://catmapper.org/api/routines/checkDomains/ArchaMap"

response=$(curl -ks --max-time 60000 "Content-Type: application/json" "$api_url")

echo "API Response for checkDomains in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/checkDomains/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for checkDomains in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/processUSES/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for processUSES in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/processUSES/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for processUSES in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadJSON/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadJSON in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadJSON/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadJSON in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadCMID/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadCMID in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadCMID/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadCMID in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getMultipleLabels/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getMultipleLabels in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getMultipleLabels/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getMultipleLabels in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadDomains/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadDomains in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadDomains/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadDomains in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadRelations/SocioMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadRelations in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/getBadRelations/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-Type: application/json" "$api_url")

echo "API Response for getBadRelations in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/CMNameNotInName/SocioMap"

response=$(curl -ks --max-time 600000 "Content-type: application/json" "$api_url")

echo "API Response for CMNameNotInName in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/CMNameNotInName/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-type: application/json" "$api_url")

echo "API Response for CMNameNotInName in ArchaMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/fixMetaTypes/SocioMap"

response=$(curl -ks --max-time 600000 "Content-type: application/json" "$api_url")

echo "API Response for fixMetaTypes in SocioMap:"
echo "$response"

api_url="https://catmapper.org/api/routines/fixMetaTypes/ArchaMap"

response=$(curl -ks --max-time 600000 "Content-type: application/json" "$api_url")

echo "API Response for fixMetaTypes in ArchaMap:"
echo "$response"