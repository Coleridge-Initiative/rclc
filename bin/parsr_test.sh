#!/usr/bin/env bash

PARSR=34.82.183.17:3001

curl -X GET \
     http://$PARSR/api/v1/queue/1ec3b910b86f2bb329684cd5763d56


exit 0

curl -X POST \
     http://$PARSR/api/v1/document \
     -H 'Content-Type: multipart/form-data' \
     -F 'file=@resources/pub/pdf/a6024f82cef41d533019.pdf;type=application/pdf' \
     -F 'config=@bin/sampleConfig.json;type=application/json'
