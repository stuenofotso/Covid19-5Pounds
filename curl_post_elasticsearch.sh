#!/bin/bash

param=$1
curl -H 'Content-Type: application/json' -u "${param}" -XPOST "http://"$2"/"$3"/"$4"/_bulk?pretty" --data-binary @$5