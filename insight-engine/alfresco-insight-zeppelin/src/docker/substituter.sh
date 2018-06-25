#!/bin/bash
set -e

if [[ $REPO_HOST == "" ]]; then
   REPO_HOST=alfresco
fi

if [[ $REPO_PORT == "" ]]; then
   REPO_PORT=8080
fi

echo "Replace 'REPO_HOST' with '$REPO_HOST' and 'REPO_PORT' with '$REPO_PORT'"

sed -i -e 's/REPO_HOST:REPO_PORT/'"$REPO_HOST:$REPO_PORT"'/g' /zeppelin/conf/shiro.ini /zeppelin/conf/interpreter.json

bash -c "$@"