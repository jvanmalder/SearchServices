#!/bin/bash
set -e

cd "$(dirname "$0")"

propertiesFile="../zeppelin.properties"
if [ -f "$propertiesFile" ]
then
   if [ -z "$REPO_PROTOCOL" ]
   then
      alfresco_repository_protocol=$(grep -i 'alfresco_repository_protocol' $propertiesFile  | cut -f2 -d'=')
   else
      alfresco_repository_protocol=$REPO_PROTOCOL
   fi

   if [ -z "$REPO_HOST" ]
   then
      alfresco_repository_host=$(grep -i 'alfresco_repository_host' $propertiesFile  | cut -f2 -d'=')
   else
      alfresco_repository_host=$REPO_HOST
   fi

   if [ -z "$REPO_PORT" ]
   then
      alfresco_repository_port=$(grep -i 'alfresco_repository_port' $propertiesFile  | cut -f2 -d'=')
   else
      alfresco_repository_port=$REPO_PORT
   fi

  sed -i -e 's/REPO_PROTOCOL:\/\/REPO_HOST:REPO_PORT/'"$alfresco_repository_protocol:\/\/$alfresco_repository_host:$alfresco_repository_port"'/g' ../conf/shiro.ini
  sed -i -e 's/REPO_HOST:REPO_PORT/'"$alfresco_repository_host:$alfresco_repository_port"'/g' ../conf/interpreter.json

  echo "Replaced 'REPO_PROTOCOL' with '$alfresco_repository_protocol', 'REPO_HOST' with '$alfresco_repository_host' and 'REPO_PORT' with '$alfresco_repository_port'"
else
  echo "$propertiesFile NOT found."
fi
