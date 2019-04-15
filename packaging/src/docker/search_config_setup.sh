#!/bin/bash
set -e

SOLR_CONF_FOLDER=$PWD/solrhome/templates/rerank/conf
SOLR_CONFIG_FILE=$SOLR_CONF_FOLDER/solrconfig.xml
SOLR_CORE_FILE=$SOLR_CONF_FOLDER/solrcore.properties

if [[ $REPLICATION_TYPE == "master" ]]; then

   findStringMaster='<requestHandler name="\/replication" class="solr\.ReplicationHandler" >'

   replaceStringMaster="\n\t<lst name=\"master\"> \n"

   if [[ $REPLICATION_AFTER == "" ]]; then
      REPLICATION_AFTER=commit
   fi

   for i in $(echo $REPLICATION_AFTER | sed "s/,/ /g")
   do
      replaceStringMaster+="\t\t<str name=\"replicateAfter\">"$i"<\/str> \n"
   done

   if [[ ! -z "$REPLICATION_CONFIG_FILES" ]]; then
      replaceStringMaster+="\t\t<str name=\"confFiles\">$REPLICATION_CONFIG_FILES<\/str> \n"
   fi

   replaceStringMaster+="\t<\/lst>"

   sed -i -e "s/$findStringMaster/$findStringMaster$replaceStringMaster/g" $SOLR_CONFIG_FILE
   sed -i -e "s/enable.alfresco.tracking=true/enable.alfresco.tracking=true\nenable.master=true\nenable.slave=false/g" $SOLR_CORE_FILE
fi

if [[ $REPLICATION_TYPE == "slave" ]]; then

   if [[ $REPLICATION_MASTER_PROTOCOL == "" ]]; then
      REPLICATION_MASTER_PROTOCOL=http
   fi

   if [[ $REPLICATION_MASTER_HOST == "" ]]; then
      REPLICATION_MASTER_HOST=localhost
   fi

   if [[ $REPLICATION_MASTER_PORT == "" ]]; then
      REPLICATION_MASTER_PORT=8083
   fi

   if [[ $REPLICATION_CORE_NAME == "" ]]; then
      REPLICATION_CORE_NAME=alfresco
   fi

   if [[ $REPLICATION_POLL_INTERVAL == "" ]]; then
      REPLICATION_POLL_INTERVAL=00:00:60
   fi

   sed -i -e 's/<requestHandler name="\/replication" class="solr\.ReplicationHandler" >/<requestHandler name="\/replication" class="solr\.ReplicationHandler" >\
      <lst name="slave">\
         <str name="masterUrl">'$REPLICATION_MASTER_PROTOCOL':\/\/'$REPLICATION_MASTER_HOST':'$REPLICATION_MASTER_PORT'\/solr\/'$REPLICATION_CORE_NAME'<\/str>\
         <str name="pollInterval">'$REPLICATION_POLL_INTERVAL'<\/str>\
      <\/lst>/g' $SOLR_CONFIG_FILE
   sed -i -e "s/enable.alfresco.tracking=true/enable.alfresco.tracking=true\nenable.master=false\nenable.slave=true/g" $SOLR_CORE_FILE
fi

SOLR_IN_FILE=$PWD/solr.in.sh

if [[ ! -z "$MAX_SOLR_RAM_PERCENTAGE" ]]; then
   MEM_CALC=$(expr $(cat /proc/meminfo | grep MemAvailable | awk '{print $2}') \* $MAX_SOLR_RAM_PERCENTAGE / 100)
   SOLR_MEM="-Xms${MEM_CALC}k -Xmx${MEM_CALC}k"
   sed -i -e "s/.*SOLR_JAVA_MEM=.*/SOLR_JAVA_MEM=\"${SOLR_MEM}\"/g" $SOLR_IN_FILE
fi

if [[ ! -z "$SOLR_HEAP" ]]; then
   sed -i -e "s/.*SOLR_HEAP=.*/SOLR_HEAP=\"$SOLR_HEAP\"/g" $SOLR_IN_FILE
fi


if [[ ! -z "$SOLR_JAVA_MEM" ]]; then
   sed -i -e "s/.*SOLR_JAVA_MEM=.*/SOLR_JAVA_MEM=\"$SOLR_JAVA_MEM\"/g" $SOLR_IN_FILE
fi

bash -c "$@"