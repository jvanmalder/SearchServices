#!/bin/bash
set -e

[ "$DEBUG" ] && set -x

nicebranch=`echo "$bamboo_planRepository_1_branch" | sed 's/\//_/'`

if [ "${nicebranch}" = "master" ] || [ "${nicebranch#release}" != "${nicebranch}" ]
then
   # set current working directory to the directory of the script
   cd "$bamboo_working_directory"

   tag_version=`echo "$bamboo_maven_version"`
   if [ "${bamboo_shortJobName}" = "Release" ]
   then
      tag_version=`echo "$bamboo_release_version"`
   fi

   dockerImage="quay.io/alfresco/insight-zeppelin:$tag_version"
   echo "Building $dockerImage from $nicebranch using version $tag_version"

   docker build -t $dockerImage target/docker-resources

   echo "Publishing $dockerImage..."
   docker push "$dockerImage"
   
   echo "Docker SUCCESS"
else
    echo "Only building and publishing docker images from master. Skipping for ${nicebranch}"
fi
