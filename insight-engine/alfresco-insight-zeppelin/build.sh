#!/bin/bash
set -e

[ "$DEBUG" ] && set -x

# set current working directory to the directory of the script
cd "$(dirname "$0")"

nicebranch=`echo "$bamboo_planRepository_1_branch" | sed 's/\//_/'`
dockerImage="docker-internal.alfresco.com/insight-zeppelin:$bamboo_maven_version"
echo "Building $dockerImage from $nicebranch using version $bamboo_maven_version"

docker build --build-arg branch=$nicebranch --build-arg version=$bamboo_maven_version -t $dockerImage target

if [ "${nicebranch}" = "local" ]
then
    echo "Skipping docker publish for local build"
else
    echo "Publishing $dockerImage..."
    docker push "$dockerImage"
fi

echo "Docker SUCCESS"
