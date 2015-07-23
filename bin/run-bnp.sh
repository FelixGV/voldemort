#!/bin/bash

base_dir=$(cd $(dirname $0)/.. && pwd)

HADOOP_CONFIG_PATH_DEFAULT=/etc/hadoop/conf/

if [[ $# < 1 ]]; then
	echo "Usage: $0 config_file [hadoop_config_path]"
	echo ""
	echo "config_file : The file containing necessary config values, in key=value format."
	echo "hadoop_config_path : The path for *-site.xml files. Defaults to $HADOOP_CONFIG_PATH_DEFAULT"
	exit 1
fi

CONFIG_FILE=$1

if [[ $# > 1 ]]; then
	HADOOP_CONFIG_PATH=$2
else
	HADOOP_CONFIG_PATH=$HADOOP_CONFIG_PATH_DEFAULT
fi

VERSION=`grep 'curr.release' $base_dir/gradle.properties | sed -e 's/curr.release=\(.*\)/\1/'`
echo "Voldemort version detected: $VERSION"
echo "Executing BnP with:"
echo "config_file : $CONFIG_FILE"
echo "hadoop_config_path : $HADOOP_CONFIG_PATH"

CLASSPATH="$base_dir/dist/voldemort-$VERSION.jar:$base_dir/dist/voldemort-contrib-$VERSION.jar:$base_dir/lib/*:$HADOOP_CONFIG_PATH"

java -cp $CLASSPATH voldemort.store.readonly.mr.azkaban.VoldemortBuildAndPushJobRunner $CONFIG_FILE
