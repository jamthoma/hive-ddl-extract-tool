#!/bin/bash

# add hive configuration files to classpath
export HIVE_CONF_DIR=${HIVE_HOME}/conf
CLASSPATH="${HIVE_CONF_DIR}"

# add all hive jar files to classpath
HIVE_LIB=${HIVE_HOME}/lib
for f in ${HIVE_LIB}/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done

# pass classpath to hadoop
if [ "$HADOOP_CLASSPATH" != "" ]; then
  export HADOOP_CLASSPATH="${CLASSPATH}:${HADOOP_CLASSPATH}"
else
  export HADOOP_CLASSPATH="$CLASSPATH"
fi

# adjust the memory allocation as needed
# larger # of databases & tables can require more memory
export HADOOP_CLIENT_OPTS="-Xms4g -Xmx4g $HADOOP_CLIENT_OPTS"

# 
JAR_NAME=hive-ddl-extract-tool-1.0-SNAPSHOT.jar
JAR_FILE=$(find . -name hive-ddl-extract-tool-1.0-SNAPSHOT.jar -print)
echo "JAR_FILE=$JAR_FILE"

hadoop jar ${JAR_FILE} "$1" "$2" "$3"
