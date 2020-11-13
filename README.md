# hive-ddl-extract-tool
Simple tool to extract hive DDL (e.g. for migrating tables between clusters).
Given a Hive database name pattern and hive table name pattern, the create DDL will be written to an output file.
The DDL also includes SQL to restore partition tables, as needed.  

When migrating to another cluster, a global replace of the hostname of the HDFS location will like need to be replaced.
`sed` command works nicely for this.<br>
For example,<br>
`sed -i 's/nn1-hdfs.your.domain.com/nn2-hdfs.your.domain.com/g' table_DDL.sql`

This tool was originally created for use in a Kerberized Hadoop 2.7.x cluster running Hive 2.3.x.
Compatibility on other clusters has not been tested. 

To execute the tool, we leverage the `hadoop jar` command to provide the necessary Hadoop library dependencies.
To properly setup the classpath for Hive, jar files under `${HIVE_HOME}/lib` should be added to the classpath,
as well as the Hive configuration files from `${HIVE_CONF_DIR}`.
The `extract_hive_ddl.sh` script provides the necessary logic to setup the classpath and execute the `hadoop jar` command.

Usage:
`./extract_hive_ddl.sh <database name pattern> <table name pattern> <output file>`

Examples:

1. Extract DDL for the `fruits` table from the `default` database:<br>
`./extract_hive_ddl.sh default fruits  fruits_DDL.sql`

2. Extract DDL for the all tables from the `default` database:<br>
`./extract_hive_ddl.sh default "*"  default_all.sql`

**Notes on partitioned tables**

By default, the DDL for restoring partitions will use the `MSCK` command to repair the table partitions.
For a large number of partitions, this is more efficient than running `ADD PARTTION` many times.

To change the default behavior and use `ADD PARTITON` sql, set the environment variable `USE_ADD_SQL` to `true`.<br>
For example, in bash shell, run the following command:<br>
 `export USE_ADD_SQL=true`  

1. Tables which use the default partition value `__HIVE_DEFAULT_PARTITION__`
For these tables, `ADD PARTITION` SQL will fail because `__HIVE_DEFAULT_PARTITION__` is a Hive keyword.
As a result, `MSCK` will always be used for these tables.

2. Tables with HDFS paths containing uppercase characters.
For these tables, the `MSCK` command was not working in our environment.
Therefore, `ADD PARTITION` SQL will be used to restore all the partitions in these tables.

The tool will throw an exception if both edge cases are true for a given table:<br>
(a) uses default hive partition<br>
(b) a partition location contain uppercase characters<br>
