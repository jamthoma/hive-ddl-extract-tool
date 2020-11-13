# hive-ddl-extract-tool
Simple tool to extract hive DDL (e.g. for migrating tables between clusters).
Given a Hive database pattern and hive table pattern, the create DDL will be written to an output file.
The DDL also includes SQL to restore partition tables, as needed.  

This tool was created for use in a Kerberized Hadoop 2.7.x cluster and Hive 2.3.x.
Compatibility on other cluster has not been tested. 

To execute run the command, we leverage the `hadoop jar` command to provide the necessary Hadoop library dependencies.
To properly setup the classpath for Hive, jar files under `${HIVE_HOME}/lib` should be added to the classpath,
as well as teh Hive configuration files from `${HIVE_CONF_DIR}`.
The `extract_hive_ddl.sh` script provides the necessary logic to setup the classpath and execute the hadoop command.

Usage:
`./extract_hive_ddl.sh <database pattern> <table name pattern> <output file>`

Examples:

1. Extract DDL for the `fruits` table from the `default` database:<br>
`./extract_hive_ddl.sh default fruits  fruits_DDL.sql`

2. Extract DDL for the all tables from the `default` database:<br>
`./extract_hive_ddl.sh default "*"  default_all.sql`

**Notes on partitioned tables**

By default, the DDL for restoring partitions will use the `MSCK` command to repair the table partitions.
For a large number of partitions, this is more efficient than running `ADD PARTTION` many times.

To change the default behavior and use `ADD PARTITON` sql, set the environment variable `USE_ADD_SQL` to `true`.<br>
For example, in bash shell, `export USE_ADD_SQL=true`  

1. Tables which use teh default partition value `__HIVE_DEFAULT_PARTITION__`
For these tables, `ADD PARTITION` SQL will fail because `__HIVE_DEFAULT_PARTITION__` is a Hive keyword.
As a result, `MSCK` must always be used.

2. Tables with HDFS path containing uppercase characters.
For these tables, the `MSCK` command was not working.  Therefore the code will always rely on `ADD PARTITION` commands to restore partitions.

The tool will throw an exception if both edge cases are true:<br>
(a) table use default hive partition<br>
(b) partition locations contain uppercase characters<br>
