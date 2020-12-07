package jamthoma;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 */
public class ExtractHiveDDL {

    private static PrintWriter writer;

    /**
     * if true, "ADD PARTITION" is always used, unless default partition is found, otherwise MSCK is used
     */
    private static final boolean USE_ADD_SQL = getBoolEnv("USE_ADD_SQL", false);
    /**
     * if false, table names and path are fully qualified;
     */
    private static final boolean USE_CONTEXT = getBoolEnv("USE_CONTEXT", true);

    /**
     *
     */
    public static void main(String[] args) {
        System.out.println(args.length + " args: " + Arrays.asList(args));
        if (args.length != 3) {
            System.out.println("Usage: ");
            System.out.println("arg[0] = database name pattern");
            System.out.println("arg[1] = table name pattern");
            System.out.println("arg[2] = output file name");
            System.exit(-1);
        }
        String databasePattern = args[0];
        String tablePattern = args[1];
        File outFile = new File(args[2]);

        System.out.println("database pattern = " + databasePattern);
        System.out.println("table pattern = " + tablePattern);
        System.out.println("output file = " + outFile);
        System.out.println("use add partition SQL = " + USE_ADD_SQL);
        System.out.println("fully qualify table names = " + !USE_CONTEXT);

        long st = System.currentTimeMillis();
        try {
            writer = new PrintWriter(new BufferedOutputStream(new FileOutputStream(outFile)));

            // count total number of tables
            List<String> dbNames = HiveClientFactory.getHiveMetaStoreClient().getDatabases(databasePattern);
            System.out.println(dbNames.size() + " databases");
            Integer totalTables = dbNames.stream().mapToInt(dbName -> getTableNames(dbName, tablePattern).size()).sum();
            System.out.println(totalTables + " total tables");

            // process all databases
            dbNames.forEach(dbName -> writeDatabaseSQL(dbName, tablePattern));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cleanup();
        }
        long duration = System.currentTimeMillis() - st;
        System.out.println("Total time = " + duration + " (" + duration / 1000 + " seconds)");
        System.exit(0);
    }

    /**
     *
     */
    private static boolean getBoolEnv(String name, boolean defaultVal) {
        String val = System.getenv(name);
        if (val == null) return defaultVal;
        return Boolean.parseBoolean(val);
    }

    /**
     *
     */
    private static void cleanup() {
        HiveClientFactory.cleanup();
        if (writer != null) {
            writer.close();
        }
        writer = null;
    }

    /**
     *
     */
    private static void writeDatabaseSQL(String dbName, String tablePattern) {
        // Database Create/Use
        writer.println("CREATE DATABASE IF NOT EXISTS " + dbName + ";");
        if (USE_CONTEXT) {
            writer.println("USE " + dbName + ";");
            writer.println();
        }

        List<String> tableNames = getTableNames(dbName, tablePattern);
        assert tableNames != null;
        tableNames.parallelStream().forEach(tableName -> writeTableSQL(dbName, tableName));
        //tableNames.forEach(tableName -> writeTableSQL(dbName, tableName));

        HiveClientFactory.getHiveMetaStoreClient().flushCache();
    }

    /**
     *
     */
    private static void writeTableSQL(String dbName, String tableName) {
        System.out.println("Processing table " + dbName + "." + tableName);

        String createSQL = getTableCreateSQL(dbName, tableName);
        List<String> addPartitionSqlLines = getTablePartitions(dbName, tableName);
        assert addPartitionSqlLines != null;

        writer.println();
        writer.println("--------------------------------------");
        writer.println("-- " + tableName);
        writer.println("--------------------------------------");
        writer.println("!sh echo \"Creating table: " + tableName + "...\";");
        writer.println(createSQL);
        if (!addPartitionSqlLines.isEmpty()) {
            writer.println();
            writer.println("!sh echo \"adding partitions: " + dbName + "." + tableName + "...\";");
            addPartitionSqlLines.forEach(line -> writer.println(line));
        }
    }

    /**
     *
     */
    private static List<String> getTableNames(String dbName, String tablePattern) {
        try {
            return HiveClientFactory.getHiveMetaStoreClient().getTables(dbName, tablePattern);
        } catch (Exception e) {
            System.err.println("Error listing tables in " + dbName);
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    /**
     *
     */
    private static String getTableCreateSQL(String dbName, String tableName) {
        try {
            List<String> lines = HiveClientFactory.getHiveClient().getTableCreateDDL(dbName, tableName);
            if (lines.size() > 0) {
                // fix "CREATE TABLE"
                //    CREATE TABLE `default.test` ==> CREATE TABLE `default`.`test`
                String line = lines.get(0);
                if (line.startsWith("CREATE TABLE")) {
                    lines.set(0, fixCreateTable(line));
                }
            }
            StringBuilder sqlSB = new StringBuilder();
            for (String line : lines)
                sqlSB.append(line).append('\n');
            sqlSB.setLength(sqlSB.length() - 1); // remove tailing newline
            sqlSB.append(';'); // end of statement
            return sqlSB.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *
     */
    private static String fixCreateTable(String line) {
        int si = line.indexOf('`');
        int ei = line.indexOf('.');
        if (si < ei && si != -1) {
            String dbPart = line.substring(0, ei);
            String tablePart = line.substring(ei + 1);
            if (!dbPart.endsWith("`")) {
                line = dbPart + "`.`" + tablePart;
            }
        }
        return line;
    }

    /**
     *
     */
    private static List<String> getTablePartitions(String dbName, String tableName) {
        try {
            HiveMetaStoreClient metaStoreClient = HiveClientFactory.getHiveMetaStoreClient();
            List<String> pNames = metaStoreClient.listPartitionNames(dbName, tableName, Short.MAX_VALUE);
            if (pNames.size() == 0) {
                // no partitions, nothing to do
                return Collections.emptyList();
            }

            boolean useAddSql = USE_ADD_SQL;  // vs always use "MSCK REPAIR TABLE <table>"
            String tablePath = metaStoreClient.getTable(dbName, tableName).getSd().getLocation() + "/";
            List<Partition> pList = metaStoreClient.listPartitions(dbName, tableName, Short.MAX_VALUE);

            // always use MSCK if default partition is found, or
            // always use SQL to add partitions if paths are not all in lower case
            // error is throw if both are true
            boolean hasDefaultPartition = false;
            for (String name : pNames) {
                if (name.contains("=__HIVE_DEFAULT_PARTITION__")) {
                    hasDefaultPartition = true;
                    break;
                }
            }
            boolean hasNonLowercase = false;
            for (Partition partition : pList) {
                // get relative path (to only check partition cols and values)
                String pLoc = "\"" + partition.getSd().getLocation().replace(tablePath, "") + "\"";
                //System.out.println("partition location = " + pLoc );
                if (!pLoc.toLowerCase().equals(pLoc)) {
                    hasNonLowercase = true;
                    break;
                }
            }
            //System.out.println("hasDefaultPartition = " + hasDefaultPartition);
            //System.out.println("hasNonLowercase = " + hasNonLowercase);
            if (hasDefaultPartition && hasNonLowercase)
                throw new Exception("Table " + tableName + "has default partition and non-lower case chars");
            if (hasDefaultPartition) useAddSql = false;
            if (hasNonLowercase) useAddSql = true;
            //System.out.println(tableName + ": hasDefaultPartition=" + hasDefaultPartition + ", hasNonLowercase=" + hasNonLowercase + ", useAddSql = " + useAddSql);

            /// list of SQL to restore/add partitions
            List<String> lines = new ArrayList<>(100);
            if (useAddSql) {
                for (int p = 0; p < pList.size(); p++) {
                    Partition partition = pList.get(p);
                    String partitionName = pNames.get(p);
                    String sql = getAddPartitionSQL(dbName, tableName, tablePath, partitionName, partition);
                    lines.add(sql);
                    //val sqlList = tmp.map( d => getAddPartitionSQL(d._1.toString,d._2.asInstanceOf[org.apache.hadoop.hive.metastore.api.Partition],tableName,tablePath))
                }
            } else {
                if (USE_CONTEXT)
                    lines.add("MSCK REPAIR TABLE " + tableName + ";");
                else
                    lines.add("MSCK REPAIR TABLE " + dbName + "." + tableName + ";");
            }
            return lines;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     *
     */
    private static String getAddPartitionSQL(String dbName, String tableName, String tablePath, String partitionName, Partition partition) {
        String pName = partitionName.replaceAll("=", "='") + "'";
        pName = pName.replaceAll("/", "',"); // mulitple partition columns
        String tName;
        String pLoc;
        if (USE_CONTEXT) {
            tName = tableName;
            pLoc = "\"" + partition.getSd().getLocation().replace(tablePath, "") + "\"";
        } else {
            tName = dbName + "." + tableName;
            pLoc = "\"" + partition.getSd().getLocation() + "\"";
        }
        return "ALTER TABLE " + tName + " ADD PARTITION (" + pName + ") LOCATION " + pLoc + ";";
    }

}
