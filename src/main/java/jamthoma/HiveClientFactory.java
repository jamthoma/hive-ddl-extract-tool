package jamthoma;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;

import java.util.ArrayList;
import java.util.List;

public class HiveClientFactory {

    private static final ThreadLocal<HiveClient> hiveClients;
    private static final List<HiveClient> allClients = new ArrayList<>(); // tracked just so they can be closed at the end

    private static final ThreadLocal<HiveMetaStoreClient> hiveMetastoreClients;
    private static final List<HiveMetaStoreClient> allMetastoreClients = new ArrayList<>(); // tracked just so they can be closed at the end

    // Initialization
    static {
        hiveClients = ThreadLocal.withInitial(() -> {
            try {
                HiveClient cli = new HiveClient();
                allClients.add(cli);
                return cli;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
        hiveMetastoreClients = ThreadLocal.withInitial(() -> {
            try {
                HiveMetaStoreClient metaStoreClient = new HiveMetaStoreClient(new HiveConf());
                allMetastoreClients.add(metaStoreClient);
                return metaStoreClient;
            } catch (Exception e) {
                e.printStackTrace();
                return null;
            }
        });
    }

    /**
     * Get a thread local hive client
     */
    public static HiveClient getHiveClient() {
        return hiveClients.get();
    }

    /**
     *
     */
    public static HiveMetaStoreClient getHiveMetaStoreClient() {
        return hiveMetastoreClients.get();
    }

    /**
     * Close all hive clients
     */
    public static void cleanup() {
        System.out.println("closing " + allClients.size() + " hive clients");
        allClients.forEach(cli -> {
            try {
                cli.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        allClients.clear();
        System.out.println("closing " + allMetastoreClients.size() + " hive metastore clients");
        allMetastoreClients.forEach(cli -> {
            try {
                cli.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        allMetastoreClients.clear();
    }

}
