package jamthoma;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper class for hive thrift client.
 */
public class HiveClient implements Closeable {

    private final ThriftCLIServiceClient cliServiceClient;
    private final SessionHandle sessionHandle;

    /**
     *
     */
    public HiveClient() throws Exception {
        this(new HiveConf());
    }

    /**
     *
     */
    public HiveClient(HiveConf hiveConf) throws Exception {
        // get thrift host name
        String host;
        String principal = hiveConf.get("hive.server2.authentication.kerberos.principal");
        if (principal != null) {
            //String principal = "<user>/<host>@<domain>";
            System.out.println("principal = " + principal);
            String[] names = principal.split("[/@]");
            host = names[1];
        } else {
            host = hiveConf.get("hive.server2.thrift.bind.host");
        }
        System.out.println("host = " + host);

        // get thrift port
        int port = hiveConf.getInt("hive.server2.thrift.port", 10000);
        System.out.println("port = " + port);

        // get user/pass
        String userName = hiveConf.get("hive.server2.thrift.client.user", "hive");
        String password = hiveConf.get("hive.server2.thrift.client.password", "hive");
        System.out.println("user/pass = " + userName + "/" + password);

        TTransport transport = new TSocket(host, port);
        if (principal != null)
            transport = KerberosSaslHelper.getKerberosTransport(principal, host, transport, null, false);
        else
            transport = PlainSaslHelper.getPlainTransport(userName, password, transport);

        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        transport.open();

        cliServiceClient = new ThriftCLIServiceClient(new TCLIService.Client(protocol), hiveConf);
        // NOTE: user and password appear to ot be validated for Kerberos cluster
        sessionHandle = cliServiceClient.openSession(userName, password);
    }

    /**
     * Get create SQL statement for the given table.
     *
     * @param dbName    Hive database name.
     * @param tableName Hive table name.
     * @return SQL lines
     */
    public List<String> getTableCreateDDL(String dbName, String tableName) throws Exception {
        String cmd = "SHOW CREATE TABLE `" + dbName + "." + tableName + "`";
        OperationHandle operationHandle = cliServiceClient.executeStatement(sessionHandle, cmd, null);
        RowSet results = cliServiceClient.fetchResults(operationHandle);
        List<String> lines = new ArrayList<>(64);
        for (Object[] result : results) {
            lines.add(result[0].toString());
        }
        cliServiceClient.closeOperation(operationHandle);
        return lines;
    }

    /**
     *
     */
    @Override
    public void close() throws IOException {
        if (sessionHandle == null) return;
        try {
            cliServiceClient.closeSession(sessionHandle);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException("Error closing session", e);
        }
    }

}
