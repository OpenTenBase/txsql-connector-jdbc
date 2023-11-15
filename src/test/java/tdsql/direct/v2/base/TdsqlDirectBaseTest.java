package tdsql.direct.v2.base;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RW;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlDataSourceUuidGenerator;
import com.tencentcloud.tdsql.mysql.cj.log.StandardLogger;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;

/**
 * <p>TDSQL专属-直连模式-测试基类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectBaseTest {

    protected static final String DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
    protected static final String PROXY_1 = "9.30.2.116:15018";
    protected static final String PROXY_2 = "9.30.2.89:15018";
    protected static final String PROXY_3 = "9.30.2.94:15018";

    protected static final String IDC_USER = "root";

    protected static final String IDC_PASS = "Azaqpvrk#ov#10391356";

    protected static final String[] PROXY_ARRAY = {PROXY_1, PROXY_2, PROXY_3};

    protected static final String DB_MYSQL = "test";
    protected static final String USER = "qt4s";
    protected static final String USER_RO = "";
    protected static final String PASS = "g<m:7KNDF.L1<^1C";

    protected static final String DEFAULT_URL_RW = "jdbc:mysql:direct://" + PROXY_1 + "," + PROXY_2 + "," + PROXY_3 + "/qt4s?tdsqlDirectReadWriteMode=rw";
    protected static final String DEFAULT_URL_RO = "jdbc:mysql:direct://" + PROXY_1 + "," + PROXY_2 + "," + PROXY_3 + "/qt4s?tdsqlDirectReadWriteMode=ro";
    protected static final String DEFAULT_CLUSTER_NAME = "test-cluster-name";
    protected static final TdsqlDirectReadWriteModeEnum DEFAULT_RW_MODE = RW;
    protected static final Boolean DEFAULT_MASTER_AS_SLAVE = false;
    protected static final String DEFAULT_MASTER_INFO = "1.1.1.1:1111@0@0";
    protected static final String[] DEFAULT_SLAVE_INFO_ARRAY_3 = new String[]{"2.2.2.2:2222@100@0@0",
            "3.3.3.3:3333@100@0@0", "4.4.4.4:4444@50@0@0"};
    protected final ConnectionUrl defaultConnectionUrlRw;
    protected final ConnectionUrl defaultConnectionUrlRo;
    protected final Properties defaultProperties;
    protected final String defaultDataSourceUuid;
    protected final HostInfo defaultMainHost;

    protected int getPort() {
        String[] ipPort = PROXY_1.split(":");
        return Integer.parseInt(ipPort[1]);
    }

    public TdsqlDirectBaseTest() {
        this.defaultProperties = new Properties();
        this.defaultProperties.put(PropertyKey.USER.getKeyName(), "qt4s");
        this.defaultProperties.put(PropertyKey.PASSWORD.getKeyName(), "g<m:7KNDF.L1<^1C");
        this.defaultConnectionUrlRw = ConnectionUrl.getConnectionUrlInstance(DEFAULT_URL_RW, this.defaultProperties);
        this.defaultConnectionUrlRo = ConnectionUrl.getConnectionUrlInstance(DEFAULT_URL_RO, this.defaultProperties);
        this.defaultDataSourceUuid = TdsqlDataSourceUuidGenerator.generateUuid(this.defaultConnectionUrlRw);
        this.defaultMainHost = this.defaultConnectionUrlRw.getMainHost();
    }

    @BeforeAll
    public static void setUp() {
        TdsqlLoggerFactory.setLogger(new StandardLogger("123"));
    }

    protected TdsqlDirectMasterTopologyInfo newMasterTopologyInfo() {
        return this.newMasterTopologyInfo(DEFAULT_MASTER_INFO);
    }

    protected TdsqlDirectMasterTopologyInfo newMasterTopologyInfo(String masterInfoStr) {
        return new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid, masterInfoStr);
    }

    protected Set<TdsqlDirectSlaveTopologyInfo> newSlaveTopologyInfoSet() {
        return this.newSlaveTopologyInfoSet(DEFAULT_SLAVE_INFO_ARRAY_3);
    }

    protected Set<TdsqlDirectSlaveTopologyInfo> newSlaveTopologyInfoSet(String... slaveInfos) {
        return Arrays.stream(slaveInfos)
                .map(slaveInfoStr -> new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, slaveInfoStr))
                .collect(Collectors.toCollection(() -> new LinkedHashSet<>(slaveInfos.length)));
    }

    protected TdsqlDirectTopologyInfo newTopologyInfo() {
        return this.newTopologyInfo(defaultDataSourceUuid, DEFAULT_CLUSTER_NAME, DEFAULT_RW_MODE,
                DEFAULT_MASTER_AS_SLAVE, this.newMasterTopologyInfo(), this.newSlaveTopologyInfoSet());
    }

    protected TdsqlDirectTopologyInfo newTopologyInfo(String dataSourceUuid, String clusterName) {
        return this.newTopologyInfo(dataSourceUuid, clusterName, this.newMasterTopologyInfo(),
                this.newSlaveTopologyInfoSet());
    }

    protected TdsqlDirectTopologyInfo newTopologyInfo(TdsqlDirectMasterTopologyInfo masterInfo,
            Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet) {
        return this.newTopologyInfo(defaultDataSourceUuid, DEFAULT_CLUSTER_NAME, masterInfo, slaveTopologyInfoSet);
    }

    protected TdsqlDirectTopologyInfo newTopologyInfo(String dataSourceUuid, String clusterName,
            TdsqlDirectMasterTopologyInfo masterInfo, Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet) {
        return this.newTopologyInfo(dataSourceUuid, clusterName, DEFAULT_RW_MODE, DEFAULT_MASTER_AS_SLAVE, masterInfo,
                slaveTopologyInfoSet);
    }

    protected TdsqlDirectTopologyInfo newTopologyInfo(String dataSourceUuid, String clusterName,
            TdsqlDirectReadWriteModeEnum rwMode, Boolean masterAsSlave, TdsqlDirectMasterTopologyInfo masterInfo,
            Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet) {
        StringJoiner joiner = new StringJoiner(",");
        for (TdsqlDirectSlaveTopologyInfo info : slaveTopologyInfoSet) {
            joiner.add(info.getSlaveInfoStr());
        }
        return this.newTopologyInfo(dataSourceUuid, clusterName, rwMode, masterAsSlave, masterInfo.getOriginalInfoStr(),
                joiner.toString());
    }

    protected TdsqlDirectTopologyInfo newTopologyInfo(String dataSourceUuid, String clusterName,
            TdsqlDirectReadWriteModeEnum rwMode, Boolean masterAsSlave, String masterInfoStr, String slavesInfoStr) {
        return new TdsqlDirectTopologyInfo(dataSourceUuid, clusterName, rwMode, masterAsSlave, masterInfoStr,
                slavesInfoStr);
    }
}
