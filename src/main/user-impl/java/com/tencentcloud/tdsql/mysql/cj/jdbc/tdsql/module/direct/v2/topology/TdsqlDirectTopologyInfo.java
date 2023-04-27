package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.DOT_MARK;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectParseTopologyException;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

/**
 * <p>
 * +------------------------+----------------+-----------+
 * | cluster_name           | cluster_status | dcn_slave |
 * +------------------------+----------------+-----------+
 * | set_1668741553_9550901 | 0              | 0         |
 * +------------------------+----------------+-----------+
 * +----------------------+-----------------------------------------------------------------------+
 * | master_ip            | slave_iplist                                                          |
 * +----------------------+-----------------------------------------------------------------------+
 * | 9.30.2.89:4017@100@0 | 9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0 |
 * +----------------------+-----------------------------------------------------------------------+
 * </p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectTopologyInfo {

    private final String dataSourceUuid;
    private final TdsqlDirectReadWriteModeEnum rwMode;
    private final String clusterName;
    private TdsqlDirectMasterTopologyInfo masterTopologyInfo;
    private Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet;

    public TdsqlDirectTopologyInfo(String dataSourceUuid, String clusterName, TdsqlDirectReadWriteModeEnum rwMode,
            Boolean masterAsSlave, String masterInfoStr, String slavesInfoStr) {
        this.dataSourceUuid = dataSourceUuid;
        this.rwMode = rwMode;
        this.clusterName = clusterName;

        if (StringUtils.isEmptyOrWhitespaceOnly(masterInfoStr)) {
            if (TdsqlDirectReadWriteModeEnum.RW.equals(rwMode)) {
                throw TdsqlExceptionFactory.logException(dataSourceUuid, TdsqlDirectParseTopologyException.class,
                        Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfMasterInfo"));
            } else {
                this.masterTopologyInfo = TdsqlDirectMasterTopologyInfo.emptyMaster();
            }
        } else {
            this.masterTopologyInfo = new TdsqlDirectMasterTopologyInfo(dataSourceUuid, masterInfoStr);
        }

        if (StringUtils.isEmptyOrWhitespaceOnly(slavesInfoStr)) {
            if (TdsqlDirectReadWriteModeEnum.RO.equals(rwMode)) {
                if (masterAsSlave) {
                    if (this.masterTopologyInfo.isEmptyMaster()) {
                        throw TdsqlExceptionFactory.logException(dataSourceUuid,
                                TdsqlDirectParseTopologyException.class, Messages.getString(
                                        "TdsqlDirectParseTopologyException.EmptyValueOfSlavesInfoAlthoughMasterAsSlave"));
                    } else {
                        this.slaveTopologyInfoSet = Collections.emptySet();
                        TdsqlLoggerFactory.logWarn(dataSourceUuid, Messages.getString(
                                "TdsqlDirectParseTopologyException.NotEmptyMasterEmptySlaveInMasterAsSlave"));
                    }
                } else {
                    throw TdsqlExceptionFactory.logException(dataSourceUuid, TdsqlDirectParseTopologyException.class,
                            Messages.getString("TdsqlDirectParseTopologyException.EmptyValueOfSlavesInfo"));
                }
            } else {
                this.slaveTopologyInfoSet = Collections.emptySet();
            }
        } else {
            List<String> tempList = StringUtils.split(slavesInfoStr, DOT_MARK, /*trim*/ true);
            this.slaveTopologyInfoSet = new LinkedHashSet<>(tempList.size());
            for (String slaveInfoStr : tempList) {
                this.slaveTopologyInfoSet.add(new TdsqlDirectSlaveTopologyInfo(dataSourceUuid, slaveInfoStr));
            }
        }
    }

    public void updateMaster(TdsqlDirectMasterTopologyInfo masterTopologyInfo) {
        this.masterTopologyInfo = masterTopologyInfo;
    }

    public void updateSlaveSet(Set<TdsqlDirectSlaveTopologyInfo> slaveTopologyInfoSet) {
        this.slaveTopologyInfoSet = slaveTopologyInfoSet;
    }

    public String printPretty() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(this.masterTopologyInfo.printPretty());
        this.slaveTopologyInfoSet.stream().map(TdsqlDirectSlaveTopologyInfo::printPretty).forEach(joiner::add);
        return joiner.toString();
    }

    public String getDataSourceUuid() {
        return dataSourceUuid;
    }

    public TdsqlDirectReadWriteModeEnum getRwMode() {
        return rwMode;
    }

    public String getClusterName() {
        return clusterName;
    }

    public TdsqlDirectMasterTopologyInfo getMasterTopologyInfo() {
        return masterTopologyInfo;
    }

    public Set<TdsqlDirectSlaveTopologyInfo> getSlaveTopologyInfoSet() {
        return slaveTopologyInfoSet;
    }
}
