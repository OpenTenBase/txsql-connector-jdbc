package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCluster;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

public class TdsqlUtil {

    private final static Executor immediateExecutor = Runnable::run;

    public static List<DataSetCluster> showRoutes(Connection connection) throws SQLException {
        connection.setNetworkTimeout(immediateExecutor, TdsqlConst.TDSQL_SHOW_ROUTES_CONN_TIMEOUT);
        List<DataSetCluster> dataSetClusters = new ArrayList<>();
        try (PreparedStatement pst = connection.prepareStatement(TdsqlConst.TDSQL_SHOW_ROUTES_SQL)) {
            pst.setQueryTimeout(TdsqlConst.TDSQL_SHOW_ROUTES_TIMEOUT_SECONDS);
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    String clusterName = rs.getString(1);
                    String master = rs.getString(2);
                    String slaves = rs.getString(4);
                    DataSetCluster dataSetCluster = new DataSetCluster(clusterName);
                    dataSetCluster.setMaster(DataSetUtil.parseMaster(master));
                    dataSetCluster.setSlaves(DataSetUtil.parseSlaveList(slaves));
                    dataSetClusters.add(dataSetCluster);
                }
            }
        }
        return dataSetClusters;
    }

}
