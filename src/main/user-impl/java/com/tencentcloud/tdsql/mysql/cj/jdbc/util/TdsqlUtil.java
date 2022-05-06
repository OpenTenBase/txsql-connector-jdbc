package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetCluster;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TdsqlUtil {

    public static List<DataSetCluster> showRoutes(Connection connection) throws SQLException {
        List<DataSetCluster> dataSetClusters = new ArrayList<>();
        try (PreparedStatement pst = connection.prepareStatement(TdsqlConst.TDSQL_SHOW_ROUTES_SQL)) {
            try (ResultSet rs = pst.executeQuery()) {
                while (rs.next()) {
                    String clusterName = rs.getString(1);
                    String master = rs.getString(2);
                    String slaves = rs.getString(3);
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
