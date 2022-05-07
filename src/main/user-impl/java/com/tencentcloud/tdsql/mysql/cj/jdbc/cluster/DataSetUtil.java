package com.tencentcloud.tdsql.mysql.cj.jdbc.cluster;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.TDSQLRouteParseException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DataSetUtil {

    private static final String separator = "@";
    private static final String endpointSeparator = ":";
    private static final String listSeparator = ",";

    public static DataSetInfo parseMaster(String masterString) throws SQLException {
        String[] masterSplit = masterString.split(separator);
        if(masterSplit.length < 3) {
            throw new TDSQLRouteParseException("Invalid master info length: " + masterString);
        }
        String[] masterIPPort = parseEndpoint(masterSplit[0]);
        DataSetInfo masterInfo = new DataSetInfo(masterIPPort[0], masterIPPort[1]);
        masterInfo.setWeight(parseWeight(masterSplit[1]));
        masterInfo.setAlive(parseAlive(masterSplit[1]));
        return masterInfo;
    }

    public static DataSetInfo parseSlave(String slaveString) throws SQLException {
        String[] slaveSplit = slaveString.split(separator);
        if(slaveSplit.length < 4) {
            throw new TDSQLRouteParseException("Invalid slave info length: " + slaveString);
        }
        String[] slaveIPPort = parseEndpoint(slaveSplit[0]);
        DataSetInfo slaveInfo = new DataSetInfo(slaveIPPort[0], slaveIPPort[1]);
        slaveInfo.setWeight(parseWeight(slaveSplit[1]));
        slaveInfo.setWatch(parseWatch(slaveSplit[2]));
        slaveInfo.setDelay(parseDelay(slaveSplit[3]));
        return slaveInfo;
    }

    public static List<DataSetInfo> parseSlaveList(String slaveListString) throws SQLException {
        List<DataSetInfo> dataSetInfos = new ArrayList<>();
        String[] slaveListSplit = slaveListString.split(listSeparator);
        for (String s : slaveListSplit) {
            dataSetInfos.add(parseSlave(s));
        }
        return dataSetInfos;
    }

    public static DataSetInfo newMasterInfo(String ip, String port, Integer weight, boolean alive){
        DataSetInfo res = new DataSetInfo(ip, port);
        res.setWeight(weight);
        res.setAlive(alive);
        return res;
    }

    public static DataSetInfo newSlaveInfo(String ip, String port, Integer weight, boolean watch, Long delay){
        DataSetInfo res = new DataSetInfo(ip, port);
        res.setWeight(weight);
        res.setWatch(watch);
        res.setDelay(delay);
        return res;
    }

    private static boolean parseAlive(String str){
        return str.trim().equals(TdsqlConst.TDSQL_ROUTE_ACTIVE_TRUE);
    }

    private static boolean parseWatch(String str){
        return str.trim().equals(TdsqlConst.TDSQL_ROUTE_WATCH_TRUE);
    }

    private static Integer parseWeight(String str) throws SQLException {
        try {
            return Integer.parseInt(str.trim());
        } catch (NumberFormatException ex) {
            throw new TDSQLRouteParseException("Invalid weight string: " + str);
        }
    }

    private static Long parseDelay(String str) throws SQLException {
        try {
            return Long.parseLong(str.trim());
        } catch (NumberFormatException ex) {
            throw new TDSQLRouteParseException("Invalid delay string: " + str);
        }
    }

    private static String[] parseEndpoint(String str) throws SQLException {
        String[] res =str.trim().split(endpointSeparator);
        if(res.length < 2) {
            throw new TDSQLRouteParseException("Invalid endpoint: " + str);
        }
        try {
            Integer.parseInt(res[1]);
        } catch (NumberFormatException ex) {
            throw new TDSQLRouteParseException("Invalid port value in endpoint: " + str);
        }
        return res;
    }

    public static TdsqlHostInfo convertDataSetInfo(DataSetInfo dataSetInfo, ConnectionUrl connectionUrl) {
        HostInfo mainHost = connectionUrl.getMainHost();
        return new TdsqlHostInfo(
                new HostInfo(mainHost.getOriginalUrl(), dataSetInfo.getIP(), Integer.parseInt(dataSetInfo.getPort()),
                        mainHost.getUser(), mainHost.getPassword(), mainHost.getHostProperties()));
    }

}
