package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionMode.DIRECT;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionMode;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.exception.TdsqlRouteParseException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConst;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TdsqlDataSetUtil {

    private static final String separator = "@";
    private static final String endpointSeparator = ":";
    private static final String listSeparator = ",";
    private static final String emptyStr = "";
    private static final Integer zero = 0;
    private static final Long zeroLong = 0L;

    public static boolean isEmpty(String str) {
        return str == null || str.equals(emptyStr);
    }

    public static TdsqlDataSetInfo parseMaster(String masterString) throws SQLException {
        if(isEmpty(masterString)) {
            return null;
        }
        String[] masterSplit = masterString.split(separator);
        if(masterSplit.length < 3) {
            throw new TdsqlRouteParseException("Invalid master info length: " + masterString);
        }
        String[] masterIPPort = parseEndpoint(masterSplit[0]);
        TdsqlDataSetInfo masterInfo = new TdsqlDataSetInfo(masterIPPort[0], masterIPPort[1]);
        masterInfo.setWeight(parseWeight(masterSplit[1]));
        masterInfo.setAlive(parseAlive(masterSplit[1]));
        return masterInfo;
    }

    public static TdsqlDataSetInfo parseSlave(String slaveString) throws SQLException {
        if(isEmpty(slaveString)) {
            return null;
        }
        String[] slaveSplit = slaveString.split(separator);
        if(slaveSplit.length < 4) {
            throw new TdsqlRouteParseException("Invalid slave info length: " + slaveString);
        }
        String[] slaveIPPort = parseEndpoint(slaveSplit[0]);
        TdsqlDataSetInfo slaveInfo = new TdsqlDataSetInfo(slaveIPPort[0], slaveIPPort[1]);
        slaveInfo.setWeight(parseWeight(slaveSplit[1]));
        slaveInfo.setWatch(parseWatch(slaveSplit[2]));
        slaveInfo.setDelay(parseDelay(slaveSplit[3]));
        return slaveInfo;
    }

    public static List<TdsqlDataSetInfo> parseSlaveList(String slaveListString) throws SQLException {
        if(isEmpty(slaveListString)) {
            return new ArrayList<>();
        }
        List<TdsqlDataSetInfo> tdsqlDataSetInfos = new ArrayList<>();
        String[] slaveListSplit = slaveListString.split(listSeparator);
        for (String s : slaveListSplit) {
            tdsqlDataSetInfos.add(parseSlave(s));
        }
        return tdsqlDataSetInfos;
    }

    public static TdsqlDataSetInfo newMasterInfo(String ip, String port, Integer weight, boolean alive){
        TdsqlDataSetInfo res = new TdsqlDataSetInfo(ip, port);
        res.setWeight(weight);
        res.setAlive(alive);
        return res;
    }

    public static TdsqlDataSetInfo newSlaveInfo(String ip, String port, Integer weight, boolean watch, Long delay){
        TdsqlDataSetInfo res = new TdsqlDataSetInfo(ip, port);
        res.setWeight(weight);
        res.setWatch(watch);
        res.setDelay(delay);
        return res;
    }

    private static boolean parseAlive(String str){
        if(isEmpty(str)) {
            return false;
        }
        return str.trim().equals(TdsqlDirectConst.TDSQL_DIRECT_ROUTE_ACTIVE_TRUE);
    }

    private static boolean parseWatch(String str){
        if(isEmpty(str)) {
            return false;
        }
        return str.trim().equals(TdsqlDirectConst.TDSQL_DIRECT_ROUTE_WATCH_TRUE);
    }

    private static Integer parseWeight(String str) throws SQLException {
        if(isEmpty(str)){
            return zero;
        }
        try {
            return Integer.parseInt(str.trim());
        } catch (NumberFormatException ex) {
            throw new TdsqlRouteParseException("Invalid weight string: " + str);
        }
    }

    private static Long parseDelay(String str) throws SQLException {
        if(isEmpty(str)) {
            return zeroLong;
        }
        try {
            return Long.parseLong(str.trim());
        } catch (NumberFormatException ex) {
            throw new TdsqlRouteParseException("Invalid delay string: " + str);
        }
    }

    private static String[] parseEndpoint(String str) throws SQLException {
        if(isEmpty(str)) {
            throw new TdsqlRouteParseException("empty endpoint");
        }
        String[] res =str.trim().split(endpointSeparator);
        if(res.length < 2) {
            throw new TdsqlRouteParseException("Invalid endpoint: " + str);
        }
        try {
            Integer.parseInt(res[1]);
        } catch (NumberFormatException ex) {
            throw new TdsqlRouteParseException("Invalid port value in endpoint: " + str);
        }
        return res;
    }

    public static TdsqlHostInfo convertDataSetInfo(TdsqlDataSetInfo tdsqlDataSetInfo, ConnectionUrl connectionUrl) {
        HostInfo mainHost = connectionUrl.getMainHost();
        TdsqlHostInfo tdsqlHostInfo = new TdsqlHostInfo(
                new HostInfo(mainHost.getOriginalUrl(), tdsqlDataSetInfo.getIp(), Integer.parseInt(tdsqlDataSetInfo.getPort()),
                        mainHost.getUser(), mainHost.getPassword(), mainHost.getHostProperties()), DIRECT);
        //暂时通过set的方式进行属性赋值，后续优化
        tdsqlHostInfo.setWeightFactor(tdsqlDataSetInfo.getWeight());
        tdsqlHostInfo.setAlive(tdsqlDataSetInfo.getAlive());
        tdsqlHostInfo.setDelay(tdsqlDataSetInfo.getDelay());
        return tdsqlHostInfo;
    }

    /*public static String dataSetList2String(List<TdsqlDataSetInfo> dataSetInfos){
        if (dataSetInfos == null) {
            return "[]";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (TdsqlDataSetInfo dataSetInfo : dataSetInfos) {
            sb.append(String.format("%s:%s", dataSetInfo.getIP(), dataSetInfo.getPort()));
            sb.append(", ");
        }
        sb.append("]");
        return sb.toString();
    }*/

    public static List<TdsqlDataSetInfo> copyDataSetList(List<TdsqlDataSetInfo> tdsqlDataSetInfos) {
        List<TdsqlDataSetInfo> res = new ArrayList<>();
        if(tdsqlDataSetInfos == null || tdsqlDataSetInfos.size() == 0) {
            return res;
        }
        for (TdsqlDataSetInfo tdsqlDataSetInfo : tdsqlDataSetInfos) {
            res.add(tdsqlDataSetInfo.copy());
        }
        return res;
    }

}
