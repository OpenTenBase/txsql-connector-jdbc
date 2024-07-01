package testsuite.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testsuite.util.model.DbConfig;
import testsuite.util.model.DbInfo;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Bussi {
    public static String RWMODE = "rw";
    public static String ROMODE = "ro";

    private static final Logger logger = LoggerFactory.getLogger(Bussi.class);
    private static String table_schema_str = "  (\n" +
            "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n" +
            "  `name1` char(32) DEFAULT NULL,\n" +
            "  `desc1` varchar(200) DEFAULT NULL,\n" +
            "  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
            "  PRIMARY KEY (`id`)\n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;";
    private Connection conn = null;
    private final String connectParams;
    private String table = "t_user";
//    private TimeLogger timelogger;

    public long getInterval() {
        return interval;
    }

    private long interval = 500;
    DbConfig dbConfig;
    private boolean stop = false;

    public String getRunMode() {
        return runMode;
    }

    public void setRunMode(String runMode) {
        this.runMode = runMode;
    }

    private String runMode = RWMODE;

    public int getConnectFailNumber() {
        return connectFailNumber;
    }

    public int getInsertNumber() {
        return insertNumber;
    }

    public int getQueryNumber() {
        return queryNumber;
    }

    private int connectFailNumber = 0;
    private int insertNumber = 0;
    private int queryNumber = 0;
    private long lastInserttime = 0;
    private long lastQuerytime = 0;
    private int queryFailNumber = 0;
    private int insertFailNumber = 0;

    public int getQueryFailNumber() {
        return queryFailNumber;
    }

    public int getInsertFailNumber() {
        return insertFailNumber;
    }

    public int getConnectNumber() {
        return connectNumber;
    }

    private int connectNumber = 0;


    public Bussi(String connectParams) {
//        timelogger = new TimeLogger("Bussi");
        this.connectParams = connectParams;
        dbConfig = DbConfig.getDbconfig();
        if (connectParams.lastIndexOf("tdsqlDirectReadWriteMode=ro") > 0) {
            runMode = ROMODE;
        }
    }

    public Bussi(String connectParams, DbConfig dbConfig) {
//        timelogger = new TimeLogger("Bussi");
        this.connectParams = connectParams;
        this.dbConfig = dbConfig;
//        if (connectParams.lastIndexOf("tdsqlDirectReadWriteMode=ro") > 0) {
//            runMode = ROMODE;
//        }
    }


    private void _getcon() {
//        timelogger.start("connect");
        while (!stop) {
//            timelogger.start("connect");
            try {
                connectNumber++;
                conn = dbConfig.getNewConnect(connectParams);
            } catch (SQLException e) {
                logger.error("_getcon error");
                e.printStackTrace();
                connectFailNumber++;
            }
//            timelogger.end("connect");
            if (conn != null) {
                return;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public Connection getConn() {

        if (conn == null) {
            logger.info("conn is null");
            _getcon();
        }
        try {
            if (conn.isClosed()) {
//                if(!DbConfig.pool){
//                    logger.info("conn is closed");
//                }
                _getcon();
            }
        } catch (SQLException e) {
            logger.error(" getConn catch sqlexception error");
            e.printStackTrace();
        }
        return conn;
    }

    public String getCreateTableSql() {
        return "CREATE TABLE IF NOT EXISTS `" + this.table + "`" + Bussi.table_schema_str;
    }

    public void initDb() throws SQLException {
        Statement stmt = getConn().createStatement();
//        stmt.executeUpdate("DROP TABLE IF EXISTS t_user");
        String sql = this.getCreateTableSql();
        logger.info("create table sql:" + sql);
        stmt.executeUpdate(sql);
    }

    public int getTableCount() throws SQLException {

//        timelogger.start("getTableCount");
        Statement stmt = this.getConn().createStatement();
        String sql = "SELECT COUNT(*) FROM " + table;
        ResultSet rs = stmt.executeQuery(sql);
//        timelogger.end("getTableCount");
        int count = -1;
        if (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        stmt.close();
        if(dbConfig.isShortConn|| dbConfig.isPool){
            conn.close();
        }

        return count;
    }

    public int getConNumberOnEcheNodes(String ip, int port, String[] proxyList) throws IOException {
        String localip = DbConfig.getDbconfig().getLocalIp();
        String result = SshClient.getDefaultHostSshClient().sendCmd(String.format("mysql -h%s  -P%d -uqt4s -p'g<m:7KNDF.L1<^1C' -e \"show processlist;\" |grep %s|grep %s|grep %s|grep -v \"%s\""
                , ip, port, dbConfig.getUser(), dbConfig.getDb_name(), localip, String.join("\\|", proxyList)));
        return (int) Arrays.stream(result.split("\n")).filter(s -> s.trim().length() > 0).count();
    }

    public Map<String, Integer> getConnectionNumber(InstanceInfo dbinfo) throws IOException {
        HashMap<String, Integer> connectMap = new HashMap<>();
        String ip = dbinfo.getMasterDbinfo().getIp();
        int port = dbinfo.getMasterDbinfo().getPort();
        String[] proxy = dbinfo.getProxyIpList();
        connectMap.put(ip, getConNumberOnEcheNodes(ip, port, proxy));

        for (DbInfo slave : dbinfo.getSlaveMapList()) {
            ip = slave.getIp();
            port = slave.getPort();
            connectMap.put(ip, getConNumberOnEcheNodes(ip, port, proxy));
        }
        logger.info("getConnectionNumber:" + connectMap);
        return connectMap;


    }

    public void insertData(int count) throws SQLException {

//        timelogger.start("insertData");
        String sql = "insert into " + table + "(name1,desc1)  values('a','qta-josephpu.test-b')";
        while (!stop && count != 0) {
            try {

                for (; count != 0 && !stop; count--) {
                    Statement stmt = getConn().createStatement();
                    ResultSet rs = stmt.executeQuery("select count(*) from " + table);
//                    while (rs.next())
//                        logger.info(String.valueOf(rs.getInt(1)));
                    queryNumber++;
                    stmt.executeUpdate(sql);
                    insertNumber++;
                    lastInserttime = System.currentTimeMillis();
                    stmt.close();
                    if(dbConfig.isShortConn|| dbConfig.isPool){
                        conn.close();
                    }
                }
                logger.info("end insertData");
//                timelogger.end("insertData");
            } catch (Exception e) {
                insertFailNumber++;
                logger.error("insertData sqlexception error： connection closed? "+conn.isClosed());
                conn.close();
                e.printStackTrace();
            }
        }
    }

    public void setStop() {
        logger.info("setStop");
        this.stop = true;
    }

    public void query(int count) {

//        timelogger.start("query");
        String sql = "select * from  " + table + " where id=?";
        while (!stop && count != 0) {
            PreparedStatement psmt;
            try {

                Random r = new Random();
                for (; count != 0 && !stop; count--) {
//                    psmt = getConn().prepareStatement(sql);
//                    psmt.setInt(1, r.nextInt(10000));
//                    psmt.executeQuery();
//                    psmt.close();
//                    getConn().createStatement().executeQuery("select sleep(600)");
                    Statement stmt = getConn().createStatement();
                    stmt.executeQuery("select * from  " + table + " where id="+r.nextInt(100000));
                    stmt.close();

                    queryNumber++;
                    //logger.info("queryNumber:" + queryNumber);
                    lastQuerytime = System.currentTimeMillis();
                    if(dbConfig.isShortConn|| dbConfig.isPool){
                        conn.close();
                    }


                }
//                logger.info("query bussi end: count=" + count);
//                timelogger.end("query");
                return;
            } catch (Exception e) {
                queryFailNumber++;
                try {
                    conn.close();
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
                logger.error("query sqlexception error sql: "+sql);
                e.printStackTrace();


            }

        }

    }

    public boolean checkInsertRunning() {
        long currentTime = System.currentTimeMillis();
        logger.info("checkInsertRunning " + insertNumber + "  laster update:" + lastInserttime);
        return (currentTime - lastInserttime) < 2 * interval;
    }

    public boolean checkQueryRunning() {
        long currentTime = System.currentTimeMillis();
        logger.info("checkQueryRunning " + queryNumber + "  laster update:" + lastQuerytime);
        return (currentTime - lastQuerytime) < 2 * interval;
    }

    public boolean checkRunning() {
        return checkQueryRunning() || checkInsertRunning();
    }

    public void deleteData(int count) throws SQLException {

        String sql = "DELETE FROM "+table+" WHERE `id` < " + count;
//        timelogger.start("deleteData");
        Statement stmt = this.getConn().createStatement();
        int rs = stmt.executeUpdate(sql);
//        timelogger.end("deleteData");
        stmt.close();
        if(dbConfig.isShortConn|| dbConfig.isPool){
            conn.close();
        }

    }

    public static boolean runBussi(Bussi bussi, int count) {
        try {
            bussi.getConn();
//            bussi.initDb();
            bussi.insertData(count);
            bussi.deleteData(1);
            bussi.getTableCount();
            return true;

        } catch (SQLException e) {
            //e.printStackTrace();
            bussi.close();
        }
        return false;
    }

    public static void runBussi(Bussi bussi) {
        runBussi(bussi, -1);
    }

    public static void runROBussi(Bussi bussi) {
        runROBussi(bussi, 1);
    }

    public static void runROBussi(Bussi bussi, int count) {
        bussi.query(count);
//        bussi.close();
//        bussi.getConn();
//        try {
//
//            bussi.getTableCount();
//            bussi.close();
//        } catch (SQLException e) {
//            logger.error("catch sqlexception error");
//            e.printStackTrace();
//
//        } finally {
//            bussi.close();
//        }

    }

//    public static void runROBussi() {
//        String params = "&useUnicode=true" +
//                "&tdsqlDirectReadWriteMode=ro" +
//                "&characterEncoding=utf-8" +
//                "&serverTimezone=Asia/Shanghai" +
//                "&tdsqlDirectTopoRefreshConnTimeoutMillis=500" +
//                "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=500" +
//                "&tdsqlDirectCloseConnTimeoutMillis=500" +
//                "&tdsqlLoadBalanceStrategy=lc" +
//                "&logger=Slf4JLogger";
//
//        runROBussi(new Bussi(params, DbConfig.getDbconfig()));
//    }

//    public void runRWBussi() {
//        String params = "&useUnicode=true"
//                + "&characterEncoding=utf-8"
//                + "&serverTimezone=Asia/Shanghai"
//                + "&tdsqlDirectTopoRefreshConnTimeoutMillis=500"
//                + "&tdsqlDirectTopoRefreshStmtTimeoutSeconds=500"
//                + "&tdsqlDirectCloseConnTimeoutMillis=500"
//                + "&tdsqlLoadBalanceStrategy=lc"
//                + "&logger=Slf4JLogger";
//
//        runBussi(new Bussi(params, DbConfig.getDbconfig()));
//    }

    public void close()  {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}