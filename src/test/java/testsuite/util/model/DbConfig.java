package testsuite.util.model;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testsuite.util.InstanceInfo;
import testsuite.util.SshClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;

public class DbConfig {
    public static final String driveName = "com.tencentcloud.tdsql.mysql.cj.jdbc.Driver";
    public static final Logger logger = LoggerFactory.getLogger(DbConfig.class);
    public boolean isShortConn=false;

    public String getProxy_ip() {
        return proxy_ip;
    }

    private static ArrayList<Connection> connList = new ArrayList<Connection>();
    private String proxy_ip;
    Map<String, DruidDataSource> dataSourceMap = new HashMap<String, DruidDataSource>();


    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    private String db_name;

    public void setUser(String user) {
        this.user = user;
    }

    public void setPwd(String pwd) {
        this.pwd = pwd;
    }

    private String user;

    public String getUser() {
        return user;
    }

    public String getPwd() {
        return pwd;
    }

    private String pwd;
    public static boolean pool = getProperties().getProperty("isPool").equalsIgnoreCase("true");
    public static String jdbcPrefix = "jdbc:mysql:direct://"; //jdbc:tdsql-mysql:direct://

    public void setPool(boolean pool) {
        isPool = pool;
    }

    public boolean isPool = pool;

    public Set<String> offlineIp = new HashSet<>();

    public void addOfflineIp(String ip) {
        offlineIp.add(ip);
    }

    public void removeOfflineIp(String ip) {
        offlineIp.remove(ip);
    }

    public boolean ifIpOffline(String ip) {
        return offlineIp.contains(ip);
    }

    static {
        if (!pool) {
            try {
                Class.forName(driveName);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public DbConfig(String proxy_ip, String dbname, String user, String pwd) {
        this.proxy_ip = proxy_ip;
        this.db_name = dbname;
        this.user = user;
        this.pwd = pwd;

    }

    public String getUrl() {
        return jdbcPrefix + this.proxy_ip + "/" + this.db_name;
    }

    private synchronized void initDataSource(String params) throws SQLException {

        if (!dataSourceMap.containsKey(params)) {
            DruidDataSource dataSource;
            String db_url = this.getUrl() + "?" + params;
            logger.info(String.format(driveName + " pool?" + isPool + " user:%s pwd:%s  db_url:%s", user, pwd, db_url));
            dataSource = new DruidDataSource();
            dataSource.setUrl(db_url);
            dataSource.setUsername(user);
            dataSource.setPassword(pwd);
            dataSource.setDriverClassName(driveName);
            dataSource.setInitialSize(20);
            dataSource.setMaxActive(20);
            dataSource.setMinIdle(20);
            dataSource.setValidationQuery("select 1");
            dataSource.setTimeBetweenEvictionRunsMillis(10000);
            dataSource.setTestWhileIdle(true);
            dataSource.init();
            dataSourceMap.put(params, dataSource);
        }
    }

    public Connection getNewConnect(String params) throws SQLException {
        if (!isPool) {
            String db_url = this.getUrl() + "?" + params;
            logger.info(String.format(driveName + " pool?" + isPool + " user:%s pwd:%s  db_url:%s", user, pwd, db_url));
            Connection conn = DriverManager.getConnection(db_url, this.user, this.pwd);
//            connList.add(conn);
            return conn;
        }
        if (!dataSourceMap.containsKey(params)) {
            initDataSource(params);
        }
        return dataSourceMap.get(params).getConnection();

    }

    public String getJdbcUrl(String params) {
        return this.getUrl() + "?" + params;
    }


    public static void closeAllConnections() {
        connList.forEach(c -> {
            try {
                c.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });

    }

    public static DbConfig getDbconfig(InstanceInfo instanceinfo) {
        return getDbconfig("", "", instanceinfo);
    }

    public static DbConfig getDbconfig() {
        return getDbconfig("", "", null);
    }

    public static DbConfig getDbconfig(String user, String db_name, InstanceInfo instanceinfo) {
        Properties p = getProperties();
        String proxyIp;
        if (instanceinfo != null) {
            proxyIp = instanceinfo.getPorxyIpPort();
        } else {
            proxyIp = p.getProperty("proxy_ip");
        }
        if (StringUtils.isEmpty(user))
            user = p.getProperty("user");
        String pwd = p.getProperty("pwd");
        if (StringUtils.isEmpty(db_name))
            db_name = p.getProperty("dbname");
        return new DbConfig(proxyIp, db_name, user, pwd);
    }


    private static Properties p;

    public static Properties getProperties() {
        if (p != null) {
            return p;
        }
        try {
            p = new Properties();
            String rootPath = DbConfig.class.getResource("/").getPath();
            File file = new File(rootPath + "db.properties");
            FileInputStream fis = new FileInputStream(file);
            p.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return p;
    }

    private static String localIp = "";

    public static synchronized String getLocalIp() throws IOException {
        if (!localIp.equals("")) {
            return localIp;
        }
        localIp = getProperties().getProperty("localip");
        if (localIp.equals("")) {
            String cmd = "echo $SSH_CLIENT | awk '{print $1}'";

            localIp = SshClient.getDefaultHostSshClient().sendCmd(cmd).trim();

        }

        return localIp;

    }
}