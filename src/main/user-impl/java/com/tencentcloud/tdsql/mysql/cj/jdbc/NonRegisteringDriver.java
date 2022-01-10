/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.tencentcloud.tdsql.mysql.cj.jdbc;

import static com.tencentcloud.tdsql.mysql.cj.util.StringUtils.isNullOrEmpty;

import com.tencentcloud.tdsql.mysql.cj.Constants;
import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.MysqlConnection;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl.Type;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import com.tencentcloud.tdsql.mysql.cj.conf.PropertyKey;
import com.tencentcloud.tdsql.mysql.cj.exceptions.CJException;
import com.tencentcloud.tdsql.mysql.cj.exceptions.ExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.exceptions.UnableToConnectException;
import com.tencentcloud.tdsql.mysql.cj.exceptions.UnsupportedConnectionStringException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.FailoverConnectionProxy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import com.tencentcloud.tdsql.mysql.cj.util.StringUtils;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * The Java SQL framework allows for multiple database drivers. Each driver should supply a class that implements the Driver interface
 * 
 * <p>
 * The DriverManager will try to load as many drivers as it can find and then for any given connection request, it will ask each driver in turn to try to
 * connect to the target URL.
 * </p>
 * 
 * <p>
 * It is strongly recommended that each Driver class should be small and standalone so that the Driver class can be loaded and queried without bringing in vast
 * quantities of supporting code.
 * </p>
 * 
 * <p>
 * When a Driver class is loaded, it should create an instance of itself and register it with the DriverManager. This means that a user can load and register a
 * driver by doing Class.forName("foo.bah.Driver")
 * </p>
 */
public class NonRegisteringDriver implements java.sql.Driver {
    private static final String ALLOWED_QUOTES = "\"'";
    private static final String URL_PREFIX = "jdbc:tdsql-mysql://";
    private List<String> haLoadBalanceWeightFactor = null;

    /*
     * Standardizes OS name information to align with other drivers/clients
     * for MySQL connection attributes
     * 
     * @return the transformed, standardized OS name
     */
    public static String getOSName() {
        return Constants.OS_NAME;
    }

    /*
     * Standardizes platform information to align with other drivers/clients
     * for MySQL connection attributes
     * 
     * @return the transformed, standardized platform details
     */
    public static String getPlatform() {
        return Constants.OS_ARCH;
    }

    static {
        try {
            Class.forName(AbandonedConnectionCleanupThread.class.getName());
        } catch (ClassNotFoundException e) {
            // ignore
        }
    }

    /**
     * Gets the drivers major version number
     * 
     * @return the drivers major version number
     */
    static int getMajorVersionInternal() {
        return StringUtils.safeIntParse(Constants.CJ_MAJOR_VERSION);
    }

    /**
     * Get the drivers minor version number
     * 
     * @return the drivers minor version number
     */
    static int getMinorVersionInternal() {
        return StringUtils.safeIntParse(Constants.CJ_MINOR_VERSION);
    }

    /**
     * Construct a new driver and register it with DriverManager
     * 
     * @throws SQLException
     *             if a database error occurs.
     */
    public NonRegisteringDriver() throws SQLException {
        // Required for Class.forName().newInstance()
    }

    /**
     * Typically, drivers will return true if they understand the subprotocol
     * specified in the URL and false if they don't. This driver's protocols
     * start with jdbc:mysql:
     * 
     * @param url
     *            the URL of the driver
     * 
     * @return true if this driver accepts the given URL
     * 
     * @exception SQLException
     *                if a database access error occurs or the url is null
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return (ConnectionUrl.acceptsUrl(url));
    }

    //
    // return the database name property
    //

    /**
     * Try to make a database connection to the given URL. The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given
     * URL. This will be common, as when the JDBC driverManager is asked to connect to a given URL, it passes the URL to each loaded driver in turn.
     * 
     * <p>
     * The driver should raise an SQLException if the URL is null or if it is the right driver to connect to the given URL, but has trouble connecting to the
     * database.
     * </p>
     * 
     * <p>
     * The java.util.Properties argument can be used to pass arbitrary string tag/value pairs as connection arguments. These properties take precedence over any
     * properties sent in the URL.
     * </p>
     * 
     * <p>
     * MySQL protocol takes the form: jdbc:mysql://host:port/database
     * </p>
     * 
     * @param url
     *            the URL of the database to connect to
     * @param info
     *            a list of arbitrary tag/value pairs as connection arguments
     * 
     * @return a connection to the URL or null if it isn't us
     * 
     * @exception SQLException
     *                if a database access error occurs or the url is {@code null}
     */
    @Override
    public java.sql.Connection connect(String url, Properties info) throws SQLException {

        try {
            if (!ConnectionUrl.acceptsUrl(url)) {
                /*
                 * According to JDBC spec:
                 * The driver should return "null" if it realizes it is the wrong kind of driver to connect to the given URL. This will be common, as when the
                 * JDBC driver manager is asked to connect to a given URL it passes the URL to each loaded driver in turn.
                 */
                return null;
            }

            ConnectionUrl conStr = ConnectionUrl.getConnectionUrlInstance(url, info);
            switch (conStr.getType()) {
                case SINGLE_CONNECTION:
                    return ConnectionImpl.getInstance(conStr.getMainHost());

                case FAILOVER_CONNECTION:
                case FAILOVER_DNS_SRV_CONNECTION:
                    return FailoverConnectionProxy.createProxyInstance(conStr);

                case LOADBALANCE_CONNECTION:
                case LOADBALANCE_DNS_SRV_CONNECTION:
                    if (url.contains("haLoadBalanceStrategy=sed") || url.contains("ha.loadBalanceStrategy=sed")) {
                        return connectionUnion(conStr, url, info);
                    }
                    return LoadBalancedConnectionProxy.createProxyInstance(conStr);

                case REPLICATION_CONNECTION:
                case REPLICATION_DNS_SRV_CONNECTION:
                    return ReplicationConnectionProxy.createProxyInstance(conStr);

                default:
                    return null;
            }

        } catch (UnsupportedConnectionStringException e) {
            // when Connector/J can't handle this connection string the Driver must return null
            return null;

        } catch (CJException ex) {
            throw ExceptionFactory.createException(UnableToConnectException.class,
                    Messages.getString("NonRegisteringDriver.17", new Object[] { ex.toString() }), ex);
        }
    }

    public java.sql.Connection connectionUnion(ConnectionUrl conStr, String initurl, Properties info) throws SQLException {
        List<String> hostList = conStr.getHostsList().stream().map(HostInfo::getHostPortPair)
                .collect(Collectors.toCollection(LinkedList::new));
        Map<String, HostInfo> hostInfoMap = conStr.getHostsList().stream()
                .collect(Collectors.toMap(HostInfo::getHostPortPair, hostInfo -> hostInfo, (a, b) -> b));
        Properties parsedProps = conStr.getConnectionArgumentsAsProperties();
        if (parsedProps == null) {
            return null;
        }
        this.checkParams(parsedProps);
        ConnectionManager.getInstance().addAllHost(hostList);
        if (parsedProps.containsKey(PropertyKey.haLoadBalanceWeightFactor.getKeyName())) {
            ConnectionManager.getInstance().addAllWeightFactor(hostList, this.haLoadBalanceWeightFactor);
        } else {
            ConnectionManager.getInstance().addAllWeightFactor(hostList, null);
        }

        String host;
        if (hostList.size() > 0) {
            int i = 0;
            int j = 0;
            while (i < hostList.size()) {
                ++j;
                host = this.dealHostConnectionCounts(hostList);
                if (host == null) {
                    continue;
                }
                if (ConnectionManager.getInstance().blackList.contains(host)) {
                    ConnectionManager.HOST_CONNECTION_COUNT_MAP.remove(host);
                    System.out.println("......");
                    hostList.remove(host);
                    --i;
                } else {
                    ConnectionManager.getInstance().getPropMap().put(host, info);
                    java.sql.Connection conn = this.chargeConnection(hostInfoMap, host, true);
                    if (conn != null) {
                        ((MysqlConnection) conn).getSession().getLog().logInfo(host + " - " + ConnectionManager.HOST_CONNECTION_COUNT_MAP.get(host));
                        return conn;
                    }
                    hostList.remove(host);
                    ConnectionManager.HOST_CONNECTION_COUNT_MAP.remove(host);
                    System.out.println("......");
                    --i;
                }
                ++i;
            }
            throw SQLError.createSQLException(Messages.getString("NonRegisteringDriver.17") + " Can't connect to"
                    + " all loadbalcance hosts.Blacklist:" + ConnectionManager.getInstance().blackList + " Trial round times[" + j + "]. Host list" +
                    ConnectionManager.getInstance().getAllHost() + "." + Messages.getString("NonRegisteringDriver.18"), MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, null);
        }
        return null;
    }

    private void checkParams(Properties parsedProps) throws SQLException {
        // haLoadBalanceWeightFactor
        List<String> haLoadBalanceWeightFactor = StringUtils.split(parsedProps.getProperty("haLoadBalanceWeightFactor",
                ""), ",", ALLOWED_QUOTES, ALLOWED_QUOTES, false);
        if (!haLoadBalanceWeightFactor.isEmpty()) {
            for (String wf : haLoadBalanceWeightFactor) {
                try {
                    Integer.parseInt(wf);
                } catch (NumberFormatException e) {
                    throw SQLError.createSQLException(
                            "Invaild haLoadBalanceWeightFactor value",
                            MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
                }
            }
            this.haLoadBalanceWeightFactor = haLoadBalanceWeightFactor;
        }

        // haLoadBalanceMaximumErrorRetries
        String haLoadBalanceMaximumErrorRetriesStr = parsedProps.getProperty("haLoadBalanceMaximumErrorRetries",
                "1");
        try {
            int haLoadBalanceMaximumErrorRetries = Integer.parseInt(haLoadBalanceMaximumErrorRetriesStr);
            ConnectionManager.getInstance().setHaLoadBalanceMaximumErrorRetries(haLoadBalanceMaximumErrorRetries);
        } catch (NumberFormatException e) {
            throw SQLError.createSQLException("Invaild haLoadBalanceMaximumErrorRetries value",
                    MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
        }

        // haLoadBalanceHeartbeatIntervalTime
        String haLoadBalanceHeartbeatIntervalTimeStr = parsedProps.getProperty("haLoadBalanceHeartbeatIntervalTime",
                "3000");
        try {
            int haLoadBalanceHeartbeatIntervalTime = Integer.parseInt(haLoadBalanceHeartbeatIntervalTimeStr);
            ConnectionManager.getInstance().setHaLoadBalanceHeartbeatIntervalTime(haLoadBalanceHeartbeatIntervalTime);
        } catch (NumberFormatException e) {
            throw SQLError.createSQLException("Invaild haLoadBalanceHeartbeatIntervalTime value",
                    MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
        }

        // haLoadBalanceBlacklistTimeout
        String haLoadBalanceBlacklistTimeoutStr = parsedProps.getProperty("haLoadBalanceBlacklistTimeout",
                "5000");
        try {
            int haLoadBalanceBlacklistTimeout = Integer.parseInt(haLoadBalanceBlacklistTimeoutStr);
            ConnectionManager.getInstance().setHaLoadBalanceBlacklistTimeout(haLoadBalanceBlacklistTimeout);
        } catch (NumberFormatException e) {
            throw SQLError.createSQLException("Invaild haLoadBalanceBlacklistTimeout value",
                    MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
        }

        // haLoadBalanceHeartbeatMonitor
        String haLoadBalanceHeartbeatMonitorStr = parsedProps.getProperty("haLoadBalanceHeartbeatMonitor",
                "false");
        try {
            boolean haLoadBalanceHeartbeatMonitor = Boolean.parseBoolean(haLoadBalanceHeartbeatMonitorStr);
            ConnectionManager.getInstance().setHaLoadBalanceHeartbeatMonitor(haLoadBalanceHeartbeatMonitor);
        } catch (Exception e) {
            throw SQLError.createSQLException("Invaild haLoadBalanceHeartbeatMonitor value",
                    MysqlErrorNumbers.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, null);
        }
    }

    private String dealHostConnectionCounts(List<String> hosts) {
        ConcurrentHashMap<String, Integer> concurrentHashMap = ConnectionManager.HOST_CONNECTION_COUNT_MAP;
        synchronized (concurrentHashMap) {
            String host = this.getRandomHost(hosts);
            int count = 1;
            if (ConnectionManager.HOST_CONNECTION_COUNT_MAP.containsKey(host)) {
                host = this.choice();
                if (host == null) {
                    return null;
                }
                count += ConnectionManager.HOST_CONNECTION_COUNT_MAP.get(host);
            }
            ConnectionManager.HOST_CONNECTION_COUNT_MAP.put(host, count);
            return host;
        }
    }

    private String choice() {
        List<Map.Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(ConnectionManager.HOST_CONNECTION_COUNT_MAP.entrySet());
        Map<String, Integer> wfMap = ConnectionManager.getInstance().getWeightFactor();

        for (int i = 0; i < list.size(); i++) {
            if (wfMap.get(list.get(i).getKey()) > 0) {
                int ci = list.get(i).getValue() + 1;
                for (int j = i + 1; j < list.size(); j++) {
                    if (wfMap.get(list.get(j).getKey()) > 0) {
                        int cj = list.get(j).getValue() + 1;
                        if (ci * wfMap.get(list.get(j).getKey()) >= cj * wfMap.get(list.get(i).getKey())) {
                            i = j;
                        }
                    }
                }
                return list.get(i).getKey();
            }
        }
        return null;
    }

    private String getRandomHost(List<String> hosts) {
        int random = (int) Math.floor(Math.random() * (double) hosts.size());
        return hosts.get(random);
    }

    private java.sql.Connection chargeConnection(Map<String, HostInfo> hostInfoMap, String hostPortPair, Boolean flag) {
        java.sql.Connection conn = null;
        try {
            HostInfo hostInfo = hostInfoMap.get(hostPortPair);
            conn = ConnectionImpl.getInstance(hostInfo);
            if (conn != null && !conn.isClosed()) {
                if (flag) {
                    ConnectionManager.getInstance().addConnection(hostInfo, hostPortPair, conn);
                }
                return conn;
            }
            return conn;
        } catch (Exception e) {
            return conn;
        }
        /*java.sql.Connection conn = null;
        try {
            props = this.parseURL(resultUrl, props);
            if (props == null) {
                return conn;
            }
            conn = ConnectionImpl.getInstance(this.host(props), this.port(props), props, this.database(props), resultUrl);
            if (conn != null && !conn.isClosed()) {
                if (flag) {
                    ConnectionManager.getInstance().addConnection(host, conn);
                }
                return conn;
            }
            return conn;
        } catch (SQLException sqlEx) {
            return conn;
        } catch (Exception ex) {
            return conn;
        }*/
    }

    @Override
    public int getMajorVersion() {
        return getMajorVersionInternal();
    }

    @Override
    public int getMinorVersion() {
        return getMinorVersionInternal();
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        String host = "";
        String port = "";
        String database = "";
        String user = "";
        String password = "";

        if (!isNullOrEmpty(url)) {
            ConnectionUrl connStr = ConnectionUrl.getConnectionUrlInstance(url, info);
            if (connStr.getType() == Type.SINGLE_CONNECTION) {
                HostInfo hostInfo = connStr.getMainHost();
                info = hostInfo.exposeAsProperties();
            }
        }

        if (info != null) {
            host = info.getProperty(PropertyKey.HOST.getKeyName());
            port = info.getProperty(PropertyKey.PORT.getKeyName());
            database = info.getProperty(PropertyKey.DBNAME.getKeyName());
            user = info.getProperty(PropertyKey.USER.getKeyName());
            password = info.getProperty(PropertyKey.PASSWORD.getKeyName());
        }

        DriverPropertyInfo hostProp = new DriverPropertyInfo(PropertyKey.HOST.getKeyName(), host);
        hostProp.required = true;
        hostProp.description = Messages.getString("NonRegisteringDriver.3");

        DriverPropertyInfo portProp = new DriverPropertyInfo(PropertyKey.PORT.getKeyName(), port);
        portProp.required = false;
        portProp.description = Messages.getString("NonRegisteringDriver.7");

        DriverPropertyInfo dbProp = new DriverPropertyInfo(PropertyKey.DBNAME.getKeyName(), database);
        dbProp.required = false;
        dbProp.description = Messages.getString("NonRegisteringDriver.10");

        DriverPropertyInfo userProp = new DriverPropertyInfo(PropertyKey.USER.getKeyName(), user);
        userProp.required = true;
        userProp.description = Messages.getString("NonRegisteringDriver.13");

        DriverPropertyInfo passwordProp = new DriverPropertyInfo(PropertyKey.PASSWORD.getKeyName(), password);
        passwordProp.required = true;
        passwordProp.description = Messages.getString("NonRegisteringDriver.16");

        JdbcPropertySet propSet = new JdbcPropertySetImpl();
        propSet.initializeProperties(info);
        List<DriverPropertyInfo> driverPropInfo = propSet.exposeAsDriverPropertyInfo();

        DriverPropertyInfo[] dpi = new DriverPropertyInfo[5 + driverPropInfo.size()];
        dpi[0] = hostProp;
        dpi[1] = portProp;
        dpi[2] = dbProp;
        dpi[3] = userProp;
        dpi[4] = passwordProp;
        System.arraycopy(driverPropInfo.toArray(new DriverPropertyInfo[0]), 0, dpi, 5, driverPropInfo.size());

        return dpi;
    }

    @Override
    public boolean jdbcCompliant() {
        // NOTE: MySQL is not SQL92 compliant
        // TODO Is it true? DatabaseMetaData.supportsANSI92EntryLevelSQL() returns true...
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }
}
