package com.tencent.tdsql.mysql.cj.jdbc.ha;

import com.tencent.tdsql.mysql.cj.Messages;
import com.tencent.tdsql.mysql.cj.conf.PropertyKey;
import com.tencent.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencent.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencent.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencent.tdsql.mysql.cj.log.Log;
import com.tencent.tdsql.mysql.cj.util.StringUtils;
import java.lang.reflect.InvocationHandler;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author dorianzhang@tencent.com
 */
public abstract class AdvancedLoadBalanceStrategy implements BalanceStrategy {

    private static final String SPLIT_EN_DOT = ",";

    private final Properties props;

    public AdvancedLoadBalanceStrategy(Properties props) {
        this.props = props;
    }

    /**
     * Called by the driver to pick a new connection to route requests over.
     * See LoadBalancedConnectionProxy.createConnectionForHost(String)
     *
     * @param proxy the InvocationHandler that deals with actual method calls to
     *         the JDBC connection, and serves as a factory for new
     *         connections for this strategy via the
     *         createConnectionForHost() method.
     *
     *         This proxy takes care of maintaining the response time list, map of
     *         host/ports to live connections, and taking connections out of the live
     *         connections map if they receive a network-related error while they are in
     *         use by the application.
     * @param configuredHosts the list of hosts/ports (in "host:port" form) as passed in by
     *         the user.
     * @param liveConnections a map of host/ports to "live" connections to them.
     * @param responseTimes the list of response times for a <strong>transaction</strong>
     *         for each host in the configured hosts list.
     * @param numRetries the number of times the driver expects this strategy to re-try
     *         connection attempts if creating a new connection fails.
     * @return the physical JDBC connection for the application to use, based
     *         upon the strategy employed.
     * @throws SQLException if a new connection can not be found or created by this
     *         strategy.
     */
    @Override
    public JdbcConnection pickConnection(InvocationHandler proxy, List<String> configuredHosts,
            Map<String, JdbcConnection> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
        BigDecimal[] weightFactorArray = this.getWeightFactorArray(this.props, configuredHosts.size());

        Map<String, BigDecimal> allowHostMap = this.getAllowHostMap(proxy, configuredHosts, weightFactorArray);

        GlobalConnectionScheduler scheduler = GlobalConnectionScheduler.getInstance(allowHostMap.keySet().stream()
                .collect(Collectors.toMap(hostPort -> hostPort, hostPort -> 0L, (a, b) -> b, ConcurrentHashMap::new)));

        Log logger;
        SQLException ex = null;
        for (int attempts = 0; attempts < numRetries; ) {
            scheduler.getLock().lock();
            String selectedHost;
            ConnectionImpl conn;
            try {
                selectedHost = this.execute(allowHostMap, scheduler);
                conn = (ConnectionImpl) liveConnections.get(selectedHost);
                if (conn == null) {
                    try {
                        conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(selectedHost);
                        scheduler.getCounter().incrementAndGet(selectedHost);
                        logger = ((JdbcConnection) conn).getSession().getLog();
                        if (logger != null && logger.isInfoEnabled()) {
                            logger.logInfo("Hosts Counter: " + scheduler.getCounter());
                        }
                    } catch (SQLException sqlEx) {
                        ex = sqlEx;
                        if (((LoadBalancedConnectionProxy) proxy).shouldExceptionTriggerConnectionSwitch(sqlEx)) {
                            if (!StringUtils.isNullOrEmpty(selectedHost)) {
                                allowHostMap.remove(selectedHost);
                                scheduler.getCounter().remove(selectedHost);
                            }
                            ((LoadBalancedConnectionProxy) proxy).addToGlobalBlocklist(selectedHost);
                            if (allowHostMap.isEmpty()) {
                                attempts++;
                                try {
                                    Thread.sleep(250);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                allowHostMap = this.getAllowHostMap(proxy, configuredHosts, weightFactorArray);
                            }
                            continue;
                        }
                        throw sqlEx;
                    }
                }
            } finally {
                scheduler.getLock().unlock();
            }
            return conn;
        }

        if (ex != null) {
            throw ex;
        }

        // we won't get here, compiler can't tell
        return null;
    }

    /**
     * Specific implementation of different algorithms
     *
     * @param allowHostMap a map of host/ports to "total of live connection counts" to them
     * @param gcs <code>{@link GlobalConnectionScheduler}</code>
     * @return The host:port pair identifying the host to connect to.
     */
    protected abstract String execute(Map<String, BigDecimal> allowHostMap, GlobalConnectionScheduler gcs);

    private BigDecimal[] getWeightFactorArray(Properties props, int numHosts) throws SQLException {
        BigDecimal[] weightFactorArray = new BigDecimal[numHosts];

        String weightFactor = props.getProperty(PropertyKey.loadBalanceWeightFactor.getKeyName(), "");
        if (StringUtils.isNullOrEmpty(weightFactor)) {
            for (int i = 0; i < numHosts; i++) {
                weightFactorArray[i] = BigDecimal.ONE;
            }
            return weightFactorArray;
        }

        String[] wfTmpArray = weightFactor.split(SPLIT_EN_DOT);
        int wfTmpArrayLength = wfTmpArray.length;
        if (wfTmpArrayLength < numHosts) {
            this.fillWeightFactorArray(wfTmpArrayLength, weightFactorArray, wfTmpArray);
            Arrays.fill(weightFactorArray, wfTmpArrayLength, numHosts, BigDecimal.ONE);
        } else {
            this.fillWeightFactorArray(numHosts, weightFactorArray, wfTmpArray);
        }
        return weightFactorArray;
    }

    private void fillWeightFactorArray(int numHosts, BigDecimal[] weightFactorArray, String[] wfTmpArray)
            throws SQLException {
        for (int i = 0; i < numHosts; i++) {
            try {
                BigDecimal wfTmp = new BigDecimal(wfTmpArray[i], new MathContext(2, RoundingMode.HALF_UP));
                if (wfTmp.compareTo(BigDecimal.ZERO) < 0) {
                    throw SQLError.createSQLException(
                            Messages.getString("ShortestExpectDelayStrategy.0", new Object[]{wfTmpArray[i]}), null);
                }
                weightFactorArray[i] = wfTmp;
            } catch (NumberFormatException e) {
                throw SQLError.createSQLException(
                        Messages.getString("ShortestExpectDelayStrategy.1", new Object[]{wfTmpArray[i]}), null);
            }
        }
    }

    private Map<String, BigDecimal> getAllowHostMap(InvocationHandler proxy, List<String> configuredHosts,
            BigDecimal[] weightFactorArray) throws SQLException {
        Map<String, BigDecimal> allowHostMap = IntStream.range(0, configuredHosts.size())
                .filter(i -> weightFactorArray[i].compareTo(BigDecimal.ZERO) > 0).boxed().collect(Collectors
                        .toMap(configuredHosts::get, i -> weightFactorArray[i], (a, b) -> b,
                                () -> new LinkedHashMap<>(configuredHosts.size())));

        Map<String, Long> blockList = ((LoadBalancedConnectionProxy) proxy).getGlobalBlocklist();
        blockList.keySet().forEach(allowHostMap::remove);

        if (allowHostMap.isEmpty()) {
            throw SQLError.createSQLException(Messages.getString("ShortestExpectDelayStrategy.2"), null);
        }
        return allowHostMap;
    }
}
