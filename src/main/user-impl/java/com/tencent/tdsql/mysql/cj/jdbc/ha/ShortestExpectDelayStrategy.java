package com.tencent.tdsql.mysql.cj.jdbc.ha;

import com.tencent.tdsql.mysql.cj.Messages;
import com.tencent.tdsql.mysql.cj.conf.PropertyKey;
import com.tencent.tdsql.mysql.cj.jdbc.ConnectionImpl;
import com.tencent.tdsql.mysql.cj.jdbc.JdbcConnection;
import com.tencent.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencent.tdsql.mysql.cj.util.StringUtils;
import java.lang.reflect.InvocationHandler;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author dorianzhang@tencent.com
 */
public class ShortestExpectDelayStrategy implements BalanceStrategy {

    private static final String SPLIT_EN_DOT = ",";

    private final Properties props;

    public ShortestExpectDelayStrategy(Properties props) {
        this.props = props;
    }

    @Override
    public JdbcConnection pickConnection(InvocationHandler proxy, List<String> configuredHosts,
            Map<String, JdbcConnection> liveConnectionMap, long[] responseTimes, int numRetries) throws SQLException {
        BigDecimal[] weightFactorArray = this.getWeightFactorArray(this.props, configuredHosts.size());

        Map<String, BigDecimal> allowHostMap = this.getAllowHostMap(proxy, configuredHosts, weightFactorArray);

        GlobalConnectionScheduler scheduler = GlobalConnectionScheduler.getInstance(allowHostMap.keySet().stream()
                .collect(Collectors.toMap(hostPort -> hostPort, hostPort -> 0L, (a, b) -> b, ConcurrentHashMap::new)));

        for (int attempts = 0; attempts < numRetries; ) {
            scheduler.getLock().lock();
            String selectedHost;
            ConnectionImpl conn;
            try {
                selectedHost = this.getBySed(allowHostMap, scheduler);
                conn = (ConnectionImpl) liveConnectionMap.get(selectedHost);
                if (conn == null) {
                    try {
                        conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(selectedHost);
                        scheduler.getCounter().incrementAndGet(selectedHost);
                        System.out.println(Thread.currentThread().getId() + " - " + scheduler.getCounter());
                    } catch (SQLException sqlEx) {
                        if (!((LoadBalancedConnectionProxy) proxy).shouldExceptionTriggerConnectionSwitch(sqlEx)) {
                            if (!StringUtils.isNullOrEmpty(selectedHost)) {
                                allowHostMap.remove(selectedHost);
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
        return null;
    }

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

    private String getBySed(Map<String, BigDecimal> allowHostMap, GlobalConnectionScheduler scheduler) {
        List<String> hostList = new ArrayList<>(allowHostMap.keySet());

        Map<String, BigDecimal> sortMap = new HashMap<>(hostList.size());
        for (String host : hostList) {
            BigDecimal weightFactor = allowHostMap.get(host);
            if (weightFactor.compareTo(BigDecimal.ZERO) == 0) {
                continue;
            }

            BigDecimal activeCount = new BigDecimal(scheduler.getCounter().get(host)).add(BigDecimal.ONE);
            BigDecimal integratedLoad = activeCount.divide(weightFactor, new MathContext(2, RoundingMode.HALF_UP));

            sortMap.put(host, integratedLoad);
        }

        Map<String, BigDecimal> sortedMap = sortMap.entrySet().stream().sorted(Entry.comparingByValue())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

        return new ArrayList<>(sortedMap.keySet()).get(0);
    }
}
