package com.tencent.tdsql.mysql.cj.jdbc.ha;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author dorianzhang@tencent.com
 */
public class ShortestExpectDelayStrategy extends AdvancedLoadBalanceStrategy {

    public ShortestExpectDelayStrategy(Properties props) {
        super(props);
    }

    /**
     * Specific implementation of different algorithms
     *
     * @param allowHostMap a map of host/ports to "total of live connection counts" to them
     * @param gcs <code>{@link GlobalConnectionScheduler}</code>
     * @return The host:port pair identifying the host to connect to.
     */
    @Override
    protected String execute(Map<String, BigDecimal> allowHostMap, GlobalConnectionScheduler gcs) {
        List<String> hostList = new ArrayList<>(allowHostMap.keySet());

        Map<String, BigDecimal> sortMap = new HashMap<>(hostList.size());
        for (String host : hostList) {
            BigDecimal weightFactor = allowHostMap.get(host);
            if (weightFactor.compareTo(BigDecimal.ZERO) == 0) {
                continue;
            }

            BigDecimal activeCount = new BigDecimal(gcs.getCounter().get(host)).add(BigDecimal.ONE);
            BigDecimal integratedLoad = activeCount.divide(weightFactor, new MathContext(2, RoundingMode.HALF_UP));

            sortMap.put(host, integratedLoad);
        }

        Map<String, BigDecimal> sortedMap = sortMap.entrySet().stream().sorted(Entry.comparingByValue())
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

        return new ArrayList<>(sortedMap.keySet()).get(0);
    }
}
