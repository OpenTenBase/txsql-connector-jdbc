package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

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
 * Based on the weighted least connection scheduling algorithm, it is an improvement to it.The main difference between
 * the two is that the shortest expected delay scheduling does not use the inactive
 * connection value when judging the total cost value of each cluster node, and adds 1 to the active connection value
 * to predict the total cost value.
 * <p>Fake code of the algorithm like this:</p>
 * <pre>
 *     {@code
 *     for (m = 0; m < n; m++) {
 *         if (W(Sm) > 0) {
 *             for (i = m+1; i < n; i++) {
 *                 if ((C(Sm) + 1)*W(Si) > (C(Si) + 1)*W(Sm))
 *                     m = i;
 *             }
 *             return Sm;
 *         }
 *     }
 *     return NULL;
 *     }
 * </pre>
 *
 * @author <a href="mailto:dorianzhang@tencent.com">dorianzhang</a>
 * @version v1.0
 * @see AdvancedLoadBalanceStrategy
 * @see NeverQueueStrategy
 * @since v1.0.1
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
