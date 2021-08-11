package com.tencent.tdsql.mysql.cj.jdbc.ha;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * It is a supplement to the shortest expected delay scheduling algorithm.If the number of connections of a server is
 * equal to zero, it will be allocated directly, and there is no need to
 * perform the shortest expected delay scheduling operation.The algorithm always guarantees that there are no servers
 * with zero connections.
 * <p>Fake code of the algorithm like this:</p>
 * <pre>
 *     {@code
 *     for (m = 0; m < n; m++) {
 *         if (W(Sm) > 0) {
 *             if (C(Sm) == 0) {
 *                 return Sm;
 *             }
 *         }
 *     }
 *
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
 * @see ShortestExpectDelayStrategy
 * @since v1.0.1
 */
public class NeverQueueStrategy extends ShortestExpectDelayStrategy {

    public NeverQueueStrategy(Properties props) {
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
        for (Entry<String, BigDecimal> entry : allowHostMap.entrySet()) {
            if (BigDecimal.ZERO.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return super.execute(allowHostMap, gcs);
    }
}
