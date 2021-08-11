package com.tencent.tdsql.mysql.cj.jdbc.ha;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;

/**
 * @author dorianzhang@tencent.com
 */
public class NeverQueueStrategy extends AdvancedLoadBalanceStrategy {

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
        return null;
    }
}
