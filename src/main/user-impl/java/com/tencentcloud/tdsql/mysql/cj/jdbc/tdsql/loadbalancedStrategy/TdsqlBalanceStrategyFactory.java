package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import java.sql.SQLException;

/**
 * <p>
 * 直连模式、负载均衡模式下的连接调度策略算法工程类
 * </p>
 *
 * @author dorianzhang@tencent.com
 * @author gyokumeixie@tencent.com
 */
public class TdsqlBalanceStrategyFactory {

    public static final String STRATEGY_LC = "lc";
    public static final String STRATEGY_SED = "sed";

    /**
     * <p>
     * 根据URL参数中给定的负载均衡策略，创建对应的负载均衡策略实例
     * </p>
     *
     * @param strategy 负载均衡策略
     * @return 创建的负载均衡策略实例
     * @see TdsqlLoadBalanceStrategy
     */
    public TdsqlLoadBalanceStrategy getStrategyInstance(String strategy) throws SQLException {
        TdsqlLoadBalanceStrategy balancer;
        try {
            switch (strategy.toLowerCase()) {
                case STRATEGY_LC:
                    balancer = new TdsqlLcBalanceStrategy();
                    break;
                case STRATEGY_SED:
                    balancer = new TdsqlSedBalanceStrategy();
                    break;
                default:
                    balancer = (TdsqlLoadBalanceStrategy) Class.forName(strategy).getDeclaredConstructor()
                            .newInstance();
            }
        } catch (Throwable t) {
            String errMessage = Messages.getString("InvalidLoadBalanceStrategy", new Object[]{strategy});
            logError(errMessage, t);
            throw SQLError.createSQLException(errMessage, MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        return balancer;
    }

    public TdsqlBalanceStrategyFactory() {
    }

    public static TdsqlBalanceStrategyFactory getInstance() {
        return TdsqlBalanceStrategyFactory.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlBalanceStrategyFactory INSTANCE = new TdsqlBalanceStrategyFactory();
    }
}