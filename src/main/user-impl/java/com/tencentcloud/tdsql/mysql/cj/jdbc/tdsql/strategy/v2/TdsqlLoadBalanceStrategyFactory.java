package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlInvalidConnectionPropertyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.algorithm.TdsqlLcAlgorithm;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2.algorithm.TdsqlSedAlgorithm;

/**
 * <p>TDSQL专属，负载均衡策略算法工厂类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlLoadBalanceStrategyFactory {

    public static <T extends TdsqlConnectionCounter> TdsqlLoadBalanceStrategy<T> getInstance(
            TdsqlLoadBalanceStrategyEnum strategyEnum) {
        TdsqlLoadBalanceStrategy<T> strategy;
        switch (strategyEnum) {
            case SED:
                strategy = new TdsqlSedAlgorithm<>();
                break;
            case LC:
                strategy = new TdsqlLcAlgorithm<>();
                break;
            case UNKNOWN:
            default:
                throw TdsqlExceptionFactory.createException(TdsqlInvalidConnectionPropertyException.class,
                        Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceStrategy",
                                new Object[]{strategyEnum.getStrategyName()}));
        }
        return strategy;
    }
}
