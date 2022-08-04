package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.loadbalancedStrategy;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.exceptions.MysqlErrorNumbers;
import com.tencentcloud.tdsql.mysql.cj.jdbc.exceptions.SQLError;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;

import java.sql.SQLException;

/**
 * <p></p>
 *
 * @author gyokumeixie@tencent.com
 */
public class TdsqlDirectLoadBalanceStrategyFactory{
    private TdsqlLoadBalanceStrategy balancer;

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
        try {
            switch (strategy){
                case "Lc":
                    this.balancer = new TdsqlLcBalanceStrategy();
                    break;
                case "Sed":
                    this.balancer = new TdsqlSedBalanceStrategy();
                    break;
                default:
                    this.balancer = (TdsqlLoadBalanceStrategy) Class.forName(strategy).newInstance();
            }
        } catch (Throwable t) {
            String errMseeage = Messages.getString("InvalidLoadBalanceStrategy", new Object[] { strategy });
            System.out.println(errMseeage);
            TdsqlLoggerFactory.logError(errMseeage, t);
            throw SQLError.createSQLException(errMseeage, MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }
        return balancer;
    }
    public TdsqlDirectLoadBalanceStrategyFactory(){
    }
    public static TdsqlDirectLoadBalanceStrategyFactory getInstance() {
        return TdsqlDirectLoadBalanceStrategyFactory.SingletonInstance.INSTANCE;
    }

    private static class SingletonInstance {

        private static final TdsqlDirectLoadBalanceStrategyFactory INSTANCE = new TdsqlDirectLoadBalanceStrategyFactory();
    }
}