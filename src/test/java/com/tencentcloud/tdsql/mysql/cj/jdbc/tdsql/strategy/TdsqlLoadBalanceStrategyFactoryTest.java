package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v1.TdsqlLoadBalanceStrategy;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v1.TdsqlLoadBalanceStrategyFactory;
import org.junit.jupiter.api.Test;
import java.sql.SQLException;

class TdsqlLoadBalanceStrategyFactoryTest {

    @Test
    public void testStrategyFactory() throws SQLException {
        GetInstance getInstance = new GetInstance();
        GetInstanceNew getInstanceNew = new GetInstanceNew();
        for (int i = 0; i < 10; i++) {
            Thread thread = new Thread(getInstance, "Thread Lc" + i);
            thread.start();
            Thread thread1 = new Thread(getInstanceNew, "Thread Sed" + i);
            thread1.start();
        }

    }
    class GetInstance implements Runnable{
        @Override
        public void run() {
            TdsqlLoadBalanceStrategyFactory instance = TdsqlLoadBalanceStrategyFactory.getInstance();
            TdsqlLoadBalanceStrategy lc = null;
            try {
                lc = instance.getStrategyInstance("Lc");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + "：" + lc);
            System.out.println();
        }
    }
    class GetInstanceNew implements Runnable{
        @Override
        public void run() {
            TdsqlLoadBalanceStrategyFactory instance = TdsqlLoadBalanceStrategyFactory.getInstance();
            TdsqlLoadBalanceStrategy lc = null;
            try {
                lc = instance.getStrategyInstance("Sed");
            } catch (SQLException e) {
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + "：" + lc);
            System.out.println();
        }
    }

}