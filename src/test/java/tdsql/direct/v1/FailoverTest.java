package tdsql.direct.v1;

import com.alibaba.druid.pool.DruidDataSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class FailoverTest {
    private static final String DB_URL = "jdbc:mysql:direct://9.30.0.250:15012,9.30.2.116:15012,9.30.2.89:15012,9.30.2.94:15012/lisheng"
            + "?useLocalSessionStates=true"
            + "&useUnicode=true"
            + "&characterEncoding=utf-8"
            + "&serverTimezone=Asia/Shanghai"
            + "&tdsqlDirectMaxSlaveDelaySeconds=10&useSSL=false&tdsqlDirectReadWriteMod=rw";
    private static final String USERNAME = "qt4s";
    private static final String PASSWORD = "g<m:7KNDF.L1<^1C";
    private static final String DRIVER_NAME = "com.mysql.cj.jdbc.Driver";
    private static final DruidDataSource dataSource = new DruidDataSource();

    static {
        try {
            Class.forName(DRIVER_NAME);

            dataSource.setUrl(DB_URL);
            dataSource.setUsername(USERNAME);
            dataSource.setPassword(PASSWORD);
            dataSource.setDriverClassName(DRIVER_NAME);
            ;
//            dataSource.setInitialSize(10);
//            dataSource.setMaxActive(20);
//            dataSource.setMinIdle(10);
//            dataSource.setValidationQuery("select 1");
//            dataSource.setTimeBetweenEvictionRunsMillis(10000);
//            dataSource.setTestWhileIdle(true);
//            //dataSource.setPhyTimeoutMillis(20000);
//            dataSource.setMaxWait(3000);

            // 初始连接数
            dataSource.setInitialSize(10);
// 最小连接池数量
            dataSource.setMinIdle(10);
// 最大连接池数量
            dataSource.setMaxActive(100);
// 配置获取连接等待超时时间  毫秒
            dataSource.setMaxWait(100);
//缓存通过以下两个方法发起的SQL:
            dataSource.setPoolPreparedStatements(true);
//每个连接最多缓存多少个SQL
            dataSource.setMaxPoolPreparedStatementPerConnectionSize(50);
//检查空闲连接的频率，单位毫秒, 非正整数时表示不进行检查
            dataSource.setTimeBetweenEvictionRunsMillis(-1);
//池中某个连接的空闲时长达到 N 毫秒后, 连接池在下次检查空闲连接时，将回收该连接,要小于防火墙超时设置
            dataSource.setMinEvictableIdleTimeMillis(300000);
//当程序请求连接，池在分配连接时，是否先检查该连接是否有效。(高效)
            dataSource.setTestWhileIdle(true);
// 程序 申请 连接时,进行连接有效性检查（低效，影响性能）
            dataSource.setTestOnBorrow(false);
//程序 返还 连接时,进行连接有效性检查（低效，影响性能）
            dataSource.setTestOnReturn(false);
// 要求程序从池中get到连接后, N 秒后必须close,否则druid 会强制回收该连接,不管该连接中是活动还是空闲, 以防止进程不会进行close而霸占连接。
            dataSource.setRemoveAbandoned(true);
// 设置druid 强制回收连接的时限，当程序从池中get到连接开始算起，超过此值后，druid将强制回收该连接，单位秒。
// 结合业务来看，存在jpa极大事务；不好设置 暂时为设置两分钟
            dataSource.setRemoveAbandonedTimeout(120);

            dataSource.init();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Disabled
    @Test
    public void testFailOver() throws InterruptedException {
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                1000,
                1000,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(10));
        while (true) {
            Thread.sleep(100);
            executorService.execute(() -> {
                final long cur = System.currentTimeMillis();
                Connection conn = null;
                try {
                    conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT `id`, `name1` FROM t_user limit 1");
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    while (rs.next()) {
                        System.out.println("ID: " + rs.getLong(1));
                    }
                    rs.close();
                    stmt.executeUpdate("insert into t_user(name1,desc1)  values('a','qta-josephpu.test-b')");
                    final long cost = System.currentTimeMillis();
                    System.out.println("正常结束，耗时：" + (cost - cur));
                    stmt.close();
                    conn.close();
                } catch (SQLException e) {
                    try {
                        conn.close();
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    }
                    final long cost_err = System.currentTimeMillis();
                    System.out.println("异常结束，时间：" + cost_err);
                    e.printStackTrace();
                }
            });
        }
    }

    @Test
    @Disabled
    public void testFailOverInDataPool() throws InterruptedException {
        ThreadPoolExecutor executorService = new ThreadPoolExecutor(
                100,
                100,
                0L,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
        while (true) {
            TimeUnit.MILLISECONDS.sleep(10);
            try {
                executorService.execute(new QueryTask());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class QueryTask implements Runnable {

        @Override
        public void run() {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT `id`, `name1` FROM t_user limit 1")) {
                while (rs.next()) {
                    System.out.println("ID: " + rs.getLong(1));
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
