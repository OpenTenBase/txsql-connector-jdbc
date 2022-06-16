package tdsql.loadbalance;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.StringJoiner;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import tdsql.loadbalance.base.BaseTest;

@TestMethodOrder(OrderAnnotation.class)
public class UrlTest extends BaseTest {

    @Test
    @Order(1)
    public void testSingle() {
        connect(String.format("jdbc:tdsql-mysql://%s/%s", PROXY_16, DB_MYSQL));
    }

    @Test
    @Order(2)
    public void testLoadBalance() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(PROXY_16).add(PROXY_44).add(PROXY_46).add(PROXY_48);
        connect(String.format("jdbc:tdsql-mysql:loadbalance://%s/%s", joiner, DB_MYSQL));
    }

    @Test
    @Order(3)
    public void testReplication() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(PROXY_16).add(PROXY_44).add(PROXY_46).add(PROXY_48);
        connect(String.format("jdbc:tdsql-mysql:replication://%s/%s", joiner, DB_MYSQL));
    }

    @Test
    @Order(4)
    public void testFailover() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(PROXY_16).add(PROXY_44).add(PROXY_46).add(PROXY_48);
        connect(String.format("jdbc:tdsql-mysql://%s/%s", joiner, DB_MYSQL));
    }

    @Test
    @Order(5)
    public void testLoadBalanceWithProperties() {
        connect(LB_URL);
    }

    private void connect(String url) {
        try (Connection conn = DriverManager.getConnection(url, USER, PASS)) {
            assertNotNull(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
