package tdsql.loadbalance;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.StringJoiner;
import org.junit.jupiter.api.Assertions;
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
        connect(String.format("jdbc:tdsql-mysql://%s/%s", PROXY_21, DB_MYSQL));
    }

    @Test
    @Order(2)
    public void testLoadBalance() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(PROXY_21).add(PROXY_22).add(PROXY_23).add(PROXY_24);
        connect(String.format("jdbc:tdsql-mysql:loadbalance://%s/%s", joiner, DB_MYSQL));
    }

    @Test
    @Order(3)
    public void testReplication() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(PROXY_21).add(PROXY_22).add(PROXY_23).add(PROXY_24);
        connect(String.format("jdbc:tdsql-mysql:replication://%s/%s", joiner, DB_MYSQL));
    }

    @Test
    @Order(4)
    public void testFailover() {
        StringJoiner joiner = new StringJoiner(",");
        joiner.add(PROXY_21).add(PROXY_22).add(PROXY_23).add(PROXY_24);
        connect(String.format("jdbc:tdsql-mysql://%s/%s", joiner, DB_MYSQL));
    }

    @Test
    @Order(5)
    public void testLoadBalanceWithProperties() {
        connect(LB_URL);
    }

    @Test
    @Order(6)
    public void testLoadBalanceError() {
        String url =
                "jdbc:tdsql-mysql:loadbalance://152.136.175.228:8960/" + DB_MYSQL + LB_URL_PROPS;
        Assertions.assertThrows(SQLNonTransientConnectionException.class, () -> {
            Connection conn = DriverManager.getConnection(url, USER, PASS);
            assertNull(conn);
        });
    }

    private void connect(String url) {
        try (Connection conn = DriverManager.getConnection(url, USER, PASS)) {
            assertNotNull(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
