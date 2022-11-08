package tdsql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.tencentcloud.tdsql.mysql.cj.NativeSession;
import com.tencentcloud.tdsql.mysql.cj.ServerPreparedQuery;
import com.tencentcloud.tdsql.mysql.cj.Session;
import com.tencentcloud.tdsql.mysql.cj.jdbc.JdbcConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;

public class PrepareEOFTest {

    private static final String URL = "jdbc:tdsql-mysql://9.30.2.94:15003/test?useServerPrepStmts=true";
    private static final String USERNAME = "test3";
    private static final String PASSWORD = "test3";

    @BeforeAll
    public static void setUp() {
        try {
            Class.forName("com.tencentcloud.tdsql.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @RepeatedTest(100)
    public void testEOFPacketForPrepareStmt() throws Exception {
        Connection testConn = DriverManager.getConnection(URL, USERNAME, PASSWORD);

        Session session = ((JdbcConnection) (testConn)).getSession();

        boolean checkEOF = !session.getServerSession().isEOFDeprecated();

        try {
            ServerPreparedQuery query = ServerPreparedQuery.getInstance((NativeSession) session);

            // two placeholders (?) and one column (col1)
            query.serverPrepare("SELECT CONCAT(?, ?) as col1;");

            if (checkEOF) {
                // check the Parameter Definition Block and make sure both of the fields are not null
                assertEquals(query.getParameterFields().length, 2);
                assertNotNull(query.getParameterFields()[0]);
                assertNotNull(query.getParameterFields()[1]);

                // check the Column Definition Block and make sure the field is not null
                assertEquals(query.getResultFields().getFields().length, 1);
                assertNotNull(query.getResultFields().getFields()[0]);

                // Should be 6 packets in total:
                // one description packet
                // Parameter Definition Block: two Parameter definition packets and one EOF packet
                // Column Definition Block: one Column definition packets and one EOF packet
                assertEquals(query.session.getProtocol().getPacketReader().getMessageSequence(), 6);
            }
        } finally {
            testConn.close();
        }
    }
}
