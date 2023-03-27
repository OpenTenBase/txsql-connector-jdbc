package tdsql.direct.v1;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.cluster.TdsqlDataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v1.cluster.TdsqlDataSetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TdsqlDataSetUtilTest {

    static class TestModel {
        public String infoStr;
        public List<TdsqlDataSetInfo> expectedInfos;
        public String expectedError = "";

        public TestModel(String infoStr, List<TdsqlDataSetInfo> expectedInfos) {
            this.infoStr = infoStr;
            this.expectedInfos = expectedInfos;
        }

        public TestModel(String infoStr, String expectedError) {
            this.infoStr = infoStr;
            this.expectedError = expectedError;
        }
    }

    static List<TestModel> forMaster;
    static List<TestModel> forSlave;

    @BeforeAll
    public static void init() throws ClassNotFoundException{
        // ip:port@weight@alive
        forMaster = Arrays.asList(
                new TestModel("1.2.3.4:3308@0@0",
                        Arrays.asList(TdsqlDataSetUtil.newMasterInfo("1.2.3.4", "3308", 0, true))),
                new TestModel("1.2.3.4:3308@88@-1",
                        Arrays.asList(TdsqlDataSetUtil.newMasterInfo("1.2.3.4", "3308", 88, false))),
                new TestModel("1.2.3.4:3308@88", "Invalid master info length: 1.2.3.4:3308@88")
        );
        // ip:port@weight@is_watch@delay
        forSlave = Arrays.asList(
                new TestModel("1.2.3.4:3308@0@1@99",
                        Arrays.asList(TdsqlDataSetUtil.newSlaveInfo("1.2.3.4", "3308", 0, true, 99L))),
                new TestModel("1.2.3.4:3308@88@0@88",
                        Arrays.asList(TdsqlDataSetUtil.newSlaveInfo("1.2.3.4", "3308", 88, false, 88L))),
                new TestModel("1.2.3.4:3308@88@0@88, 5.6.7.8:8890@99@0@77",
                        Arrays.asList(
                                TdsqlDataSetUtil.newSlaveInfo("1.2.3.4", "3308", 88, false, 88L),
                                TdsqlDataSetUtil.newSlaveInfo("5.6.7.8", "8890", 99, false, 77L))),

                new TestModel("1.2.3.4:3308@88", "Invalid slave info length: 1.2.3.4:3308@88")
        );
    }

    @Test
    public void parseMasterTest() {
        for (TestModel testModel : forMaster) {
            TdsqlDataSetInfo tdsqlDataSetInfo = new TdsqlDataSetInfo("null", "null");
            String error = "";
            try {
                tdsqlDataSetInfo = TdsqlDataSetUtil.parseMaster(testModel.infoStr);
            } catch (SQLException ex) {
                error = ex.getMessage();
            }
            if(!testModel.expectedError.equals("")) {
                assertEquals(testModel.expectedError, error);
            } else {
                assertDataSetInfo(testModel.expectedInfos.get(0), tdsqlDataSetInfo);
            }
        }
    }

    @Test
    public void parseSlaveListTest() {
        for (TestModel testModel : forSlave) {
            List<TdsqlDataSetInfo> tdsqlDataSetInfos = new ArrayList<>();
            String error = "";
            try {
                tdsqlDataSetInfos = TdsqlDataSetUtil.parseSlaveList(testModel.infoStr);
            } catch (SQLException ex) {
                error = ex.getMessage();
            }
            if(!testModel.expectedError.equals("")) {
                assertEquals(testModel.expectedError, error);
            } else {
                assertEquals(tdsqlDataSetInfos.size(), testModel.expectedInfos.size());
                for(int i=0; i< tdsqlDataSetInfos.size(); i++) {
                    assertDataSetInfo(testModel.expectedInfos.get(i), tdsqlDataSetInfos.get(i));
                }
            }
        }
    }

    void assertDataSetInfo(TdsqlDataSetInfo expected, TdsqlDataSetInfo actual) {
        assertEquals(expected.getIp(), actual.getIp());
        assertEquals(expected.getPort(), actual.getPort());
        assertEquals(expected.getWeight(), actual.getWeight());
        assertEquals(expected.getAlive(), actual.getAlive());
        assertEquals(expected.getDelay(), actual.getDelay());
        assertEquals(expected.getWatch(), actual.getWatch());
    }

}
