package tdsql.direct;

import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.cluster.DataSetUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataSetUtilTest {

    static class TestModel {
        public String infoStr;
        public List<DataSetInfo> expectedInfos;
        public String expectedError = "";

        public TestModel(String infoStr, List<DataSetInfo> expectedInfos) {
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
                        Arrays.asList(DataSetUtil.newMasterInfo("1.2.3.4", "3308", 0, true))),
                new TestModel("1.2.3.4:3308@88@-1",
                        Arrays.asList(DataSetUtil.newMasterInfo("1.2.3.4", "3308", 88, false))),
                new TestModel("1.2.3.4:3308@88", "Invalid master info length: 1.2.3.4:3308@88")
        );
        // ip:port@weight@is_watch@delay
        forSlave = Arrays.asList(
                new TestModel("1.2.3.4:3308@0@1@99",
                        Arrays.asList(DataSetUtil.newSlaveInfo("1.2.3.4", "3308", 0, true, 99L))),
                new TestModel("1.2.3.4:3308@88@0@88",
                        Arrays.asList(DataSetUtil.newSlaveInfo("1.2.3.4", "3308", 88, false, 88L))),
                new TestModel("1.2.3.4:3308@88@0@88, 5.6.7.8:8890@99@0@77",
                        Arrays.asList(
                                DataSetUtil.newSlaveInfo("1.2.3.4", "3308", 88, false, 88L),
                                DataSetUtil.newSlaveInfo("5.6.7.8", "8890", 99, false, 77L))),

                new TestModel("1.2.3.4:3308@88", "Invalid slave info length: 1.2.3.4:3308@88")
        );
    }

    @Test
    public void parseMasterTest() {
        for (TestModel testModel : forMaster) {
            DataSetInfo dataSetInfo = new DataSetInfo("null", "null");
            String error = "";
            try {
                dataSetInfo = DataSetUtil.parseMaster(testModel.infoStr);
            } catch (SQLException ex) {
                error = ex.getMessage();
            }
            if(!testModel.expectedError.equals("")) {
                assertEquals(testModel.expectedError, error);
            } else {
                assertDataSetInfo(testModel.expectedInfos.get(0), dataSetInfo);
            }
        }
    }

    @Test
    public void parseSlaveListTest() {
        for (TestModel testModel : forSlave) {
            List<DataSetInfo> dataSetInfos = new ArrayList<>();
            String error = "";
            try {
                dataSetInfos = DataSetUtil.parseSlaveList(testModel.infoStr);
            } catch (SQLException ex) {
                error = ex.getMessage();
            }
            if(!testModel.expectedError.equals("")) {
                assertEquals(testModel.expectedError, error);
            } else {
                assertEquals(dataSetInfos.size(), testModel.expectedInfos.size());
                for(int i=0; i<dataSetInfos.size(); i++) {
                    assertDataSetInfo(testModel.expectedInfos.get(i), dataSetInfos.get(i));
                }
            }
        }
    }

    void assertDataSetInfo(DataSetInfo expected, DataSetInfo actual) {
        assertEquals(expected.getIP(), actual.getIP());
        assertEquals(expected.getPort(), actual.getPort());
        assertEquals(expected.getWeight(), actual.getWeight());
        assertEquals(expected.getAlive(), actual.getAlive());
        assertEquals(expected.getDelay(), actual.getDelay());
        assertEquals(expected.getWatch(), actual.getWatch());
    }

}
