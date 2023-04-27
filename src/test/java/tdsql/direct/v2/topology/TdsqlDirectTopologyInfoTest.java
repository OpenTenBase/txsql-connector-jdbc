package tdsql.direct.v2.topology;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum.RW;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.EMPTY_STRING;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectParseTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectTopologyInfo;
import java.util.Collections;
import java.util.Iterator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * <p>TDSQL专属 - 直连模式 - 拓扑信息单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectTopologyInfoTest {

    /**
     * 根据主备字符串创建正确的拓扑信息
     */
    @Test
    public void testCase01() {
        TdsqlDirectTopologyInfo topoInfo = new TdsqlDirectTopologyInfo("1234567890", "set_1668741553_9550901", RW,
                false,
                "9.30.2.89:4017@100@0",
                "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0");

        Assertions.assertEquals("1234567890", topoInfo.getDataSourceUuid());
        Assertions.assertEquals(RW, topoInfo.getRwMode());
        Assertions.assertEquals("set_1668741553_9550901", topoInfo.getClusterName());

        Assertions.assertEquals("9.30.2.89", topoInfo.getMasterTopologyInfo().getIp());
        Assertions.assertEquals(4017, topoInfo.getMasterTopologyInfo().getPort());
        Assertions.assertEquals(100, topoInfo.getMasterTopologyInfo().getWeight());
        Assertions.assertEquals(0, topoInfo.getMasterTopologyInfo().getIsAlive());

        Assertions.assertEquals(3, topoInfo.getSlaveTopologyInfoSet().size());

        Iterator<TdsqlDirectSlaveTopologyInfo> iterator = topoInfo.getSlaveTopologyInfoSet().iterator();
        TdsqlDirectSlaveTopologyInfo next = iterator.next();

        Assertions.assertEquals("9.30.0.250", next.getIp());
        Assertions.assertEquals(4017, next.getPort());
        Assertions.assertEquals(100, next.getWeight());
        Assertions.assertEquals(0, next.getIsWatch());
        Assertions.assertEquals(0, next.getDelay());

        next = iterator.next();

        Assertions.assertEquals("9.30.2.116", next.getIp());
        Assertions.assertEquals(4017, next.getPort());
        Assertions.assertEquals(100, next.getWeight());
        Assertions.assertEquals(0, next.getIsWatch());
        Assertions.assertEquals(0, next.getDelay());

        next = iterator.next();

        Assertions.assertEquals("9.30.2.94", next.getIp());
        Assertions.assertEquals(4017, next.getPort());
        Assertions.assertEquals(50, next.getWeight());
        Assertions.assertEquals(0, next.getIsWatch());
        Assertions.assertEquals(0, next.getDelay());
    }

    /**
     * 读写模式参数测试
     *
     * @param rwModeEnum {@link TdsqlDirectReadWriteModeEnum}
     */
    @ParameterizedTest
    @EnumSource
    public void testCase02(TdsqlDirectReadWriteModeEnum rwModeEnum) {
        TdsqlDirectTopologyInfo structureInfo = new TdsqlDirectTopologyInfo("1234567890", "set_1668741553_9550901",
                rwModeEnum, false,
                "9.30.2.89:4017@100@0",
                "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0");

        Assertions.assertEquals(rwModeEnum, structureInfo.getRwMode());
    }

    /**
     * 测试NULL和空主库信息字符串
     *
     * @param masterInfoStr 主库信息字符串
     */
    @ParameterizedTest
    @NullAndEmptySource
    public void testCase03(String masterInfoStr) {
        // 读写模式，主库字符串不允许为NULL或者空
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false, masterInfoStr,
                        "don't care"));

        // 只读模式，主库字符串允许为空，备库字符串不允许为空
        Assertions.assertDoesNotThrow(() -> {
            TdsqlDirectTopologyInfo topoInfo = new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false,
                    masterInfoStr,
                    "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0");

            Assertions.assertEquals(EMPTY_STRING, topoInfo.getMasterTopologyInfo().getIp());
            Assertions.assertEquals(-99, topoInfo.getMasterTopologyInfo().getPort());
            Assertions.assertEquals(-99, topoInfo.getMasterTopologyInfo().getWeight());
            Assertions.assertEquals(-99, topoInfo.getMasterTopologyInfo().getIsAlive());

            Assertions.assertEquals(3, topoInfo.getSlaveTopologyInfoSet().size());

            Iterator<TdsqlDirectSlaveTopologyInfo> iterator = topoInfo.getSlaveTopologyInfoSet().iterator();
            TdsqlDirectSlaveTopologyInfo next = iterator.next();

            Assertions.assertEquals("9.30.0.250", next.getIp());
            Assertions.assertEquals(4017, next.getPort());
            Assertions.assertEquals(100, next.getWeight());
            Assertions.assertEquals(0, next.getIsWatch());
            Assertions.assertEquals(0, next.getDelay());

            next = iterator.next();

            Assertions.assertEquals("9.30.2.116", next.getIp());
            Assertions.assertEquals(4017, next.getPort());
            Assertions.assertEquals(100, next.getWeight());
            Assertions.assertEquals(0, next.getIsWatch());
            Assertions.assertEquals(0, next.getDelay());

            next = iterator.next();

            Assertions.assertEquals("9.30.2.94", next.getIp());
            Assertions.assertEquals(4017, next.getPort());
            Assertions.assertEquals(50, next.getWeight());
            Assertions.assertEquals(0, next.getIsWatch());
            Assertions.assertEquals(0, next.getDelay());
        });
    }

    /**
     * 测试NULL和空备库字符串
     *
     * @param slavesInfoStr 备库信息字符串
     */
    @ParameterizedTest
    @NullAndEmptySource
    public void testCase04(String slavesInfoStr) {
        // 读写模式，备库字符串允许为NULL或空
        Assertions.assertDoesNotThrow(() -> {
            TdsqlDirectTopologyInfo structureInfo = new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false,
                    "9.30.2.89:4017@100@0", slavesInfoStr);

            Assertions.assertEquals("9.30.2.89", structureInfo.getMasterTopologyInfo().getIp());
            Assertions.assertEquals(4017, structureInfo.getMasterTopologyInfo().getPort());
            Assertions.assertEquals(100, structureInfo.getMasterTopologyInfo().getWeight());
            Assertions.assertEquals(0, structureInfo.getMasterTopologyInfo().getIsAlive());

            Assertions.assertEquals(Collections.emptySet(), structureInfo.getSlaveTopologyInfoSet());
        });

        // 只读模式，备库字符串不允许为NULL或空
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, "don't care",
                        slavesInfoStr));
    }

    /**
     * 测试只有分隔符的主库字符串
     *
     * @param masterInfoStr 主库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"@", "@@@"})
    public void testCase05(String masterInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false, masterInfoStr,
                        "don't care"));

        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, masterInfoStr,
                        "don't care"));
    }

    /**
     * 测试只有分隔符的备库字符串
     *
     * @param slavesInfoStr 备库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"@@", "@@@@"})
    public void testCase06(String slavesInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class, () -> {
            TdsqlDirectTopologyInfo structureInfo = new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false,
                    "9.30.2.89:4017@100@0", slavesInfoStr);

            Assertions.assertEquals("9.30.2.89", structureInfo.getMasterTopologyInfo().getIp());
            Assertions.assertEquals(4017, structureInfo.getMasterTopologyInfo().getPort());
            Assertions.assertEquals(100, structureInfo.getMasterTopologyInfo().getWeight());
            Assertions.assertEquals(0, structureInfo.getMasterTopologyInfo().getIsAlive());
        });

        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, "don't care",
                        slavesInfoStr));
    }

    /**
     * 测试缺失信息的主库字符串
     *
     * @param masterInfoStr 主库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"@100@0", "9.30.2.89@100@0", "4017@100@0", "9.30.2.89:@100@0", ":4017@100@0"})
    public void testCase07(String masterInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false, masterInfoStr,
                        "don't care"));
    }

    /**
     * 测试缺失信息的主库字符串
     *
     * @param masterInfoStr 主库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"9.30.2.89:4017@@0", "9.30.2.89:4017@-1@0", "9.30.2.89:4017@101@0", "9.30.2.89:4017@abc@0"})
    public void testCase08(String masterInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false, masterInfoStr,
                        "don't care"));
    }

    /**
     * 测试缺失信息的主库字符串
     *
     * @param masterInfoStr 主库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"9.30.2.89:4017@0@", "9.30.2.89:4017@0@-2", "9.30.2.89:4017@0@1", "9.30.2.89:4017@0@a"})
    public void testCase09(String masterInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RW, false, masterInfoStr,
                        "don't care"));
    }

    /**
     * 测试缺失信息的备库字符串
     *
     * @param slavesInfoStr 备库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {
            ",,",
            ",,9.30.2.94:4017@50@0@0",
            ",9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0",
            "@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0",
            "4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0",
            ":4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:4017@100@0@0,@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:4017@100@0@0,4017@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116:@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:4017@100@0@0,:4017@100@0@0,9.30.2.94:4017@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,4017@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,9.30.2.94:@50@0@0",
            "9.30.0.250:4017@100@0@0,9.30.2.116:4017@100@0@0,:4017@50@0@0",
    })
    public void testCase10(String slavesInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, "9.30.2.89:4017@100@0",
                        slavesInfoStr));
    }

    /**
     * 测试缺失信息的备库字符串
     *
     * @param slavesInfoStr 备库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"9.30.2.94:4017@@0", "9.30.2.94:4017@-1@0", "9.30.2.94:4017@101@0", "9.30.2.94:4017@abc@0"})
    public void testCase11(String slavesInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, "9.30.2.89:4017@100@0",
                        slavesInfoStr));
    }

    /**
     * 测试错误信息的备库字符串
     *
     * @param slavesInfoStr 备库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"9.30.2.94:4017@@0@0", "9.30.2.94:4017@50@-1@0", "9.30.2.94:4017@50@2@0",
            "9.30.2.94:4017@abc@0"})
    public void testCase12(String slavesInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, "9.30.2.89:4017@100@0",
                        slavesInfoStr));
    }

    /**
     * 测试错误信息的备库字符串
     *
     * @param slavesInfoStr 备库字符串
     */
    @ParameterizedTest
    @ValueSource(strings = {"9.30.2.94:4017@50@0@", "9.30.2.94:4017@50@0@-1", "9.30.2.94:4017@50@0@abc"})
    public void testCase13(String slavesInfoStr) {
        Assertions.assertThrows(TdsqlDirectParseTopologyException.class,
                () -> new TdsqlDirectTopologyInfo("don't care", "don't care", RO, false, "9.30.2.89:4017@100@0",
                        slavesInfoStr));
    }
}
