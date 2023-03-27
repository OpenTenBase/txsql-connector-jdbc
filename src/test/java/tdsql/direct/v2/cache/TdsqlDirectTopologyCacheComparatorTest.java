package tdsql.direct.v2.cache;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.NO_CHANGE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_ALL_ATTR_CHANGE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_DELAY_CHANGE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_OFFLINE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_ONLINE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SLAVE_WEIGHT_CHANGE;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum.SWITCH;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheComparator;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.MasterResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyCacheCompareResult.SlaveResult;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache.TdsqlDirectTopologyChangeEventEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectCompareTopologyException;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectMasterTopologyInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.topology.TdsqlDirectSlaveTopologyInfo;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tdsql.direct.v2.base.TdsqlDirectBaseTest;

/**
 * <p>TDSQL专属 - 直连模式 - 拓扑缓存比较器单元测试用例</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectTopologyCacheComparatorTest extends TdsqlDirectBaseTest {

    private TdsqlDirectTopologyCacheComparator comparator;

    @BeforeEach
    public void beforeEach() {
        this.comparator = new TdsqlDirectTopologyCacheComparator(super.defaultDataSourceUuid);
    }

    /**
     * 主库比较
     */
    @Test
    public void testCase01() {
        // 新主库不能为NULL
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class,
                () -> this.comparator.compareMaster(super.newMasterTopologyInfo(), null));

        // 已缓存主库不能为NULL
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class,
                () -> this.comparator.compareMaster(null, super.newMasterTopologyInfo()));

        // 两者均不能为NULL
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class,
                () -> this.comparator.compareMaster(null, null));

        // 两个空主库比较结果应该为无变化
        Assertions.assertDoesNotThrow(() -> {
            MasterResult masterResult = this.comparator.compareMaster(TdsqlDirectMasterTopologyInfo.emptyMaster(),
                    TdsqlDirectMasterTopologyInfo.emptyMaster());

            Assertions.assertNotNull(masterResult);
            Assertions.assertTrue(masterResult.isNoChange());
            Assertions.assertEquals(NO_CHANGE, masterResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(masterResult.getOldMaster());
            Assertions.assertNull(masterResult.getNewMaster());
        });

        // 两个相同信息主库比较结果应该为无变化
        Assertions.assertDoesNotThrow(() -> {
            TdsqlDirectMasterTopologyInfo m1 = new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid,
                    "1.1.1.1:1111@0@0");
            TdsqlDirectMasterTopologyInfo m2 = new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid,
                    "1.1.1.1:1111@0@0");

            MasterResult masterResult = this.comparator.compareMaster(m1, m2);

            Assertions.assertNotNull(masterResult);
            Assertions.assertTrue(masterResult.isNoChange());
            Assertions.assertEquals(NO_CHANGE, masterResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(masterResult.getOldMaster());
            Assertions.assertNull(masterResult.getNewMaster());
        });

        // 不同信息主库比较结果应该为主库切换
        Assertions.assertDoesNotThrow(() -> {
            TdsqlDirectMasterTopologyInfo m1 = new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid,
                    "1.1.1.1:1111@0@0");
            TdsqlDirectMasterTopologyInfo m2 = new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid,
                    "2.2.2.2:2222@0@0");

            MasterResult masterResult = this.comparator.compareMaster(m1, m2);

            Assertions.assertNotNull(masterResult);
            Assertions.assertFalse(masterResult.isNoChange());
            Assertions.assertEquals(SWITCH, masterResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertEquals(m1, masterResult.getOldMaster());
            Assertions.assertEquals(m2, masterResult.getNewMaster());
        });

        // 属性信息不同的主库比较结果应该为无变化
        Assertions.assertDoesNotThrow(() -> {
            TdsqlDirectMasterTopologyInfo m1 = new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid,
                    "1.1.1.1:1111@1@0");
            TdsqlDirectMasterTopologyInfo m2 = new TdsqlDirectMasterTopologyInfo(defaultDataSourceUuid,
                    "1.1.1.1:1111@2@-1");

            MasterResult masterResult = this.comparator.compareMaster(m1, m2);

            Assertions.assertNotNull(masterResult);
            Assertions.assertTrue(masterResult.isNoChange());
            Assertions.assertEquals(NO_CHANGE, masterResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(masterResult.getOldMaster());
            Assertions.assertNull(masterResult.getNewMaster());
        });
    }

    /**
     * 从库比较
     */
    @Test
    public void testCase02() {
        // 新从库不能为NULL
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class,
                () -> this.comparator.compareSlaves(Collections.emptySet(), null));

        // 已缓存从库不能为NULL
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class,
                () -> this.comparator.compareSlaves(null, Collections.emptySet()));

        // 两者均不能为NULL
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class,
                () -> this.comparator.compareSlaves(null, null));

        // 新从库集合不能为空
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class, () -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(2);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@1@1@1"));

            this.comparator.compareSlaves(s1, Collections.emptySet());
        });

        // 已缓存从库集合不能为空
        Assertions.assertThrows(TdsqlDirectCompareTopologyException.class, () -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(2);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@1@1@1"));

            this.comparator.compareSlaves(Collections.emptySet(), s1);
        });

        Assertions.assertDoesNotThrow(() -> {
            // 两个空备库集合比较结果应为无变化
            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(Collections.emptySet(),
                    Collections.emptySet());

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            SlaveResult slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertTrue(slaveResult.isNoChange());
            Assertions.assertEquals(NO_CHANGE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(slaveResult.getOldSlaveSet());
            Assertions.assertNull(slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());

            // 备库上线
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            slaveResultSet = this.comparator.compareSlaves(Collections.emptySet(), s1);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertEquals(SLAVE_ONLINE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(slaveResult.getOldSlaveSet());
            Assertions.assertEquals(s1, slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());

            // 备库下线
            slaveResultSet = this.comparator.compareSlaves(s1, Collections.emptySet());

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertEquals(SLAVE_OFFLINE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertEquals(s1, slaveResult.getOldSlaveSet());
            Assertions.assertNull(slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());

            // 相同备库集合比较结果应为无变化
            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertTrue(slaveResult.isNoChange());
            Assertions.assertEquals(NO_CHANGE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(slaveResult.getOldSlaveSet());
            Assertions.assertNull(slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());
        });
    }

    @Test
    public void testCase03() {
        Assertions.assertDoesNotThrow(() -> {
            // 备库上线
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(Collections.emptySet(), s1);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            SlaveResult slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertEquals(SLAVE_ONLINE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(slaveResult.getOldSlaveSet());
            Assertions.assertEquals(new LinkedHashSet<TdsqlDirectSlaveTopologyInfo>(3) {{
                add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
                add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
                add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));
            }}, slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());

            // 备库下线
            slaveResultSet = this.comparator.compareSlaves(s1, Collections.emptySet());

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertEquals(SLAVE_OFFLINE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertEquals(new LinkedHashSet<TdsqlDirectSlaveTopologyInfo>(3) {{
                add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
                add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
                add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));
            }}, slaveResult.getOldSlaveSet());
            Assertions.assertNull(slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());
        });

        // 既有备库上线又有备库下线
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "4.4.4.4:4444@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "5.5.5.5:5555@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "6.6.6.6:6666@0@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(2, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                switch (changeEventEnum) {
                    case SLAVE_ONLINE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertEquals(new LinkedHashSet<TdsqlDirectSlaveTopologyInfo>(3) {{
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "4.4.4.4:4444@0@0@0"));
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "5.5.5.5:5555@0@0@0"));
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "6.6.6.6:6666@0@0@0"));
                        }}, slaveResult.getNewSlaveSet());
                        Assertions.assertNull(slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_OFFLINE:
                        Assertions.assertEquals(new LinkedHashSet<TdsqlDirectSlaveTopologyInfo>(3) {{
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));
                        }}, slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertNull(slaveResult.getAttributeChangedMap());
                        break;
                    default:
                        Assertions.fail();
                        break;
                }
            }
        });
    }

    /**
     * 属性变化
     */
    @Test
    public void testCase04() {
        // 权重变化
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@100@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@100@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@50@0@0"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@50@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@100@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@100@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                if (changeEventEnum == SLAVE_WEIGHT_CHANGE) {
                    Assertions.assertNull(slaveResult.getOldSlaveSet());
                    Assertions.assertNull(slaveResult.getNewSlaveSet());
                    Assertions.assertEquals(
                            new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(2) {{
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@100@0@0"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@50@0@0"));
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@50@0@0"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                "3.3.3.3:3333@100@0@0"));
                            }}, slaveResult.getAttributeChangedMap());
                } else {
                    Assertions.fail();
                }
            }
        });

        // 延迟变化
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@100"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@100"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@50"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@50"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@100"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@100"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                if (changeEventEnum == SLAVE_DELAY_CHANGE) {
                    Assertions.assertNull(slaveResult.getOldSlaveSet());
                    Assertions.assertNull(slaveResult.getNewSlaveSet());
                    Assertions.assertEquals(
                            new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(2) {{
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@100"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@50"));
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@50"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                "3.3.3.3:3333@0@0@100"));
                            }}, slaveResult.getAttributeChangedMap());
                } else {
                    Assertions.fail();
                }
            }
        });

        // isWatch变化结果应为无变化
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@1@0"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@1@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            SlaveResult slaveResult = Arrays.asList(slaveResultSet.toArray(new SlaveResult[0])).get(0);

            Assertions.assertNotNull(slaveResult);
            Assertions.assertEquals(NO_CHANGE, slaveResult.getTdsqlDirectTopoChangeEventEnum());
            Assertions.assertNull(slaveResult.getOldSlaveSet());
            Assertions.assertNull(slaveResult.getNewSlaveSet());
            Assertions.assertNull(slaveResult.getAttributeChangedMap());
        });
    }

    /**
     * 仅权重变化+仅延迟变化+两者均变化
     */
    @Test
    public void testCase05() {
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@100@0@50"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@50"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@50@0@100"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@50@0@100"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@100@0@50"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@50@0@50"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(3, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                switch (changeEventEnum) {
                    case SLAVE_ALL_ATTR_CHANGE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertEquals(
                                new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                    put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "1.1.1.1:1111@100@0@50"),
                                            new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "1.1.1.1:1111@50@0@100"));
                                }}, slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_WEIGHT_CHANGE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertEquals(
                                new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                    put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@50"),
                                            new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "2.2.2.2:2222@100@0@50"));
                                }}, slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_DELAY_CHANGE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertEquals(
                                new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                    put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "3.3.3.3:3333@50@0@100"),
                                            new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "3.3.3.3:3333@50@0@50"));
                                }}, slaveResult.getAttributeChangedMap());
                        break;
                    default:
                        Assertions.fail();
                        break;
                }
            }
        });
    }

    @Test
    public void testCase06() {
        // 两者均变化
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@100@0@50"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@50@0@100"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                if (changeEventEnum == SLAVE_ALL_ATTR_CHANGE) {
                    Assertions.assertNull(slaveResult.getOldSlaveSet());
                    Assertions.assertNull(slaveResult.getNewSlaveSet());
                    Assertions.assertEquals(
                            new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@100@0@50"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                "1.1.1.1:1111@50@0@100"));
                            }}, slaveResult.getAttributeChangedMap());
                } else {
                    Assertions.fail();
                }
            }
        });

        // 权重变化
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@50"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@100@0@50"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                if (changeEventEnum == SLAVE_WEIGHT_CHANGE) {
                    Assertions.assertNull(slaveResult.getOldSlaveSet());
                    Assertions.assertNull(slaveResult.getNewSlaveSet());
                    Assertions.assertEquals(
                            new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@50"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                "2.2.2.2:2222@100@0@50"));
                            }}, slaveResult.getAttributeChangedMap());
                } else {
                    Assertions.fail();
                }
            }
        });

        // 延迟变化
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(3);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@100"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(3);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@0"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@50"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(1, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                if (changeEventEnum == SLAVE_DELAY_CHANGE) {
                    Assertions.assertNull(slaveResult.getOldSlaveSet());
                    Assertions.assertNull(slaveResult.getNewSlaveSet());
                    Assertions.assertEquals(
                            new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@0@0@100"),
                                        new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                "3.3.3.3:3333@0@0@50"));
                            }}, slaveResult.getAttributeChangedMap());
                } else {
                    Assertions.fail();
                }
            }
        });
    }

    /**
     * 上线+下线+仅权重变化+仅延迟变化+两者均变化
     */
    @Test
    public void testCase07() {
        Assertions.assertDoesNotThrow(() -> {
            Set<TdsqlDirectSlaveTopologyInfo> s1 = new LinkedHashSet<>(4);
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@100@0@50"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@50"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@50@0@100"));
            s1.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "4.4.4.4:4444@50@0@0"));

            Set<TdsqlDirectSlaveTopologyInfo> s2 = new LinkedHashSet<>(4);
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "1.1.1.1:1111@50@0@100"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@100@0@50"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "3.3.3.3:3333@50@0@50"));
            s2.add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "5.5.5.5:5555@100@0@0"));

            Set<SlaveResult> slaveResultSet = this.comparator.compareSlaves(s1, s2);

            Assertions.assertNotNull(slaveResultSet);
            Assertions.assertEquals(5, slaveResultSet.size());

            for (SlaveResult slaveResult : slaveResultSet) {
                Assertions.assertNotNull(slaveResult);

                TdsqlDirectTopologyChangeEventEnum changeEventEnum = slaveResult.getTdsqlDirectTopoChangeEventEnum();
                switch (changeEventEnum) {
                    case SLAVE_ALL_ATTR_CHANGE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertEquals(
                                new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                    put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "1.1.1.1:1111@100@0@50"),
                                            new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "1.1.1.1:1111@50@0@100"));
                                }}, slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_ONLINE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertEquals(new LinkedHashSet<TdsqlDirectSlaveTopologyInfo>(1) {{
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "5.5.5.5:5555@100@0@0"));
                        }}, slaveResult.getNewSlaveSet());
                        Assertions.assertNull(slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_OFFLINE:
                        Assertions.assertEquals(new LinkedHashSet<TdsqlDirectSlaveTopologyInfo>(1) {{
                            add(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "4.4.4.4:4444@50@0@0"));
                        }}, slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertNull(slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_WEIGHT_CHANGE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertEquals(
                                new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                    put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid, "2.2.2.2:2222@0@0@50"),
                                            new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "2.2.2.2:2222@100@0@50"));
                                }}, slaveResult.getAttributeChangedMap());
                        break;
                    case SLAVE_DELAY_CHANGE:
                        Assertions.assertNull(slaveResult.getOldSlaveSet());
                        Assertions.assertNull(slaveResult.getNewSlaveSet());
                        Assertions.assertEquals(
                                new LinkedHashMap<TdsqlDirectSlaveTopologyInfo, TdsqlDirectSlaveTopologyInfo>(1) {{
                                    put(new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "3.3.3.3:3333@50@0@100"),
                                            new TdsqlDirectSlaveTopologyInfo(defaultDataSourceUuid,
                                                    "3.3.3.3:3333@50@0@50"));
                                }}, slaveResult.getAttributeChangedMap());
                        break;
                    default:
                        Assertions.fail();
                        break;
                }
            }
        });
    }
}
