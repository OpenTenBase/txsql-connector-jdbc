package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.strategy.v2;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.EMPTY_STRING;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * <p>TDSQL专属，负载均衡策略算法枚举类</p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlLoadBalanceStrategyEnum {

    /**
     * <p>负载均衡策略算法：最小连接数（Least Connection）</p>
     * <p>选择连接计数的最小值，特点是数据库连接较均匀的分布在每个从库上。</p>
     * <p>
     * 在URL串上的配置为：<code>tdsqlLoadBalanceStrategy=lc</code>，key值<code>tdsqlLoadBalanceStrategy</code>区分大小写，
     * value值<code>lc</code>不区分大小写。
     * </p>
     * <p>
     * 允许在连接模式为负载均衡（Load Balance）和直连（Direct）时使用，且当连接模式为直连时，仅可以在读写分离模式为只读（Read-Only）时使用
     * </p>
     */
    LC("lc", new HashSet<TdsqlConnectionModeEnum>(2) {{
        add(TdsqlConnectionModeEnum.LOAD_BALANCE);
        add(TdsqlConnectionModeEnum.DIRECT);
    }}, new HashSet<TdsqlDirectReadWriteModeEnum>(1) {{
        add(TdsqlDirectReadWriteModeEnum.RO);
    }}),

    /**
     * <p>负载均衡策略算法：最短期望延迟（Shortest Excepted Delay）</p>
     * <p>特点是将权重纳入算法逻辑，同时结合连接计数，通过算法计算得到最终结果。</p>
     * <p>
     * 在URL串上的配置为：<code>tdsqlLoadBalanceStrategy=sed</code>，key值<code>tdsqlLoadBalanceStrategy</code>区分大小写，
     * value值<code>sed</code>不区分大小写。
     * </p>
     * <p>
     * 允许在连接模式为负载均衡（Load Balance）和直连（Direct）时使用，且当连接模式为直连时，仅可以在读写分离模式为只读（Read-Only）时使用
     * </p>
     */
    SED("sed", new HashSet<TdsqlConnectionModeEnum>(2) {{
        add(TdsqlConnectionModeEnum.LOAD_BALANCE);
        add(TdsqlConnectionModeEnum.DIRECT);
    }}, new HashSet<TdsqlDirectReadWriteModeEnum>(1) {{
        add(TdsqlDirectReadWriteModeEnum.RO);
    }}),

    /**
     * <p>未知的负载均衡策略算法</p>
     */
    UNKNOWN("unknown", Collections.EMPTY_SET, Collections.EMPTY_SET);

    /**
     * <p>记录所有可用的负载均衡策略算法的名称和其枚举类的对应关系，方便判断URL串配置的值是否有效。</p>
     * <p>注意：当增加或删除负载均衡策略算法时，记得修改<code>HashMap</code>的初始化容量。</p>
     */
    private static final Map<String, TdsqlLoadBalanceStrategyEnum> strategyNameMap = new HashMap<>(2);

    /**
     * <p>记录所有可用的负载均衡策略算法的名称和其允许被使用的连接模式枚举类的对应关系，方便判断URL串配置的值是否有效。</p>
     * <p>注意：当增加或删除负载均衡策略算法时，记得修改<code>HashMap</code>的初始化容量。</p>
     */
    private static final Map<String, Set<TdsqlConnectionModeEnum>> connectionModeMap = new HashMap<>(2);

    /**
     * <p>记录所有可用的负载均衡策略算法的名称和其在直连模式下允许被使用的读写分离模式枚举类的对应关系，方便判断URL串配置的值是否有效。</p>
     * <p>注意：当增加或删除负载均衡策略算法时，记得修改<code>HashMap</code>的初始化容量。</p>
     */
    private static final Map<String, Set<TdsqlDirectReadWriteModeEnum>> rwModeMap = new HashMap<>(2);

    static {
        for (TdsqlLoadBalanceStrategyEnum strategyEnum : values()) {
            String name = strategyEnum.strategyName;
            if (!UNKNOWN.strategyName.equalsIgnoreCase(name)) {
                strategyNameMap.put(name, strategyEnum);
                connectionModeMap.put(name, strategyEnum.acceptConnectionMode);
                rwModeMap.put(name, strategyEnum.acceptDirectReadWriteMode);
            }
        }
    }

    private final String strategyName;
    private final Set<TdsqlConnectionModeEnum> acceptConnectionMode;
    private final Set<TdsqlDirectReadWriteModeEnum> acceptDirectReadWriteMode;

    TdsqlLoadBalanceStrategyEnum(String strategyName, Set<TdsqlConnectionModeEnum> acceptConnectionMode,
            Set<TdsqlDirectReadWriteModeEnum> acceptDirectReadWriteMode) {
        this.strategyName = strategyName;
        this.acceptConnectionMode = acceptConnectionMode;
        this.acceptDirectReadWriteMode = acceptDirectReadWriteMode;
    }

    public String getStrategyName() {
        return strategyName;
    }

    /**
     * <p>判断当所处某个连接模式下时，某种负载均衡策略算法是否被允许使用。</p>
     *
     * @param connectionMode {@link TdsqlConnectionModeEnum} 当前所处的连接模式
     * @param strategyName 需要判断是否被允许使用的负载均衡策略算法的名称
     * @return 自定义返回类型 {@link IsAllowedStrategyReturned}，其中<code>isAllowed</code>代表是否允许，
     *         <code>errorMessage</code>代表不允许时的原因说明
     */
    public static IsAllowedStrategyReturned isAllowedStrategy(TdsqlConnectionModeEnum connectionMode,
            String strategyName) {

        // 通用的前置检查
        IsAllowedStrategyReturned preCheckReturned = preCheck(connectionMode, strategyName);
        if (!preCheckReturned.isAllowed) {
            return preCheckReturned;
        }
        // 再结合当前连接模式，判断负载均衡策略算法是否被允许使用
        if (!connectionModeMap.containsKey(strategyName) || !connectionModeMap.get(strategyName)
                .contains(connectionMode)) {
            return new IsAllowedStrategyReturned(false, UNKNOWN,
                    Messages.getString("ConnectionProperties.notAllowedStrategyInConnectionMode",
                            new Object[]{strategyName, connectionMode.name()}));
        }
        return new IsAllowedStrategyReturned(true, strategyNameMap.get(strategyName), EMPTY_STRING);
    }

    /**
     * <p>判断当处于直连模式的某种读写分离模式下时，某种负载均衡策略算法是否被允许使用。</p>
     *
     * @param rwModeEnum {@link TdsqlDirectReadWriteModeEnum} 当前所处的直连模式的某种读写分离模式
     * @param strategyName 需要判断是否被允许使用的负载均衡策略算法的名称
     * @return 自定义返回类型 {@link IsAllowedStrategyReturned}，其中<code>isAllowed</code>代表是否允许，
     *         <code>errorMessage</code>代表不允许时的原因说明
     */
    public static IsAllowedStrategyReturned isAllowedStrategy(TdsqlDirectReadWriteModeEnum rwModeEnum,
            String strategyName) {

        // 通用的前置检查
        IsAllowedStrategyReturned preCheckReturned = preCheck(TdsqlConnectionModeEnum.DIRECT, strategyName);
        if (!preCheckReturned.isAllowed) {
            return preCheckReturned;
        }
        // 再结合读写分离模式，判断负载均衡策略算法是否被允许使用
        if (!rwModeMap.containsKey(strategyName) || !rwModeMap.get(strategyName).contains(rwModeEnum)) {
            return new IsAllowedStrategyReturned(false, UNKNOWN,
                    Messages.getString("ConnectionProperties.invalidTdsqlLoadBalanceStrategyInRwMode",
                            new Object[]{strategyName, rwModeEnum.getRwModeName()}));
        }
        return new IsAllowedStrategyReturned(true, strategyNameMap.get(strategyName), EMPTY_STRING);
    }

    /**
     * <p>通用的前置检查</p>
     *
     * @param connectionMode {@link TdsqlConnectionModeEnum} 当前所处的连接模式
     * @param strategyName 需要判断是否被允许使用的负载均衡策略算法的名称
     * @return 自定义返回类型
     *         {@link IsAllowedStrategyReturned}，其中<code>isAllowed</code>代表是否允许，<code>errorMessage</code>代表不允许时的原因说明
     */
    private static IsAllowedStrategyReturned preCheck(TdsqlConnectionModeEnum connectionMode, String strategyName) {
        // 负载均衡策略算法名称的非空判断
        if (strategyName == null || EMPTY_STRING.equals(strategyName.trim())) {
            return new IsAllowedStrategyReturned(false, UNKNOWN,
                    Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceStrategy",
                            new Object[]{connectionMode.getModeName(), strategyName}));
        }
        // 根据负载均衡策略算法名称判断是否有效
        if (!strategyNameMap.containsKey(strategyName)) {
            return new IsAllowedStrategyReturned(false, UNKNOWN,
                    Messages.getString("ConnectionProperties.badValueForTdsqlLoadBalanceStrategy",
                            new Object[]{connectionMode.getModeName(), strategyName}));
        }
        return new IsAllowedStrategyReturned(true, strategyNameMap.get(strategyName), EMPTY_STRING);
    }

    /**
     * <p><code>isAllowedStrategy</code>方法的自定义返回类型。</p>
     * <p><code>isAllowed</code>代表是否允许</p>
     * <p><code>strategyEnum</code>代表负载均衡策略算法枚举类实例</p>
     * <p><code>errorMessage</code>代表不允许时的原因说明</p>
     */
    public static class IsAllowedStrategyReturned {

        private final boolean isAllowed;
        private final TdsqlLoadBalanceStrategyEnum strategyEnum;
        private final String errorMessage;

        public IsAllowedStrategyReturned(boolean isAllowed, TdsqlLoadBalanceStrategyEnum strategyEnum,
                String errorMessage) {
            this.isAllowed = isAllowed;
            this.strategyEnum = strategyEnum;
            this.errorMessage = errorMessage;
        }

        public boolean isAllowed() {
            return isAllowed;
        }

        public TdsqlLoadBalanceStrategyEnum getStrategyEnum() {
            return strategyEnum;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }
}
