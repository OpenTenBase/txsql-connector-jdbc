package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.cache;

/**
 * <p>TDSQL专属，直连模式拓扑信息变化枚举类</p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlDirectTopologyChangeEventEnum {
    /**
     * 第一次加载
     */
    FIRST_LOAD,

    /**
     * 主备切换
     */
    SWITCH,

    /**
     * 备库上线
     */
    SLAVE_ONLINE,

    /**
     * 备库下线
     */
    SLAVE_OFFLINE,

    /**
     * 备库权重变化
     */
    SLAVE_WEIGHT_CHANGE,

    /**
     * 备库延迟变化
     */
    SLAVE_DELAY_CHANGE,

    /**
     * 备库权重、延迟均变化
     */
    SLAVE_ALL_ATTR_CHANGE,

    /**
     * 无变化
     */
    NO_CHANGE
}
