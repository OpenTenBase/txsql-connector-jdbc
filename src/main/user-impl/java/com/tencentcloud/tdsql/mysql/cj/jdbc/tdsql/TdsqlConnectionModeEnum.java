package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

/**
 * <p>TDSQL专属，数据库连接模式枚举类</p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlConnectionModeEnum {

    /**
     * 负载均衡模式
     */
    LOAD_BALANCE("Load Balance"),

    /**
     * 直连模式
     */
    DIRECT("Direct"),

    /**
     * 未知模式
     */
    UNKNOWN("Unknown");

    private final String modeName;

    TdsqlConnectionModeEnum(String modeName) {
        this.modeName = modeName;
    }

    public String getModeName() {
        return modeName;
    }
}
