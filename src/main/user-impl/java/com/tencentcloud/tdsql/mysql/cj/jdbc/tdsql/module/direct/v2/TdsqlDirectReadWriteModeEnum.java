package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.EMPTY_STRING;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>TDSQL专属，直连模式，读写分离类型定义枚举类</p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlDirectReadWriteModeEnum {

    /**
     * 读写模式
     */
    RW("rw"),

    /**
     * 只读模式
     */
    RO("ro"),

    /**
     * 未知模式
     */
    UNKNOWN("unknown");

    /**
     * <p>记录所有可用的读写分离模式的名称和模式枚举类的映射关系，方便判断URL串配置的值是否有效。</p>
     * <p>注意：当增加或删除读写分离模式时，记得修改<code>HashMap</code>的初始化容量。</p>
     */
    private static final Map<String, TdsqlDirectReadWriteModeEnum> rwModeEnumMap = new HashMap<>(2);

    static {
        for (TdsqlDirectReadWriteModeEnum rwModeEnum : values()) {
            if (!UNKNOWN.equals(rwModeEnum)) {
                rwModeEnumMap.put(rwModeEnum.rwModeName, rwModeEnum);
            }
        }
    }

    private final String rwModeName;

    TdsqlDirectReadWriteModeEnum(String rwModeName) {
        this.rwModeName = rwModeName;
    }

    public String getRwModeName() {
        return rwModeName;
    }

    /**
     * <p>判断当前读写分离模式是否有效。</p>
     *
     * @param rwModeName 需要判断是否有效的读写分离模式的名称
     * @return 自定义返回类型 {@link IsValidRwModeReturned}，其中<code>isValid</code>代表是否有效，
     *         <code>errorMessage</code>代表无效时的原因说明
     */
    public static IsValidRwModeReturned isValidRwMode(String rwModeName) {
        // 读写分离模式名称的非空判断
        if (rwModeName == null || EMPTY_STRING.equals(rwModeName.trim())) {
            return new IsValidRwModeReturned(false, UNKNOWN,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                            new Object[]{rwModeName}));
        }
        // 根据读写分离模式的名称匹配是否有效
        if (!rwModeEnumMap.containsKey(rwModeName)) {
            return new IsValidRwModeReturned(false, UNKNOWN,
                    Messages.getString("ConnectionProperties.badValueForTdsqlDirectReadWriteMode",
                            new Object[]{rwModeName}));
        }
        return new IsValidRwModeReturned(true, rwModeEnumMap.get(rwModeName), EMPTY_STRING);
    }

    @Deprecated
    public static TdsqlDirectReadWriteModeEnum convert(String modeNameStr) {
        if (TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(modeNameStr)) {
            return RO;
        }
        return RW;
    }

    /**
     * <p><code>isValidRwModeReturned</code>方法的自定义返回类型。</p>
     * <p><code>isValid</code>代表是否有效</p>
     * <p><code>rwModeEnum</code>代表读写分离模式枚举类实例</p>
     * <p><code>errorMessage</code>代表无效时的原因说明</p>
     */
    public static class IsValidRwModeReturned {

        private final boolean isValid;
        private final TdsqlDirectReadWriteModeEnum rwModeEnum;
        private final String errorMessage;

        public IsValidRwModeReturned(boolean isValid, TdsqlDirectReadWriteModeEnum rwModeEnum, String errorMessage) {
            this.isValid = isValid;
            this.rwModeEnum = rwModeEnum;
            this.errorMessage = errorMessage;
        }

        public boolean isValid() {
            return isValid;
        }

        public TdsqlDirectReadWriteModeEnum getRwModeEnum() {
            return rwModeEnum;
        }

        public String getErrorMessage() {
            return errorMessage;
        }
    }
}
