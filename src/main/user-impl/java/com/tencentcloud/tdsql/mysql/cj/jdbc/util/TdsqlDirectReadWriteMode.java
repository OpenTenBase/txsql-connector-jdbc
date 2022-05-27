package com.tencentcloud.tdsql.mysql.cj.jdbc.util;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlConst.TDSQL_READ_WRITE_MODE_RO;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlDirectReadWriteMode {
    RW,
    RO;

    public static TdsqlDirectReadWriteMode convert(String readWriteModeStr) {
        if (TDSQL_READ_WRITE_MODE_RO.equalsIgnoreCase(readWriteModeStr)) {
            return RO;
        }
        return RW;
    }
}
