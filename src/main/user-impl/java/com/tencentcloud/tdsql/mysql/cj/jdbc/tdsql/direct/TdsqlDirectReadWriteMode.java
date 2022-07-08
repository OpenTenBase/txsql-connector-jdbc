package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.TdsqlDirectConst.TDSQL_DIRECT_READ_WRITE_MODE_RO;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public enum TdsqlDirectReadWriteMode {
    RW,
    RO;

    public static TdsqlDirectReadWriteMode convert(String readWriteModeStr) {
        if (TDSQL_DIRECT_READ_WRITE_MODE_RO.equalsIgnoreCase(readWriteModeStr)) {
            return RO;
        }
        return RW;
    }
}
