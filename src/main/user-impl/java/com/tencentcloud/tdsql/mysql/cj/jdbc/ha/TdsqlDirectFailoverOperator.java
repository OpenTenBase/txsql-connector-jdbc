package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectMasterSlaveSwitchMode;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectReadWriteMode;
import java.util.List;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectFailoverOperator {

    private TdsqlDirectFailoverOperator() {
    }

    public static void subsequentOperation(TdsqlDirectReadWriteMode rwMode, TdsqlDirectMasterSlaveSwitchMode switchMode,
            List<String> toCloseList) {
        switch (rwMode) {
            case RW:
                optOfReadWriteMode(switchMode);
                break;
            case RO:
                optOfReadOnlyMode(switchMode, toCloseList);
                break;
            default:
                // no-op
                break;
        }
    }

    private static void optOfReadWriteMode(TdsqlDirectMasterSlaveSwitchMode switchMode) {
        switch (switchMode) {
            case MASTER_SLAVE_SWITCH:
                optOfReadWriteModeInMasterSlaveSwitch();
                break;
            case SLAVE_ONLINE:
            case SLAVE_OFFLINE:
                // no-op
            default:
                // no-op
                break;
        }
    }

    private static void optOfReadOnlyMode(TdsqlDirectMasterSlaveSwitchMode switchMode, List<String> toCloseList) {
        switch (switchMode) {
            case MASTER_SLAVE_SWITCH:
                optOfReadOnlyModeInMasterSlaveSwitch(toCloseList);
                break;
            case SLAVE_ONLINE:
                optOfReadOnlyModeInSlaveOnline();
                break;
            case SLAVE_OFFLINE:
                optOfReadOnlyModeInSlaveOffline(toCloseList);
                break;
            default:
                // no-op
                break;
        }
    }

    private static void optOfReadWriteModeInMasterSlaveSwitch() {
        TdsqlDirectConnectionManager.getInstance().closeAll();
    }

    private static void optOfReadOnlyModeInMasterSlaveSwitch(List<String> toCloseList) {
        TdsqlDirectConnectionManager.getInstance().close(toCloseList);
    }

    private static void optOfReadOnlyModeInSlaveOnline() {
        // no-op
    }

    private static void optOfReadOnlyModeInSlaveOffline(List<String> toCloseList) {
        TdsqlDirectConnectionManager.getInstance().close(toCloseList);
    }
}
