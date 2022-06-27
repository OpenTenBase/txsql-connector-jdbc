package com.tencentcloud.tdsql.mysql.cj.jdbc.ha;

import com.tencentcloud.tdsql.mysql.cj.jdbc.TdsqlDirectConnectionManager;
import com.tencentcloud.tdsql.mysql.cj.jdbc.util.TdsqlDirectLoggerFactory;
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
        TdsqlDirectLoggerFactory.logDebug("Because current direct read write mode is: " + rwMode);
        switch (rwMode) {
            case RW:
                optOfReadWriteMode(switchMode, toCloseList);
                break;
            case RO:
                optOfReadOnlyMode(switchMode, toCloseList);
                break;
            default:
                TdsqlDirectLoggerFactory.logError("Unknown direct read write mode: " + rwMode + "! NOOP!");
                break;
        }
    }

    private static void optOfReadWriteMode(TdsqlDirectMasterSlaveSwitchMode switchMode, List<String> toCloseList) {
        TdsqlDirectLoggerFactory.logDebug("Because current switch mode is: " + switchMode);
        switch (switchMode) {
            case MASTER_SLAVE_SWITCH:
                optOfReadWriteModeInMasterSlaveSwitch(toCloseList);
                break;
            case SLAVE_ONLINE:
                optOfReadWriteModeInSlaveOnline();
                break;
            case SLAVE_OFFLINE:
                optOfReadWriteModeInSlaveOffline();
                break;
            default:
                TdsqlDirectLoggerFactory.logError("Unknown switch mode: " + switchMode + "! NOOP!");
                break;
        }
    }

    private static void optOfReadOnlyMode(TdsqlDirectMasterSlaveSwitchMode switchMode, List<String> toCloseList) {
        TdsqlDirectLoggerFactory.logDebug("Because current switch mode is: " + switchMode);
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
                TdsqlDirectLoggerFactory.logError("Unknown switch mode: " + switchMode + "! NOOP!");
                break;
        }
    }

    private static void optOfReadWriteModeInMasterSlaveSwitch(List<String> toCloseList) {
        TdsqlDirectLoggerFactory.logDebug("So we will close [" + toCloseList + "]'s connections!");
        TdsqlDirectConnectionManager.getInstance().close(toCloseList);
    }

    private static void optOfReadWriteModeInSlaveOnline() {
        // no-op
        TdsqlDirectLoggerFactory.logDebug("So NOOP!");
    }

    private static void optOfReadWriteModeInSlaveOffline() {
        // no-op
        TdsqlDirectLoggerFactory.logDebug("So NOOP!");
    }

    private static void optOfReadOnlyModeInMasterSlaveSwitch(List<String> toCloseList) {
        TdsqlDirectLoggerFactory.logDebug("So we will close [" + toCloseList + "]'s connections!");
        TdsqlDirectConnectionManager.getInstance().close(toCloseList);
    }

    private static void optOfReadOnlyModeInSlaveOnline() {
        // no-op
        TdsqlDirectLoggerFactory.logDebug("So NOOP!");
    }

    private static void optOfReadOnlyModeInSlaveOffline(List<String> toCloseList) {
        TdsqlDirectLoggerFactory.logDebug("So we will close [" + toCloseList + "]'s connections!");
        TdsqlDirectConnectionManager.getInstance().close(toCloseList);
    }
}
