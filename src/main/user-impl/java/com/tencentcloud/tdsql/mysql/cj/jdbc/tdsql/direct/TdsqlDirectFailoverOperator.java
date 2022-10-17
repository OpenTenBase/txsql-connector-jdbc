package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logError;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.multiDataSource.TdsqlDirectDataSourceCounter;
import java.util.List;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectFailoverOperator {

    private TdsqlDirectFailoverOperator() {
    }

    public static synchronized void subsequentOperation(TdsqlDirectReadWriteMode rwMode,
            TdsqlDirectMasterSlaveSwitchMode switchMode,
            List<String> toCloseList, String ownerUuid) {
        logInfo("[" + ownerUuid + "] Because current direct read write mode is: " + rwMode);
        switch (rwMode) {
            case RW:
                optOfReadWriteMode(switchMode, toCloseList, ownerUuid);
                break;
            case RO:
                optOfReadOnlyMode(switchMode, toCloseList, ownerUuid);
                break;
            default:
                logError("[" + ownerUuid + "] Unknown direct read write mode: " + rwMode + "! NOOP!");
                break;
        }
    }

    private static void optOfReadWriteMode(TdsqlDirectMasterSlaveSwitchMode switchMode, List<String> toCloseList,
            String ownerUuid) {
        logInfo("[" + ownerUuid + "] Because current switch mode is: " + switchMode);
        switch (switchMode) {
            case MASTER_SLAVE_SWITCH:
                optOfReadWriteModeInMasterSlaveSwitch(toCloseList, ownerUuid);
                break;
            case SLAVE_ONLINE:
                optOfReadWriteModeInSlaveOnline(ownerUuid);
                break;
            case SLAVE_OFFLINE:
                optOfReadWriteModeInSlaveOffline(ownerUuid);
                break;
            default:
                logError("[" + ownerUuid + "] Unknown switch mode: " + switchMode + "! NOOP!");
                break;
        }
    }

    private static void optOfReadOnlyMode(TdsqlDirectMasterSlaveSwitchMode switchMode, List<String> toCloseList,
            String ownerUuid) {
        logInfo("[" + ownerUuid + "] Because current switch mode is: " + switchMode);
        switch (switchMode) {
            case MASTER_SLAVE_SWITCH:
                optOfReadOnlyModeInMasterSlaveSwitch(toCloseList, ownerUuid);
                break;
            case SLAVE_ONLINE:
                optOfReadOnlyModeInSlaveOnline(ownerUuid);
                break;
            case SLAVE_OFFLINE:
                optOfReadOnlyModeInSlaveOffline(toCloseList, ownerUuid);
                break;
            default:
                logError("[" + ownerUuid + "] Unknown switch mode: " + switchMode + "! NOOP!");
                break;
        }
    }

    private static void optOfReadWriteModeInMasterSlaveSwitch(List<String> toCloseList, String ownerUuid) {
        logInfo("[" + ownerUuid + "] So we will close [" + toCloseList + "]'s connections!");
        TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(ownerUuid).getTdsqlDirectConnectionManager()
                .close(toCloseList);
    }

    private static void optOfReadWriteModeInSlaveOnline(String ownerUuid) {
        // no-op
        logInfo("[" + ownerUuid + "] So NOOP!");
    }

    private static void optOfReadWriteModeInSlaveOffline(String ownerUuid) {
        // no-op
        logInfo("[" + ownerUuid + "] So NOOP!");
    }

    private static void optOfReadOnlyModeInMasterSlaveSwitch(List<String> toCloseList, String ownerUuid) {
        logInfo("[" + ownerUuid + "] So we will close [" + toCloseList + "]'s connections!");
        TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(ownerUuid).getTdsqlDirectConnectionManager()
                .close(toCloseList);
    }

    private static void optOfReadOnlyModeInSlaveOnline(String ownerUuid) {
        // no-op
        logInfo("[" + ownerUuid + "] So NOOP!");
    }

    private static void optOfReadOnlyModeInSlaveOffline(List<String> toCloseList, String ownerUuid) {
        logInfo("[" + ownerUuid + "] So we will close [" + toCloseList + "]'s connections!");
        TdsqlDirectDataSourceCounter.getInstance().getTdsqlDirectInfo(ownerUuid).getTdsqlDirectConnectionManager()
                .close(toCloseList);
    }
}
