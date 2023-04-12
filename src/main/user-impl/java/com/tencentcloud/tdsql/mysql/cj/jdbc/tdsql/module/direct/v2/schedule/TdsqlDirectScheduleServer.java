package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.schedule;

import com.tencentcloud.tdsql.mysql.cj.Messages;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.AbstractTdsqlHostInfo;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlConnectionCounter;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlScheduleServer;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.exception.TdsqlExceptionFactory;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.TdsqlDirectReadWriteModeEnum;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.datasource.TdsqlDirectDataSourceConfig;
import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.module.direct.v2.exception.TdsqlDirectScheduleTopologyException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory.logInfo;

/**
 * <p>TDSQL专属，直连模式调度服务类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDirectScheduleServer implements
        TdsqlScheduleServer<TdsqlDirectConnectionCounter, TdsqlDirectHostInfo> {

    private final String dataSourceUuid;
    private final TdsqlDirectDataSourceConfig dataSourceConfig;
    private final ReentrantReadWriteLock rwLock;
    private TdsqlDirectConnectionCounter masterCounter;
    private final Set<TdsqlDirectConnectionCounter> slaveCounterSet;

    /**
     * 构造方法
     *
     * @param dataSourceConfig 数据源配置信息
     */
    public TdsqlDirectScheduleServer(TdsqlDirectDataSourceConfig dataSourceConfig) {
        this.dataSourceUuid = dataSourceConfig.getDataSourceUuid();
        this.dataSourceConfig = dataSourceConfig;
        this.rwLock = new ReentrantReadWriteLock();
        this.masterCounter = null;
        this.slaveCounterSet = new LinkedHashSet<>(8);
    }

    public ReentrantReadWriteLock.ReadLock getSchedualeReadLock() {
        return this.rwLock.readLock();
    }

    /**
     * 获取主库调度信息
     *
     * @return 主库连接计数器，继承自 {@link TdsqlConnectionCounter}
     */
    @Override
    public TdsqlDirectConnectionCounter getMaster() {
        this.rwLock.readLock().lock();
        try {
            return this.masterCounter;
        } finally {
            this.rwLock.readLock().unlock();
        }
    }

    /**
     * 新增主库调度信息
     *
     * @param master 主库的主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    @Override
    public void addMaster(TdsqlDirectHostInfo master) {
        // 校验主库主机信息
        this.validateNewMaster(master);

        this.rwLock.writeLock().lock();
        try {

            if (this.masterCounter != null) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.RepeatedAddMaster",
                                new Object[]{this.masterCounter.getTdsqlHostInfo(), master}));
            }

            this.masterCounter = new TdsqlDirectConnectionCounter(master);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /**
     * 更新主库调度信息
     *
     * @param oldMaster 已有主库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     * @param newMaster 待更新主库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    @Override
    public void updateMaster(TdsqlDirectHostInfo oldMaster, TdsqlDirectHostInfo newMaster) {
        // 校验主库主机信息
        this.validateNewMaster(newMaster);

        this.rwLock.writeLock().lock();
        try {

            if (this.masterCounter == null) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.EmptyOldMaster"));
            }

            if (oldMaster.equals(newMaster)) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.SameMaster", new Object[]{newMaster}));
            }

            if (!this.masterCounter.getTdsqlHostInfo().equals(oldMaster)) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.NotEqualsOldMaster",
                                new Object[]{this.masterCounter.getTdsqlHostInfo(), oldMaster}));
            }

            this.masterCounter = new TdsqlDirectConnectionCounter(newMaster);
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /**
     * 获取备库调度信息集合
     *
     * @return 备库连接计数器集合，继承自 {@link TdsqlConnectionCounter}
     */
    @Override
    public Set<TdsqlDirectConnectionCounter> getSlaveSet() {
        this.rwLock.readLock().lock();
        try {
            return Collections.unmodifiableSet(this.slaveCounterSet);
        } finally {
            this.rwLock.readLock().unlock();
        }
    }

    /**
     * 新增备库调度信息
     *
     * @param slave 备库的主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    @Override
    public void addSlave(TdsqlDirectHostInfo slave) {
        this.rwLock.writeLock().lock();
        try {
            Set<TdsqlDirectHostInfo> uniqueSet = this.slaveCounterSet.stream()
                    .map(TdsqlDirectConnectionCounter::getTdsqlHostInfo)
                    .collect(Collectors.toCollection(() -> new HashSet<>(this.slaveCounterSet.size())));
            if (uniqueSet.contains(slave)) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.RepeatedAddSlave",
                                new Object[]{slave}));
            }
            logInfo("Add new slave through addSlave, host:" + slave.getHostPortPair());
            this.slaveCounterSet.add(new TdsqlDirectConnectionCounter(slave));
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /**
     * 移除备库调度信息
     *
     * @param slave 备库的主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    @Override
    public void removeSlave(TdsqlDirectHostInfo slave) {
        this.rwLock.writeLock().lock();
        try {
            if (this.slaveCounterSet.removeIf(counter -> counter.getTdsqlHostInfo().equals(slave)))
                logInfo("remove slave successfully, host:" + slave.getHostPortPair());
            else {
                logInfo("remove slave failed, host:" + slave.getHostPortPair());
            }
        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /**
     * 更新备库调度信息
     *
     * @param oldSlave 已有备库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     * @param newSlave 待更新备库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    @Override
    public void updateSlave(TdsqlDirectHostInfo oldSlave, TdsqlDirectHostInfo newSlave) {
        this.rwLock.writeLock().lock();
        try {
            if (oldSlave.equals(newSlave)) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.SameSlave", new Object[]{newSlave}));
            }

            TdsqlDirectConnectionCounter toBeRemoved = null;
            for (TdsqlDirectConnectionCounter counter : this.slaveCounterSet) {
                if (counter.getTdsqlHostInfo().equals(oldSlave)) {
                    toBeRemoved = counter;
                    break;
                }
            }
//            if (toBeRemoved == null) {
//                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
//                        Messages.getString("TdsqlDirectScheduleTopologyException.NotEqualsOldSlave",
//                                new Object[]{oldSlave}));
//            }
            if (toBeRemoved != null) {
                logInfo("remove slave through updateSlave, host:" + oldSlave.getHostPortPair());
                this.slaveCounterSet.remove(toBeRemoved);
                logInfo("Add new slave through updateSlave, host:" + newSlave.getHostPortPair());
                this.slaveCounterSet.add(new TdsqlDirectConnectionCounter(newSlave, toBeRemoved.getCount()));
            } else {
                logInfo("Add new slave through updateSlave, host:" + newSlave.getHostPortPair());
                this.slaveCounterSet.add(new TdsqlDirectConnectionCounter(newSlave, new LongAdder()));
            }

        } finally {
            this.rwLock.writeLock().unlock();
        }
    }

    /**
     * 校验主库主机信息
     *
     * @param master 待校验的直连模式专属的主机信息
     */
    private void validateNewMaster(TdsqlDirectHostInfo master) {
        TdsqlDirectReadWriteModeEnum rwMode = this.dataSourceConfig.getTdsqlDirectReadWriteMode();
        if (master.isEmptyDirectHostInfo()) {
            if (TdsqlDirectReadWriteModeEnum.RW.equals(rwMode)) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.AddEmptyMasterInRwMode"));
            } else if (TdsqlDirectReadWriteModeEnum.RO.equals(rwMode)
                    && this.dataSourceConfig.getTdsqlDirectMasterCarryOptOfReadOnlyMode()) {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.AddEmptyMasterInRoModeUseMaster"));
            } else {
                throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                        Messages.getString("TdsqlDirectScheduleTopologyException.AddEmptyMaster"));
            }
        }

        if (!this.dataSourceUuid.equals(master.getDataSourceUuid())) {
            throw TdsqlExceptionFactory.createException(TdsqlDirectScheduleTopologyException.class,
                    Messages.getString("TdsqlDirectScheduleTopologyException.DifferentDataSourceUuid",
                            new Object[]{this.dataSourceUuid, master.getDataSourceUuid()}));
        }
    }
}
