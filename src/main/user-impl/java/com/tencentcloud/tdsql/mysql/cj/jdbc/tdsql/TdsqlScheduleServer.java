package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import java.util.Set;

/**
 * <p>TDSQL专属，直连模式调度服务接口类</p>
 *
 * @author dorianzhang@tencent.com
 */
public interface TdsqlScheduleServer<T extends TdsqlConnectionCounter, E extends AbstractTdsqlHostInfo> {

    /**
     * 获取主库调度信息
     *
     * @return 主库连接计数器，继承自 {@link TdsqlConnectionCounter}
     */
    T getMaster();

    /**
     * 新增主库调度信息
     *
     * @param master 主库的主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    void addMaster(E master);

    /**
     * 更新主库调度信息
     *
     * @param oldMaster 已有主库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     * @param newMaster 待更新主库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    void updateMaster(E oldMaster, E newMaster);

    /**
     * 获取备库调度信息集合
     *
     * @return 备库连接计数器集合，继承自 {@link TdsqlConnectionCounter}
     */
    Set<T> getSlaveSet();

    /**
     * 新增备库调度信息
     *
     * @param slave 备库的主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    void addSlave(E slave);

    /**
     * 移除备库调度信息
     *
     * @param slave 备库的主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    void removeSlave(E slave);

    /**
     * 更新备库调度信息
     *
     * @param oldSlave 已有备库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     * @param newSlave 待更新备库主机信息，继承自 {@link AbstractTdsqlHostInfo}
     */
    void updateSlave(E oldSlave, E newSlave);
}
