package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import java.util.Objects;

/**
 * <p>
 * 自定义节点信息类
 * count表示该节点被建立连接实例的数量
 * isMaster表示该节点是否为主库节点
 * </p>
 *
 * @author gyokumeixie@tencent.com
 */
public class NodeMsg implements Cloneable {

    private Long count;
    private Boolean isMaster;

    public NodeMsg() {
    }

    public NodeMsg(Long count, Boolean isMaster) {
        this.count = count;
        this.isMaster = isMaster;
    }

    public synchronized Long getCount() {
        return count;
    }

    public synchronized void setCount(Long count) {
        this.count = count;
    }

    public Boolean getIsMaster() {
        return isMaster;
    }

    @Override
    public NodeMsg clone() throws CloneNotSupportedException {
        return (NodeMsg) super.clone();
    }

    @Override
    public int hashCode() {
        return Objects.hash(count, isMaster);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof NodeMsg)) {
            return false;
        }
        NodeMsg nodeMsg = (NodeMsg) obj;
        return Objects.equals(getCount(), nodeMsg.getCount()) &&
                Objects.equals(getIsMaster(), nodeMsg.getIsMaster());
    }

    @Override
    public String toString() {
        return "NodeMsg{" +
                "count=" + count +
                ", isMaster=" + isMaster +
                '}';
    }
}
