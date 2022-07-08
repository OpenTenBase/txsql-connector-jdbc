package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.direct.cluster;

import java.util.List;

public class TdsqlDataSetCluster {

    private String clusterName;
    private TdsqlDataSetInfo master;
    private List<TdsqlDataSetInfo> slaves;

    public TdsqlDataSetCluster(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public TdsqlDataSetInfo getMaster() {
        return master;
    }

    public void setMaster(TdsqlDataSetInfo master) {
        this.master = master;
    }

    public List<TdsqlDataSetInfo> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<TdsqlDataSetInfo> slaves) {
        this.slaves = slaves;
    }
}
