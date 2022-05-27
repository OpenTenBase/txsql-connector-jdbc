package com.tencentcloud.tdsql.mysql.cj.jdbc.cluster;

import java.util.List;

public class DataSetCluster {

    private String clusterName;
    private DataSetInfo master;
    private List<DataSetInfo> slaves;

    public DataSetCluster(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public DataSetInfo getMaster() {
        return master;
    }

    public void setMaster(DataSetInfo master) {
        this.master = master;
    }

    public List<DataSetInfo> getSlaves() {
        return slaves;
    }

    public void setSlaves(List<DataSetInfo> slaves) {
        this.slaves = slaves;
    }
}
