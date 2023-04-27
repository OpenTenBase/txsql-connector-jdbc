package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql;

import com.tencentcloud.tdsql.mysql.cj.conf.DatabaseUrlContainer;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.util.Map;

/**
 * <p>TDSQL专属，主机信息抽象类</p>
 *
 * @author dorianzhang@tencent.com
 */
public abstract class AbstractTdsqlHostInfo extends HostInfo {

    protected String dataSourceUuid;
    protected TdsqlConnectionModeEnum connectionMode;
    protected DatabaseUrlContainer originalUrl;
    protected String host;
    protected int port;
    protected String user;
    protected String password;
    protected Map<String, String> hostProperties;
    protected Integer weight;

    public AbstractTdsqlHostInfo() {
        super();
    }

    public AbstractTdsqlHostInfo(HostInfo hostInfo) {
        super(hostInfo.getOriginalUrl(), hostInfo.getHost(), hostInfo.getPort(), hostInfo.getUser(),
                hostInfo.getPassword(), hostInfo.getHostProperties());
    }

    public Integer getWeight() {
        return weight;
    }
}
