package testsuite.util;

import testsuite.util.model.DbConfig;
import testsuite.util.model.DbInfo;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class InstanceInfo {
    private int[] proxyPortList;

    private String[] proxyIpList;

    private DbInfo masterDbinfo;
    private ArrayList<DbInfo> slaveMapList;

    public InstanceInfo(String[] proxyIpList, int[] proxyPortList, DbInfo masterDbinfo, ArrayList<DbInfo> slaveMapList) {
        this.proxyIpList = proxyIpList;
        this.masterDbinfo = masterDbinfo;
        this.slaveMapList = slaveMapList;
        this.proxyPortList = proxyPortList;
    }

    public InstanceInfo(String[] proxyIpList, int[] proxyPortList) {
        this.proxyIpList = proxyIpList;
        this.proxyPortList = proxyPortList;
    }

    public ArrayList<DbInfo> getSlaveMapList() {
        return slaveMapList;
    }
    public DbInfo getSlaveDbinfo(String ip) {
        for(DbInfo dbInfo : slaveMapList){
            if(dbInfo.getIp().equals(ip))
                return dbInfo;
        }
        return null;
    }

    public String[] getProxyIpList() {
        return proxyIpList;
    }
    public String getPorxyIpPort() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < proxyIpList.length; i++) {
            if (i != 0) {
                builder.append(",");
            }
            builder.append(proxyIpList[i] + ":" + proxyPortList[i]);
        }
        return builder.toString();
    }

    public DbInfo getMasterDbinfo() {
        return masterDbinfo;
    }

    public int getProxyPort(String ip) {
        for (int i = 0; i < proxyIpList.length; i++) {
            if (proxyIpList[i].equals(ip)) {
                return proxyPortList[i];
            }
        }
        return -1;
    }

    public int[] getProxyPortList() {
        return proxyPortList;
    }

    int getConNumberOnEacheNode(String ip, int port, String[] proxyList, DbConfig dbConfig) throws IOException {
        String localip = InetAddress.getLocalHost().getHostAddress();
        String result = SshClient.getDefaultHostSshClient().sendCmd(String.format("mysql -h%s  -P%d -uqt4s -p'g<m:7KNDF.L1<^1C' -e \"show processlist;\" |grep %s|grep %s|grep %s|grep -v \"%s\""
                , ip, port, dbConfig.getUser(), dbConfig.getDb_name(), localip, String.join("\\|", proxyList)));
        return (int) Arrays.stream(result.split("\n")).filter(s -> s.trim().length() > 0).count();
    }

    public int getConNumberOnEacheProxy(String ip, int port, String idcUser, String idcPasswd, String user, String passwd, String dbName) throws IOException {
        String localip = InetAddress.getLocalHost().getHostAddress();
        SshClient sshClient = new SshClient(ip,idcUser,idcPasswd);
        String result = sshClient.sendCmd(String.format("mysql -h%s  -P%d -u%s -p'%s' -e \"show processlist;\" |grep %s|grep %s|grep %s|grep %s"
                , ip, port, user, passwd, user, dbName, localip, ip));
        return (int) Arrays.stream(result.split("\n")).filter(s -> s.trim().length() > 0).count();
    }

    public Map<String, Integer> getConnectionNumber(DbConfig dbConfig) throws IOException {
        HashMap<String, Integer> connectMap = new HashMap<>();
        String ip = getMasterDbinfo().getIp();
        int port = getMasterDbinfo().getPort();
        String[] proxy = getProxyIpList();
        if (!dbConfig.ifIpOffline(ip))
            connectMap.put(ip, getConNumberOnEacheNode(ip, port, proxy, dbConfig));
        else
            connectMap.put(ip, 0);

        for (DbInfo slave : getSlaveMapList()) {
            ip = slave.getIp();
            port = slave.getPort();
            if (dbConfig.ifIpOffline(ip)){
                connectMap.put(ip, 0);
                continue;
            }
            connectMap.put(ip, getConNumberOnEacheNode(ip, port, proxy, dbConfig));
        }
        //logger.info("getConnectionNumber:" + connectMap);
        return connectMap;
    }
}