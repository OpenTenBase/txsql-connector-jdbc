package testsuite.util.model;

public class DbInfo {
    private final int port;

    public int getConnectionNumber() {
        return ConnectionNumber;
    }

    public void setConnectionNumber(int connectionNumber) {
        ConnectionNumber = connectionNumber;
    }

    private int ConnectionNumber;

    private String[] proxyIpList;

    public DbInfo(String ip, int port, int delay) {
        this.delay = delay;
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public int getDelay() {
        return delay;
    }

    public int getPort() {
        return port;
    }

    private final String ip;
    private final int delay;
}