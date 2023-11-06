package testsuite.util;

import java.io.IOException;

public class InstanceOp {
    private String setId;
    InstanceInfo instanceStatus;

    public InstanceOp(String setId){
        this.setId = setId;
        try {
            instanceStatus= new OssOp().getInstanceStatus(setId);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public InstanceOp(InstanceInfo instanceStatus) {
        this.instanceStatus = instanceStatus;
    }
    public InstanceInfo refreshInstanceStatus() throws IOException {
        instanceStatus= new OssOp().getInstanceStatus(setId);
        return instanceStatus;
    }

    public Undo setProxyNetFailure(String ip) throws IOException {
        return setPortFailed(ip,instanceStatus.getProxyPort(ip));
    }

    public void recoverProxyNetFailure(String ip) throws IOException {
        recoverPortFailed(ip,instanceStatus.getProxyPort(ip));
    }


    public Undo setSlaveDelay(String ip, int delay) throws IOException {
        new OssOp().setSlaveDelay(setId, ip,delay);
        return ()-> {
            try {
                deleteSlaveDelay(ip);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }
    public void deleteSlaveDelay(String ip) throws IOException {
        new OssOp().deleteSetSlaveDelay(setId, ip);
    }

    public void switchMaster(String slaveIp) throws IOException {
        new OssOp().switchMaster(setId, slaveIp);
    }

    public Undo offline() throws IOException {
        new OssOp().DeactiveInstance(setId);
        return ()-> {
            try {
                online();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    public void online() throws IOException {
        new OssOp().ActiveInstance(setId);
    }

    public Undo setPortFailed(String ip, int port, String user, String passwd) throws IOException {
        SshClient sshClient = new SshClient(ip,user,passwd);
        sshClient.sendCmd(String.format("iptables -t filter -A INPUT -p tcp  --dport %d -j REJECT", port));
        return ()-> {
            try {
                recoverPortFailed(ip,port, user, passwd);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
    }

    public Undo setPortFailed(String ip,int port) throws IOException {
        SshClient sshClient=SshClient.getHostSshClient(ip);
        sshClient.sendCmd(String.format("iptables -t filter -A INPUT -p tcp  --dport %d -j REJECT", port));
        return ()-> {
            try {
                recoverPortFailed(ip,port);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };

    }

    public void recoverPortFailed(String ip, int port, String user, String passwd) throws IOException {
        SshClient sshClient = new SshClient(ip,user,passwd);
        while (sshClient.sendCmd(String.format("iptables -t filter -D INPUT -p tcp  --dport %d -j REJECT", port)).trim().length()>0);
    }
    public void recoverPortFailed(String ip,int port) throws IOException {
        SshClient sshClient=SshClient.getHostSshClient(ip);
        while (sshClient.sendCmd(String.format("iptables -t filter -D INPUT -p tcp  --dport %d -j REJECT", port)).trim().length()>0);
    }
    public Undo setSlavePortFailed(String slaveIp) throws IOException {
        int port = instanceStatus.getSlaveMapList().stream().filter(s ->s.getIp().equals(slaveIp)).findAny().get().getPort();
        return setPortFailed(slaveIp,port);

    }
}