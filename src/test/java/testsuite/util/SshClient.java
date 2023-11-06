package testsuite.util;

import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.channel.direct.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.schmizz.sshj.SSHClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class SshClient {
    private static final Logger logger = LoggerFactory.getLogger(SshClient.class);

    private final String host;
    private final String user;
    private final String pwd;

    public SshClient(String host, String user, String pwd){
        this.host=host;
        this.user=user;
        this.pwd=pwd;
    }

    public String sendCmd(String cmd,int timeout)
            throws IOException {
        logger.info(String.format("cmd>>> %s[%s]: %s",host,user,cmd));
        final SSHClient ssh = new SSHClient();
        ssh.loadKnownHosts();
        ssh.connect(this.host);
        Session session = null;

        try {
            ssh.authPassword(this.user,this.pwd);
            session = ssh.startSession();
            final Session.Command cmd_session = session.exec(cmd);
            cmd_session.join(timeout, TimeUnit.SECONDS);

            String result  = IOUtils.readFully(cmd_session.getInputStream()).toString();
            logger.info(String.format("result<<< %s[%s]:status:%d -> %s", host,user,cmd_session.getExitStatus(),result));
            return result;
        } finally {
            try {
                if (session != null) {
                    session.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            ssh.disconnect();
        }
    }
    public String sendCmd(String cmd) throws IOException {
        return sendCmd(cmd,10);
    }
    public static SshClient getHostSshClient(String ip){
        Properties p  = new Properties();
        try {
            String rootPath = SshClient.class.getResource("/").getPath();
            File file = new File(rootPath + "db.properties");
            FileInputStream fis = new FileInputStream(file);
            p.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String host_user = p.getProperty("host_user");
        String host_pwd = p.getProperty("host_pwd");
        return new SshClient(ip,host_user,host_pwd);
    }
    public static SshClient getDefaultHostSshClient(){
        Properties p  = new Properties();
        try {
            String rootPath = SshClient.class.getResource("/").getPath();
            File file = new File(rootPath + "db.properties");
            FileInputStream fis = new FileInputStream(file);
            p.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
        String host = p.getProperty("host");
        String host_user = p.getProperty("host_user");
        String host_pwd = p.getProperty("host_pwd");
        return new SshClient(host,host_user,host_pwd);
    }
}