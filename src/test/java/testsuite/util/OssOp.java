package testsuite.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import testsuite.util.model.DbInfo;

import java.io.IOException;
import java.util.ArrayList;

public class OssOp {
    private static final Logger logger = LoggerFactory.getLogger(OssOp.class);
    private static final SshClient sshClient = SshClient.getDefaultHostSshClient();


    public InstanceInfo getInstanceStatus(String instanceId) throws IOException {

        String param = String.format("{\"instance\":[{\"id\":\"%s\"}]}", instanceId);
        JsonObject jsonOb = sendOssCurl("QueryGWInstance", param);
        JsonObject dbinfo = jsonOb.getAsJsonObject("returnData")
                .getAsJsonArray("instance")
                .get(0)
                .getAsJsonObject();
        JsonArray dbs = dbinfo
                .getAsJsonObject("tdsql")
                .getAsJsonArray("db");
        ArrayList<DbInfo> slaveMapList = new ArrayList<>();
//        proxy_host   available_proxy_host
        String[] proxyIpList = dbinfo.get("proxy_host").getAsString().split(";");
        int proxyPort = dbinfo.get("proxy_port").getAsInt();
        DbInfo masterDb = null;

        for (JsonElement db : dbs) {
            JsonObject jb = db.getAsJsonObject();

            int isMaster = jb.get("master").getAsInt();
            int delay_slave = jb.get("delay_slave").getAsInt();
            String ip = jb.get("ip").getAsString();
            int port = jb.get("port").getAsInt();
            logger.info(String.valueOf(jb));
            if (isMaster == 0) {
                logger.info("add slave ip " + ip);
                slaveMapList.add(new DbInfo(ip, port, delay_slave));
            } else {
                logger.info("master ip is:" + ip);
                masterDb = new DbInfo(ip, port, delay_slave);
            }
        }
        int[] proxyPortList = new int[proxyIpList.length];
        for (int i = 0; i < proxyPortList.length; i++) {
            proxyPortList[i] = proxyPort;
        }
        return new InstanceInfo(proxyIpList, proxyPortList, masterDb, slaveMapList);


    }

    protected JsonObject sendOssCurl(String interfaceName, String param) throws IOException {
        String curl_cmd = String.format( "curl http://9.30.0.210:8080/tdsql -d"
                + "'{\"version\":\"1.0\",\"caller\":\"DES\",\"password\":\"DES\",\"callee\":\"admin\"," +
                "\"eventId\":101,\"timestamp\":1680178535,\"interface\":" +
                "{\"interfaceName\":\"TDSQL.%s\",\"para\":%s}}'", interfaceName, param);
        String rs = sshClient.sendCmd(curl_cmd);
        System.out.println(rs);
        JsonObject jo = JsonParser.parseString(rs).getAsJsonObject();
        if (jo.get("returnValue").getAsInt() != 0) {
            logger.error("oss curl error: " + rs);
            throw new RuntimeException("oss curl error");
        }
        return jo;
    }

    public void setSlaveDelay(String instanceId, String ip, int delay) throws IOException {
        String param = String.format("{\"id\":\"%s\",\"groupid\":\"\",\"ip\":\"%s\",\"mode\":0,\"delay\":%d}", instanceId, ip, delay);
        JsonObject rs = sendOssCurl("SetSlaveDelay", param);
    }

    public void deleteSetSlaveDelay(String instanceId, String ip) throws IOException {
        String param = String.format("{\"id\":\"%s\",\"groupid\":\"\",\"ip\":\"%s\",\"mode\":1}", instanceId, ip);
        JsonObject rs = sendOssCurl("SetSlaveDelay", param);
    }

    public void switchMaster(String instanceId, String ip) throws IOException {
        String param = String.format("{\"id\":\"%s\",\"groupid\":\"\",\"slave\": \"%s\"}", instanceId, ip);
        JsonObject rs = sendOssCurl("ForceSwitchMaster", param);
    }

    public void DeactiveInstance(String instanceId) throws IOException {
        String param = "{\n" +
                "      \"instance\": [\n" +
                "        {\n" +
                "          \"id\": \"%s\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }";
        param = String.format(param, instanceId);
        JsonObject rs = sendOssCurl("DeactiveInstance", param);
    }

    public void ActiveInstance(String instanceId) throws IOException {
        String param = "{\n" +
                "      \"instance\": [\n" +
                "        {\n" +
                "          \"id\": \"%s\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }";
        param = String.format(param, instanceId);
        JsonObject rs = sendOssCurl("ActiveInstance", param);
    }

    public void DeleteHost(String instanceId,String ip) throws IOException {
        String param = "{\n" +
                "\t\t\t\"dsthost\": \"9.30.2.89\",\n" +
                "\t\t\t\"idc_name\": \"IDC_SZ_FT_001\",\n" +
                "\t\t\t\"set\": \"set_1680849332_9551140\",\n" +
                "\t\t\t\"groupid\": \"\",\n" +
                "\t\t\t\"sync_type\": 0,\n" +
                "\t\t\t\"release_flag\": 1\n" +
                "\t\t}";
        param = String.format(param, instanceId);
        JsonObject rs = sendOssCurl("DeleteHost", param);
    }
}
