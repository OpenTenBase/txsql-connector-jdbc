package testsuite.util;

import testsuite.util.model.DbConfig;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class LocalCommandExecutor {
    /**
     * 执行本地命令并返回输出结果
     *
     * @param command 命令字符串
     * @return 命令输出结果字符串
     */

    public static String executeCommand(String command) {
        DbConfig.logger.info("local cmd:>>> " + command);
        try {
            // 构建命令
            Process process = new ProcessBuilder("sh", "-c", command).start();

            // 读取命令输出
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            StringBuilder output = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }

            // 等待命令执行完成
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Command exited with code " + exitCode);
            }
            String result = output.toString();
            DbConfig.logger.info("local cmd result: <<< "+result);

            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        String output = executeCommand("netstat -an |grep 15014|grep  ESTABLISHED |wc -l");
        System.out.println(output);
    }
}