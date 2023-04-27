package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.AND_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.EQUAL_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.LEFT_CURLY_BRACES;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.PLUS_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.QUESTION_MARK;
import static com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util.TdsqlConst.RIGHT_CURLY_BRACES;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.HostInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.stream.Collectors;

/**
 * <p>TDSQL专属，数据源唯一ID生成器</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlDataSourceUuidGenerator {

    public static final String UNKNOWN_UUID = "UNKNOWN-UUID";

    /**
     * 通过{@link ConnectionUrl}生成数据源唯一ID，为16进制32位MD5哈希字符串
     *
     * @param connectionUrl {@link ConnectionUrl}
     * @return 数据源唯一ID，为16进制32位MD5哈希字符串
     */
    public static String generateUuid(ConnectionUrl connectionUrl) {
        List<HostInfo> proxyHostList = connectionUrl.getHostsList();
        List<String> hostPortPairList = proxyHostList.stream()
                .map(HostInfo::getHostPortPair)
                .sorted()
                .collect(Collectors.toCollection(() -> new ArrayList<>(proxyHostList.size())));

        // 拼装所有IP和PORT
        StringJoiner ipPortDbJoiner = new StringJoiner(PLUS_MARK);
        for (String hostPort : hostPortPairList) {
            ipPortDbJoiner.add(hostPort);
        }

        // 继续拼装数据库名称
        HostInfo info = proxyHostList.get(0);
        ipPortDbJoiner.add(info.getDatabase());

        // 继续拼装用户名和密码
        StringJoiner userPassJoiner = new StringJoiner(AND_MARK);
        userPassJoiner.add(info.getUser()).add(info.getPassword());

        // 继续拼装URL全部参数
        StringJoiner propJoiner = new StringJoiner(AND_MARK, LEFT_CURLY_BRACES, RIGHT_CURLY_BRACES);
        for (Entry<String, String> entry : info.getHostProperties().entrySet()) {
            propJoiner.add(entry.getKey() + EQUAL_MARK + entry.getValue());
        }

        // 最终的UUID，为16进制32位MD5哈希字符串
        return TdsqlMD5Util.md5Hex(ipPortDbJoiner + QUESTION_MARK + userPassJoiner + AND_MARK + propJoiner);
    }
}
