package com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.util;

import com.tencentcloud.tdsql.mysql.cj.jdbc.tdsql.TdsqlLoggerFactory;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * <p>TDSQL专属，普通字符串转换为16进制32位MD5哈希字符串工具类</p>
 *
 * @author dorianzhang@tencent.com
 */
public class TdsqlMD5Util {

    /**
     * 普通字符串转换为16进制32位MD5哈希字符串
     *
     * @param source 待转换的普通字符串
     * @return 16进制32位MD5哈希字符串，如果捕获到{@link NoSuchAlgorithmException}异常，则返回待转换的普通字符串
     */
    public static String md5Hex(String source) {
        MessageDigest md5;
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            TdsqlLoggerFactory.logWarn(e.getMessage());
            return source;
        }

        byte[] md5Bytes = md5.digest(source.getBytes(StandardCharsets.UTF_8));

        StringBuilder hexValue = new StringBuilder();
        for (byte md5Byte : md5Bytes) {
            int val = ((int) md5Byte) & 0xff;
            if (val < 16) {
                hexValue.append("0");
            }
            hexValue.append(Integer.toHexString(val));
        }
        return hexValue.toString();
    }
}
