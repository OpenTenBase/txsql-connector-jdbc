package com.tencentcloud.tdsql.mysql.cj.conf.url;

import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrl;
import com.tencentcloud.tdsql.mysql.cj.conf.ConnectionUrlParser;
import java.util.Properties;

/**
 * <p></p>
 *
 * @author dorianzhang@tencent.com
 */
public class DirectConnectionUrl extends ConnectionUrl {

    /**
     * Constructs an instance of {@link ConnectionUrl}, performing all the required initializations.
     *
     * @param connStrParser a {@link ConnectionUrlParser} instance containing the parsed version of the original
     *         connection string
     * @param info the connection arguments map
     */
    public DirectConnectionUrl(ConnectionUrlParser connStrParser, Properties info) {
        super(connStrParser, info);
        this.type = Type.DIRECT_CONNECTION;
    }
}
