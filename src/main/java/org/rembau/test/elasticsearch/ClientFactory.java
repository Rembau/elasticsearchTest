package org.rembau.test.elasticsearch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by rembau on 2017/3/3.
 */
public class ClientFactory {
    private final static Logger logger = LogManager.getLogger(ClientFactory.class);

    private static TransportClient client;

    public static synchronized TransportClient newInstance() {

        if (client != null) {
            return client;
        }

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        //创建client
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.87"), 9300));
        } catch (UnknownHostException e) {
            logger.error("", e);
        }
        return client;
    }

}
