package parseDouguo;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import parseData.DubboDataUtil;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by rembau on 2017/3/9.
 */
public class FormatJson {

    @Test
    public void readBsonFile() {
        DubboDataUtil.readBsonFile(
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\douguoRecipeInfoCollection.bson",
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\douguoRecipeInfoCollection-lines.bson");
    }

    public TransportClient newInstance() {

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        try {
            return new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.83"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }
}
