package parsePvData;

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
    public void bulkIn() {
        DubboDataUtil.bulkIn(newInstance(),"C:\\Users\\youyue\\Desktop\\数据\\pv数据-lines-es.json",
                "pvdata_v1", "webPv", WebEventCollection.class);
    }

    @Test
    public void addEsInfo() {
        DubboDataUtil.addEsInfo(
                "C:\\Users\\youyue\\Desktop\\数据\\pv数据-lines.json",
                "C:\\Users\\youyue\\Desktop\\数据\\pv数据-lines-es.json",
                WebEventCollection.class);
    }

    @Test
    public void remoreKonggeHuanhang() {
        DubboDataUtil.remoreKonggeHuanhang(
                "C:\\Users\\youyue\\Desktop\\数据\\pv数据.json",
                "C:\\Users\\youyue\\Desktop\\数据\\pv数据-lines.json");
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
