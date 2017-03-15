package parseDubboMonitor;

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
        String source[] = new String[]{//"C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines.json",
        "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines.json.1",
        "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines.json.2"};
        DubboDataUtil.bulkIn(newInstance(), source,
                "dubbo_v2", "dubboMonitor", MonitorDataCollection.class, new DubboMonitorDataHandleData());
    }

    @Test
    public void addEsInfo() {
        DubboDataUtil.addEsInfo(
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines.json",
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines-es.json",
                MonitorDataCollection.class);
    }

    @Test
    public void readBsonFile() {
        DubboDataUtil.readBsonFile(
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14.bson",
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines.json");
    }

    @Test
    public void remoreKonggeHuanhang() {
        DubboDataUtil.remoreKonggeHuanhang(
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14.bson",
                "C:\\Users\\youyue\\Desktop\\数据\\dubboMonitor\\monitorDataCollection-2017-03-14-lines.json");
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
