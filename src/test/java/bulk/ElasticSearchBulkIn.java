package bulk;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.rembau.test.elasticsearch.tools.GsonUtil;
import parseDubboMonitor.MonitorDataCollection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchBulkIn {

    public static void main(String[] args) {

        try {

            TransportClient client = newInstance();

            File article = new File("C:\\Users\\youyue\\Desktop\\tem\\dubbo-monitor-lines-es.json");
//            File article = new File("C:\\Users\\youyue\\Desktop\\tem\\collections-anon.txt");
            FileReader fr=new FileReader(article);
            BufferedReader bfr=new BufferedReader(fr);
            String line=null;
            BulkRequestBuilder bulkRequest=client.prepareBulk();

            long start = System.currentTimeMillis();
            long _start = System.currentTimeMillis();
            int count=1;
            while((line=bfr.readLine())!=null) {
                if (count > 1000) {
                    count ++;
                    break;
                }
                if (line.startsWith("{ \"index\"")) {
                    continue;
                }


                MonitorDataCollection monitorDataCollection = GsonUtil.fromJson(line, MonitorDataCollection.class);
                IndexRequestBuilder requestBuilder = client.prepareIndex("test", "test", monitorDataCollection.get_id());
                monitorDataCollection.set_id(null);
                String toJson = GsonUtil.toJson(monitorDataCollection);

                bulkRequest.add(requestBuilder.setSource(toJson));

                if (count%1000==0) {
                    bulkRequest.execute().actionGet();

                    System.out.println("处理第 "+count+" 行完成，耗时：" + (System.currentTimeMillis() - start) + "--" + (System.currentTimeMillis() - _start));
                    _start = System.currentTimeMillis();

                }

                if (count % 10000 == 0) {
                    bulkRequest=client.prepareBulk();
                    System.out.println("初始化bulk");
                }
                count++;
            }
            bulkRequest.execute().actionGet();
            System.out.println("处理第 "+count+" 行完成，耗时：" + (System.currentTimeMillis() - start));
            bfr.close();
            fr.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static TransportClient newInstance() throws UnknownHostException {

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        return new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.83"), 9300));
    }
}