package parsePvData;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.rembau.test.elasticsearch.tools.GsonUtil;
import parseDubboMonitor.MonitorDataCollection;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchBulkIn {

    @Test
    public void bulkIn() {

        try {

            TransportClient client = newInstance();

            File article = new File("C:\\Users\\youyue\\Desktop\\数据\\pv数据-lines-es.json");
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

                MonitorDataCollection monitorDataCollection = GsonUtil.fromJson(line, MonitorDataCollection.class);
                IndexRequestBuilder requestBuilder = client.prepareIndex("test", "test", monitorDataCollection.get_id());
                monitorDataCollection.set_id(null);
                String toJson = GsonUtil.toJson(monitorDataCollection);

                bulkRequest.add(requestBuilder.setSource(toJson));

                if (count%2000==0) {
                    bulkRequest.execute().actionGet();

                    System.out.println("处理第 "+count+" 行完成，耗时：" + (System.currentTimeMillis() - start) + "--" + (System.currentTimeMillis() - _start));
                    _start = System.currentTimeMillis();
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

    @Test
    public void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest("pvData_v1");
        TransportClient client = newInstance();

        client.admin().indices().create(request);

        PutMappingRequest mapping = Requests.putMappingRequest("pvData_v1").type("webPv").source(getMapping());
        client.admin().indices().putMapping(mapping);
    }

    public XContentBuilder getMapping(){
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject()
                    //开启倒计时功能
//                    .startObject("_ttl")
//                    .field("enabled",false)
//                    .endObject()
                    .startObject("properties")
                    .startObject("eventType")
                    .field("type","string")
                    .endObject()
                    .startObject("userId")
                    .field("type","integer")
                    .endObject()
                    .startObject("account")
                    .field("type","string")
                    .endObject()
                    .startObject("url")
                    .field("type","string")
                    .endObject()
                    .startObject("openId")
                    .field("type","string")
                    .endObject()
                    .startObject("eventTime")
                    .field("type","date")
                    .endObject()
                    .startObject("answer_author")
                    .field("type","string")
                    .endObject()
                    .startObject("answer_date")
                    .field("type","string")
                    .endObject()
                    .startObject("description")
                    .field("type","string")
                    .endObject()
                    .startObject("keywords")
                    .field("type","string")
                    .endObject()
                    .startObject("read_count")
                    .field("type","integer")
                    .field("index","not_analyzed")
                    .endObject()
                    //关联数据
                    .startObject("list").field("type","object").endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return mapping;
    }
}