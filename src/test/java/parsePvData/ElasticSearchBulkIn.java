package parsePvData;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearchBulkIn {

    @Test
    public void bulkIn() {

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