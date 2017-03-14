package index;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by rembau on 2017/3/10.
 */
public class IndexManager {
    void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest("testIndex");
        TransportClient client = newInstance();

        client.admin().indices().create(request);

        PutMappingRequest mapping = Requests.putMappingRequest("testIndex").type("testType").source(getMapping());
    }

    public static XContentBuilder getMapping(){

        try {
            XContentBuilder builder= XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("testIndexName")
                    .startObject("properties")
                    .startObject("id").field("type", "integer").field("store", "yes").endObject()
                    .startObject("kw").field("type", "string").field("store", "yes").field("indexAnalyzer", "ik").field("searchAnalyzer", "ik").endObject()
                    .startObject("edate").field("type", "date").field("store", "yes").field("indexAnalyzer", "ik").field("searchAnalyzer", "ik").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject()
                    //开启倒计时功能
                    .startObject("_ttl")
                    .field("enabled",false)
                    .endObject()
                    .startObject("properties")
                    .startObject("title")
                    .field("type","string")
                    .endObject()
                    .startObject("question")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("answer")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("category")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("author")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("date")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("answer_author")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("answer_date")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("description")
                    .field("type","string")
                    .field("index","not_analyzed")
                    .endObject()
                    .startObject("keywords")
                    .field("type","string")
                    .field("index","not_analyzed")
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
