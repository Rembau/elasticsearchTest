package index;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import parseData.GsonUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by rembau on 2017/3/10.
 */
public class IndexManager {
    @Test
    public void createIndex() {
        CreateIndexRequest request = new CreateIndexRequest("blog");
        TransportClient client = newInstance();

        CreateIndexResponse indexResponse = client.admin().indices().create(request).actionGet();
        //System.out.println(GsonUtil.gson.toJson(indexResponse));
//        XContentBuilder mappingBuilder = getMapping();
//        try {
//            System.out.println(mappingBuilder.string());
//        } catch (IOException e) {
//            e.printStackTrace();
//            return;
//        }
        PutMappingRequest mapping = Requests.putMappingRequest("blog").type("blog").source(testMapping());
        System.out.println("===================1s");
        PutMappingResponse mappingResponse = client.admin().indices().putMapping(mapping).actionGet();
        System.out.println("===================2");
        System.out.println(GsonUtil.gson.toJson(mappingResponse));

    }

    public static XContentBuilder getMapping(){

        /*try {
            XContentBuilder builder= XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("test_index")
                    .startObject("properties")
                    .startObject("id").field("type", "integer").field("store", "yes").endObject()
                    .startObject("kw").field("type", "string").field("store", "yes").field("indexAnalyzer", "ik").field("searchAnalyzer", "ik").endObject()
                    .startObject("edate").field("type", "date").field("store", "yes").field("indexAnalyzer", "ik").field("searchAnalyzer", "ik").endObject()
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }*/
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject()
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

    private String testMapping() {
        String mapping = "{\n" +
                "  \"blog\": {\n" +
                "    \"properties\": {\n" +
                "      \"title\": {\n" +
                "        \"type\": \"string\",\n" +
                "        \"index\": \"not_analyzed\",\n" +
                "        \"include_in_all\": false\n" +
                "      },\n" +
                "      \"posttime\": {\n" +
                "        \"type\": \"string\",\n" +
                "        \"index\": \"no\"\n" +
                "      },\n" +
                "      \"content\": {\n" +
                "        \"type\": \"string\",\n" +
                "        \"analyzer\": \"ik_smart\"\n" +
                "      },\n" +
                "      \"id\": {\n" +
                "        \"type\": \"long\"\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        return mapping;
    }

    public TransportClient newInstance() {

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        try {
            return new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.87"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }
}
