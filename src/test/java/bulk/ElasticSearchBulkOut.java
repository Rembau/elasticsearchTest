package bulk;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchBulkOut {

    public static void main(String[] args) {

        try {
            TransportClient client = newInstance();

            QueryBuilder qb = QueryBuilders.matchAllQuery();
            SearchResponse response = client.prepareSearch("blog")
                    .setTypes("article").setQuery(QueryBuilders.matchAllQuery())
                    .execute().actionGet();
            SearchHits resultHits = response.getHits();

            File article = new File("files/bulk.txt");
            FileWriter fw = new FileWriter(article);
            BufferedWriter bfw = new BufferedWriter(fw);

            if (resultHits.getHits().length == 0) {
                System.out.println("查到0条数据!");

            } else {
                for (int i = 0; i < resultHits.getHits().length; i++) {
                    String jsonStr = resultHits.getHits()[i]
                            .getSourceAsString();
                    System.out.println(jsonStr);
                    bfw.write(jsonStr);
                    bfw.write("\n");
                }
            }
            bfw.close();
            fw.close();

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