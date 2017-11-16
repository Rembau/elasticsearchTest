package bulk;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ElasticSearchBulkOut {

    public static void main(String[] args) throws IOException {
        TransportClient client = newInstance();

        String indexFrom = "mobile-sc_interface_call_v1-2017-08-29";
        int timeMillis = 60000;
        SearchResponse scrollResp = client.prepareSearch(indexFrom)
                .setScroll(new TimeValue(timeMillis))
                .setQuery(QueryBuilders.matchQuery("url.keyword","/service/recipe/searchRecipe")).setSize(1000).execute().actionGet();
// /service/app/v1/recipe/getRecipe
        // /service/recipe/getFavoriteRecipe
        // /service/recipe/searchRecipeByName
        // /service/userHistory/addHistory
        // /service/recipe/searchRecipe
        File article = new File("C:\\Users\\youyue\\Desktop\\testdata\\bulk4.txt");
        FileWriter fw = new FileWriter(article);
        BufferedWriter bfw = new BufferedWriter(fw);


        long start = System.currentTimeMillis();
        long _start = System.currentTimeMillis();
        long num = 0;
        while (true) {
            _start = System.currentTimeMillis();

            SearchHit[] hits = scrollResp.getHits().getHits();
            System.out.println(hits.length);
            if(hits.length > 0){
                num += hits.length;
                for (int i = 0; i < hits.length; i++) {
                    String jsonStr = hits[i]
                            .getSourceAsString();
                    System.out.println(jsonStr);
                    bfw.write(jsonStr);
                    bfw.write("\n");
                }

                System.out.println("处理完成 "+num+" ，耗时："+(System.currentTimeMillis() - start) +" || " + (System.currentTimeMillis() - _start));
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(timeMillis)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
        bfw.close();
        fw.close();
    }

    public static TransportClient newInstance() {

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        try {
            return new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("115.159.110.151"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

}