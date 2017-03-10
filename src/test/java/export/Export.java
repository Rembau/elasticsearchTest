package export;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by rembau on 2017/3/10.
 */
public class Export {
    public void handle() throws UnknownHostException {
        TransportClient client = newInstance();

        String indexFrom = "dubbo";
        String indexTo = "dubbo_new";
        int timeMillis = 60000;
        SearchResponse scrollResp = client.prepareSearch(indexFrom)
                .setScroll(new TimeValue(timeMillis))
                .setQuery(QueryBuilders.matchAllQuery()).setSize(10000).execute().actionGet();
        long start = System.currentTimeMillis();
        long _start = System.currentTimeMillis();
        long num = 0;
        while (true) {
            _start = System.currentTimeMillis();
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            SearchHit[] hits = scrollResp.getHits().getHits();
            System.out.println(hits.length);
            if(hits.length > 0){
                num += hits.length;
                for (SearchHit searchHit : hits) {
                    bulkRequest.add(client.prepareIndex(indexTo, searchHit.getType(),searchHit.getId())
                            .setSource(searchHit.getSource()));
                }
                bulkRequest.execute().actionGet();

                System.out.println("处理完成 "+num+" ，耗时："+(System.currentTimeMillis() - start) +" || " + (System.currentTimeMillis() - _start));
            }
            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId())
                    .setScroll(new TimeValue(timeMillis)).execute().actionGet();
            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }
    }

    public TransportClient newInstance() throws UnknownHostException {

        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        return new PreBuiltTransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("192.168.2.83"), 9300));
    }

    public static void main(String args[]) {
        try {
            new Export().handle();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }
}
