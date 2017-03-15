import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;
import org.rembau.test.elasticsearch.ClientFactory;
import org.rembau.test.elasticsearch.commons.Constant;
import org.rembau.test.elasticsearch.tools.GsonUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestEsClient {
    private final static Logger logger = LogManager.getLogger(TestEsClient.class);

    @Test
    public void get() {

        try {
            //搜索数据
            GetResponse response = ClientFactory.newInstance().prepareGet(Constant.INDEX_NAME, Constant.INDEX_TYPE_COOKBOOK, "AVqTOigV0TWwRV-pz8SC").execute().actionGet();
            //输出结果
            logger.info("获取结果：" + response.getSourceAsString());
            //关闭client
            ClientFactory.newInstance().close();

        } catch (Exception e) {
            logger.error("", e);
        }
    }

    @Test
    public void query() {
        QueryBuilder queryBuilder = QueryBuilders.disMaxQuery();
                //.add(QueryBuilders.quer("catalogName", "家常菜"));
                //.add(QueryBuilders.prefixQuery("name", "一下"));
        logger.info("queryBuilder=========：{}" + queryBuilder.toString());
        SearchResponse response = ClientFactory.newInstance().prepareSearch(Constant.INDEX_NAME)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();

        logger.info("response==========：{}", response);
        for(SearchHit hit: response.getHits().getHits()) {
            logger.info("hit=============：{}", GsonUtil.toJson(hit));
        }
    }

    @Test
    public void matchQuery() {
        //如果是搜索标签中的不同属性，需要设置不同权值，可以query中Multi Field中的语法设置权值。例如，将 鞋 的权值设为5.
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().should(
                QueryBuilders
                .matchQuery("name", "萝卜").boost(3.0f)).should(QueryBuilders
                .matchQuery("desc", "萝卜"));

                                                            //"fields" : ["nike", "鞋^5"]
        System.out.println("===============" + queryBuilder);
        SearchResponse response = ClientFactory.newInstance().prepareSearch(Constant.INDEX_NAME)
                .setQuery(queryBuilder).setExplain(true)
                .execute().actionGet();

        logger.info("response==========：{}", response);
//        for(SearchHit hit: response.getHits().getHits()) {
//            logger.info("hit=============：{}", GsonUtil.toJson(hit));
//        }
    }

    @Test
    public void analyze() {
        IndicesAdminClient indicesAdminClient = ClientFactory.newInstance().admin().indices();
        AnalyzeRequestBuilder request = new AnalyzeRequestBuilder(indicesAdminClient, AnalyzeAction.INSTANCE,"youyue","我是中国人");
        request.setAnalyzer("ik_smart");    //ik_smart、ik_max_word
        //request.setTokenizer("ik_smart");
// Analyzer（分析器）、Tokenizer（分词器）
        List listAnalysis = request.execute().actionGet().getTokens();
        System.out.println(GsonUtil.toJson(listAnalysis));
// listAnalysis中的结果就是分词的结果
    }

    public void matchAllQuery() {
        QueryBuilder queryBuilder = QueryBuilders.matchAllQuery()
                .boost(11f);
    }

    public void update() {
        Map<String, Object> params = new HashMap<>();
        params.put("ntitle", "ElasticSearch Server Book");
        Script script = new Script("ctx._source.title = ntitle");
        UpdateResponse response = ClientFactory.newInstance().prepareUpdate("library", "book", "2")
                .setDoc("json")
                .execute().actionGet();
    }

}