package org.rembau.test.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.rembau.test.elasticsearch.ClientFactory;
import org.rembau.test.elasticsearch.tools.GsonUtil;

/**
 * Created by rembau on 2017/3/3.
 */
public class IndexManagerTest {
    private final static Logger logger = LogManager.getLogger(IndexManagerTest.class);

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void create() throws Exception {
        String json = "{\"name\":\"linmao\", \"age\":\"25\"}";
        new IndexManager().create(json, "test", "test", "1");
    }

    @Test
    public void update() throws Exception {
        String json = "{\"name\":\"rembau\", \"age\":\"25\"}";
        new IndexManager().update(json, "test", "test", "1");
    }

    @Test
    public void delete() throws Exception {

    }


    @Test
    public void matchQuery() {
        QueryBuilder queryBuilder = QueryBuilders
                .matchQuery("name", "linmao");

        SearchResponse response = ClientFactory.newInstance().prepareSearch("test")
                .setQuery(queryBuilder)
                .execute().actionGet();

        logger.info("response==========：{}", response);
        for(SearchHit hit: response.getHits().getHits()) {
            logger.info("hit=============：{}", GsonUtil.toJson(hit));
        }
    }

}