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

import java.util.ArrayList;
import java.util.List;

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
        List<String> list = new ArrayList<String>();
        String data1 = parseData.GsonUtil.toJson(new blog(1, "git简介", "2016-06-19", "SVN与Git最主要的区别..."));
        String data2 = parseData.GsonUtil.toJson(new blog(2, "Java中泛型的介绍与简单使用", "2016-06-19", "学习目标 掌握泛型的产生意义..."));
        String data3 = parseData.GsonUtil.toJson(new blog(3, "SQL基本操作", "2016-06-19", "基本操作：CRUD ..."));
        String data4 = parseData.GsonUtil.toJson(new blog(4, "Hibernate框架基础", "2016-06-19", "Hibernate框架基础..."));
        String data5 = parseData.GsonUtil.toJson(new blog(5, "Shell基本知识", "2016-06-19", "Shell是什么..."));
        list.add(data1);
        list.add(data2);
        list.add(data3);
        list.add(data4);
        list.add(data5);

        int i = 1;
        for (String json : list) {
            new IndexManager().create(json, i+"", "blog", "blog");
            i++;
        }
    }
    class blog{
        private Integer id;
        private String title;
        private String posttime;
        private String content;

        public blog(Integer id, String title, String posttime, String content) {
            this.id = id;
            this.title = title;
            this.posttime = posttime;
            this.content = content;
        }
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