package org.rembau.test.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.rembau.test.elasticsearch.ClientFactory;
import org.rembau.test.elasticsearch.tools.GsonUtil;
import org.springframework.stereotype.Service;

/**
 * Created by rembau on 2017/3/3.
 */

@Service
public class IndexManager {
    private final static Logger logger = LogManager.getLogger(IndexManager.class);

    public void create(String json, String id, String index, String type) {
        IndexResponse response = ClientFactory.newInstance().prepareIndex(index, type, id)
                .setSource(json)
                .get();

        logger.info("response：{}", GsonUtil.toJson(response));
    }

    public void update(String json, String id, String index, String type) {
        UpdateResponse response = ClientFactory.newInstance().prepareUpdate(index, type, id)
                .setDoc(json)
                .execute().actionGet();

        logger.info("response：{}", GsonUtil.toJson(response));
    }

    public void delete(String id, String index, String type) {
        DeleteResponse response = ClientFactory.newInstance().prepareDelete(index, type, id)
                .execute().actionGet();

        logger.info("response：{}", GsonUtil.toJson(response));
    }
}
