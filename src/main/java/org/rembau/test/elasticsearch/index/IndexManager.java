package org.rembau.test.elasticsearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.rembau.test.elasticsearch.ClientFactory;
import org.rembau.test.elasticsearch.tools.GsonUtil;
import org.springframework.stereotype.Service;

/**
 * Created by rembau on 2017/3/3.
 */

@Service
public class IndexManager {
    private final static Logger logger = LogManager.getLogger(IndexManager.class);

    public void create(String id, String json, String index, String type) {
        IndexResponse response = ClientFactory.newInstance().prepareIndex(index, type, id)
                .setSource(json)
                .get();

        logger.info("response：{}", GsonUtil.toJson(response));
    }

}
