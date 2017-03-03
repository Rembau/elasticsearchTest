package org.rembau.test.elasticsearch.data;

import com.unilife.media.model.mongo.douguo.DouguoRecipeInfoCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rembau.test.elasticsearch.commons.Constant;
import org.rembau.test.elasticsearch.index.IndexManager;
import org.rembau.test.elasticsearch.mongo.MongoBaseService;
import org.rembau.test.elasticsearch.tools.GsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by rembau on 2017/3/3.
 */
@Service
public class CookbookData extends MongoBaseService {
    private final static Logger logger = LogManager.getLogger(CookbookData.class);

    @Autowired
    private IndexManager indexManager;

    public void add() {
        Map<String, Object> map = new HashMap();
        DouguoRecipeInfoCollection douguoRecipeInfoCollection = getOne(map, DouguoRecipeInfoCollection.class);
        indexManager.create(GsonUtil.toJson(douguoRecipeInfoCollection), douguoRecipeInfoCollection.getRecipeId(), Constant.INDEX_NAME, Constant.INDEX_TYPE_COOKBOOK);
    }

    @PostConstruct
    public void addAll() {
        Query q = new Query();

        int pageSize = 1000;
        int offset = 0;
        long count = count(q, DouguoRecipeInfoCollection.class);

        logger.info("共查询到{}个食谱", count);

        long startTime = System.currentTimeMillis();
        while (offset < count) {
            long onceStartTime = System.currentTimeMillis();
            logger.info("开始处理食谱{} -- {}", offset, offset+pageSize);
            List<DouguoRecipeInfoCollection> collections = list(q, DouguoRecipeInfoCollection.class, "DouguoRecipeInfoCollection", offset, pageSize);

            logger.info("查询到食谱数量：{}个", collections.size());
            for (int i = 0; i < collections.size(); i++) {
                DouguoRecipeInfoCollection douguoRecipeInfoCollection = collections.get(i);
                indexManager.create(GsonUtil.toJson(douguoRecipeInfoCollection), douguoRecipeInfoCollection.getRecipeId(), Constant.INDEX_NAME, Constant.INDEX_TYPE_COOKBOOK);
            }
            logger.info("处理完成：{}个，耗时：{}ms", collections.size(), System.currentTimeMillis() - onceStartTime);
            offset += pageSize;
        }
        logger.info("处理完成，总共耗时：{}ms", System.currentTimeMillis() - startTime);
    }

    public void delete() {
        Query q = new Query();

        int pageSize = 1000;
        int offset = 0;
        long count = count(q, DouguoRecipeInfoCollection.class);

        logger.info("共查询到{}个食谱", count);

        long startTime = System.currentTimeMillis();
        while (offset < count) {
            long onceStartTime = System.currentTimeMillis();
            logger.info("开始处理食谱{} -- {}", offset, offset+pageSize);
            List<DouguoRecipeInfoCollection> collections = list(q, DouguoRecipeInfoCollection.class, "DouguoRecipeInfoCollection", offset, pageSize);

            logger.info("查询到食谱数量：{}个", collections.size());
            for (int i = 0; i < collections.size(); i++) {
                DouguoRecipeInfoCollection douguoRecipeInfoCollection = collections.get(i);
                indexManager.delete(douguoRecipeInfoCollection.getRecipeId(), Constant.INDEX_NAME, Constant.INDEX_TYPE_COOKBOOK);
            }
            logger.info("处理完成：{}个，耗时：{}ms", collections.size(), System.currentTimeMillis() - onceStartTime);
            offset += pageSize;
        }
        logger.info("处理完成，总共耗时：{}ms", System.currentTimeMillis() - startTime);
    }
}
