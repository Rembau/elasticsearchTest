package org.rembau.test.elasticsearch.data;

import com.unilife.media.model.mongo.douguo.DouguoRecipeInfoCollection;
import org.rembau.test.elasticsearch.commons.Constant;
import org.rembau.test.elasticsearch.index.IndexManager;
import org.rembau.test.elasticsearch.mongo.MongoBaseService;
import org.rembau.test.elasticsearch.tools.GsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by rembau on 2017/3/3.
 */
@Service
public class CookbookData extends MongoBaseService {

    @Autowired
    private IndexManager indexManager;

    @PostConstruct
    public void handle() {
        Map<String, Object> map = new HashMap();
        DouguoRecipeInfoCollection douguoRecipeInfoCollection = getOne(map, DouguoRecipeInfoCollection.class);
        indexManager.create(douguoRecipeInfoCollection.getRecipeId(), GsonUtil.toJson(douguoRecipeInfoCollection), Constant.INDEX_NAME, Constant.INDEX_TYPE_COOKBOOK);
    }

}
