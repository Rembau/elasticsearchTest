package parseData;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by rembau on 2017/3/13.
 */
public class MongoData implements Serializable {
    private Map<String, Integer> _id;

    public Map<String, Integer> get_id() {
        return _id;
    }

    public void set_id(Map<String, Integer> _id) {
        this._id = _id;
    }
}
