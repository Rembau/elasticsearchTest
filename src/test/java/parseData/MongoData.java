package parseData;

import java.io.Serializable;

/**
 * Created by rembau on 2017/3/13.
 */
public class MongoData implements Serializable {
    private String _id;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }
}
