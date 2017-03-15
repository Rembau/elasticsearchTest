package parseData;

/**
 * Created by rembau on 2017/3/14.
 */
public interface HandleDataInf {
    <T extends MongoData> void handle(T t);
}
