package parseDubboMonitor;

import org.rembau.test.elasticsearch.tools.GsonUtil;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rembau on 2017/3/9.
 */
public class FormatDubboMonitorDataTest {
    public static void main(String args[]) {
        String json = "{\"_id\" : ObjectId(\"58bd6c031da7816e0eacb71b\"),\"_class\" : \"com.unilife.media.model.mongo.common.parseDubboMonitor.MonitorDataCollection\",\"protocol\" : \"count\",\"host\" : \"10.133.202.169\",\"port\" : NumberInt(0),\"path\" : \"com.unilife.media.service.thirdpublic.ApiTokenService/getNewAccessInfo\",\"method\" : \"getNewAccessInfo\",\"concurrent\" : \"1\",\"interfaceStr\" : \"com.unilife.media.service.thirdpublic.ApiTokenService\",\"elapsed\" : \"207\",\"output\" : \"5899\",\"maxConcurrent\" : \"1\",\"input\" : \"0\",\"application\" : \"umobile\",\"provider\" : \"10.105.44.118:30004\",\"success\" : \"17\",\"failure\" : \"0\",\"maxElapsed\" : \"42\",\"maxInput\" : \"0\",\"maxOutput\" : \"347\",\"timestamp\" : \"1488808962291\",\"date\" : ISODate(\"2017-03-06T14:02:42.291+0000\")}";


        System.out.println(json);

        Pattern patternId = Pattern.compile("ObjectId\\((\\S+)\\)");
        Pattern patternPort = Pattern.compile("NumberInt\\((\\w+)\\)");
        Pattern patternDate = Pattern.compile(",\"date\" : ISODate\\(\"(\\S+)\"\\)");

        Matcher matcher = patternId.matcher(json);
        if (matcher.find()) {
            json = json.replace(matcher.group(), matcher.group(1));
        }

        matcher = patternPort.matcher(json);
        if (matcher.find()) {
            json = json.replace(matcher.group(), matcher.group(1));
        }

        matcher = patternDate.matcher(json);
        if (matcher.find()) {
            json = json.replace(matcher.group(), "");
        }

        System.out.println(json);

        MonitorDataCollection monitorDataCollection = GsonUtil.fromJson(json, MonitorDataCollection.class);
        Date date = new Date();
        Long aLong = Long.valueOf(monitorDataCollection.getTimestamp());
        date.setTime(aLong);
        monitorDataCollection.setDate(date);
        System.out.println(GsonUtil.toJson(monitorDataCollection));

    }
}
