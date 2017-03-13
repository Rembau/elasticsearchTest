package parseData;

import com.google.gson.*;

import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Created by rembau on 2017/3/13.
 */
public class GsonUtil {
    public static Gson gson;
    static {
        final GsonBuilder gsonBuilder = new GsonBuilder();

        final DateFormat iso8601Format = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ", Locale.US);
        iso8601Format.setTimeZone(TimeZone.getTimeZone("UTC"));

        final DateFormat simpleDateFormat = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        gsonBuilder.registerTypeAdapter(Date.class, new JsonDeserializer<Date>() {

            public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
                String asString = null;
                try {
                    asString = json.getAsJsonPrimitive().getAsString();
                    return iso8601Format.parse(asString);
                } catch (ParseException e) {
                    System.out.println("时间字符串解析失败：" + json + "，时间字符串：" + asString);
                    e.printStackTrace();
                }
                return null;
            }
        });

        gsonBuilder.registerTypeAdapter(Date.class, new JsonSerializer<Date>(){
            @Override
            public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext context) {
                String format = simpleDateFormat.format(src);
                return new JsonPrimitive(format);
            }
        });

        gson = gsonBuilder.create();
    }
}
