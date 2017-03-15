package parseData;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Test;

import java.io.*;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rembau on 2017/3/13.
 */
public class DubboDataUtil {

    public static <T extends MongoData> void bulkIn(TransportClient client, String[] source, String index, String type, Class<T> tClass, HandleDataInf handleDataInf) {
        long start = System.currentTimeMillis();
        for (String fileName : source) {
            System.out.println("开始处理文件：" + fileName);
            bulkIn(client, fileName, index, type, tClass, handleDataInf);
            System.out.println("处理文件：" + fileName + " 完成，耗时：" + (System.currentTimeMillis() - start));
        }
    }

    public static <T extends MongoData> void bulkIn(TransportClient client, String source, String index, String type, Class<T> tClass, HandleDataInf handleDataInf) {

        String line=null;
        int count=1;
        try {
            File article = new File(source);
            FileReader fr=new FileReader(article);
            BufferedReader bfr=new BufferedReader(fr);
            BulkRequestBuilder bulkRequest=client.prepareBulk();

            long start = System.currentTimeMillis();
            long _start = System.currentTimeMillis();

            while((line=bfr.readLine())!=null) {

                try {
                    T t = GsonUtil.gson.fromJson(line, tClass);
                    Map<String, Integer> integerMap = t.get_id();
                    ObjectId objectId = ObjectId.createFromLegacyFormat(integerMap.get("_time"), integerMap.get("_machine"), integerMap.get("_inc"));
                    IndexRequestBuilder requestBuilder = client.prepareIndex(index, type, objectId.toHexString());
                    t.set_id(null);

                    String toJson = null;
                    if (handleDataInf != null) {
                        handleDataInf.handle(t);
                    }
                    toJson = GsonUtil.gson.toJson(t);
                    bulkRequest.add(requestBuilder.setSource(toJson));
                } catch (Exception e) {
                    System.out.println("================异常数据：" + line + "，行数：" + count);
                    e.printStackTrace();
                }


//                IndexRequestBuilder requestBuilder = client.prepareIndex(index, type);
//                bulkRequest.add(requestBuilder.setSource(line));

                if (count%2000==0) {
                    BulkResponse bulkItemResponses = bulkRequest.execute().actionGet();
                    if (bulkItemResponses.hasFailures()) {
                        System.out.println("处理失败：" + GsonUtil.gson.toJson(bulkItemResponses));

                    }
                    System.out.println("处理第 "+count+" 行完成，耗时：" + (System.currentTimeMillis() - start) + "--" + (System.currentTimeMillis() - _start));
                    _start = System.currentTimeMillis();
                    bulkRequest=client.prepareBulk();
                    System.out.println("初始化bulk");
                }

                count++;
            }
            bulkRequest.execute().actionGet();
            System.out.println("处理第 "+count+" 行完成，耗时：" + (System.currentTimeMillis() - start));
            bfr.close();
            fr.close();
        } catch (Exception e) {
            System.out.println("================异常数据：" + line + "，行数：" + count);
            e.printStackTrace();
        }
    }

    public static <T> void addEsInfo(final String source, final String target, final Class<T> tClass) {

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(source));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(target));

            long start = System.currentTimeMillis();
            int i = 1;

            Pattern patternId = Pattern.compile("ObjectId\\((\\S+)\\)");
            Pattern patternInt = Pattern.compile("NumberInt\\((\\w+)\\)");
            Pattern patternDate = Pattern.compile("ISODate\\((\"\\S+\")\\)");


            String json = bufferedReader.readLine();
            String newLine = "";
            while (json != null) {
                Matcher matcher = patternId.matcher(json);
                while (matcher.find()) {
                    json = json.replace(matcher.group(), matcher.group(1));
                }

                matcher = patternInt.matcher(json);
                while (matcher.find()) {
                    json = json.replace(matcher.group(), matcher.group(1));
                }

                matcher = patternDate.matcher(json);
                while (matcher.find()) {
                    json = json.replace(matcher.group(), matcher.group(1));
                }

//                System.out.println(json);

                T t = null;
                try {
                    t = GsonUtil.gson.fromJson(json, tClass);
                } catch (Exception e) {
                    System.out.println(json);
                    e.printStackTrace();
                    return;
                }

                bufferedWriter.write(GsonUtil.gson.toJson(t));
                bufferedWriter.newLine();

                if (i % 10000 == 0) {
                    System.out.println("生成标准JSON数据，处理第"+i+"行完成，耗时：" + (System.currentTimeMillis() - start));
                }

                i++;

                json = bufferedReader.readLine();
            }

            bufferedReader.close();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testObjectId() {
        org.bson.types.ObjectId objectId = org.bson.types.ObjectId.createFromLegacyFormat(1488808962, 1923037543, -430984668);
        System.out.println(GsonUtil.gson.toJson(objectId) + " " + objectId.toHexString());
    }

    public static void readBsonFile(String bsonPah, String target) {
        BufferedInputStream bufferedInputStream = null;
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        try {
            bufferedInputStream = new BufferedInputStream(new FileInputStream(bsonPah));
            fileWriter = new FileWriter(target);
            bufferedWriter = new BufferedWriter(fileWriter, 50*1024*1024);

            ObjectMapper mapper = new ObjectMapper(new BsonFactory());
            @SuppressWarnings("deprecation")
            MappingIterator<BSONObject> iterator =
                    mapper.reader(BasicBSONObject.class).readValues(bufferedInputStream);

            int fileNum = 1;
            int i = 1;
            long start = System.currentTimeMillis();
            while (iterator.hasNext()) {
                BSONObject object = iterator.next();
                String json = GsonUtil.gson.toJson(object);

                bufferedWriter.write(json);
                bufferedWriter.newLine();

                if (i % 10000 == 0) {
                    System.out.println("解析bson文件，处理第" + i + "条完成，耗时：" + (System.currentTimeMillis() - start));
                }
                i++;

                if (i % 3000000 == 0) {   //300万行，换一个文件fileWriter);
                    try {
                        bufferedWriter.close();
                        fileWriter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    fileWriter = new FileWriter(target + "." + fileNum);
                    bufferedWriter = new BufferedWriter(fileWriter, 50*1024*1024);
                    fileNum ++;
                }
            }
            System.out.println("解析bson文件，处理第" + i + "条完成，耗时：" + (System.currentTimeMillis() - start));
            System.out.println("解析bson文件，完成");

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                bufferedWriter.close();
                fileWriter.close();
                bufferedInputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void remoreKonggeHuanhang(String source, String target) {
        StringBuilder newLine = new StringBuilder();
        try {
            FileReader fileReader = new FileReader(source);
            BufferedReader bufferedReader = new BufferedReader(fileReader, 50*1024*1024);
            FileWriter fileWriter = new FileWriter(target);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter, 50*1024*1024);

            String line = bufferedReader.readLine();
            long start = System.currentTimeMillis();
            Stack<String> stack = new Stack<>();
            int i = 1;
            while (line != null) {
                String trim = line.trim();

                if (trim.contains("{")) {
                    stack.push("{");
                }

                if (trim.startsWith("}")) {
                    stack.pop();
                    if (stack.size() == 0) {
                        newLine.append("}");
                        String str = String.valueOf(newLine);

                        if (!str.matches("\\{.*?\\}")) {
                            System.out.println("json格式不正确" + str);
                            return;
                        }

                        bufferedWriter.write(str);
                        bufferedWriter.newLine();
                        newLine = new StringBuilder();
                        System.out.println("去除空格，处理第" + i + "行完成，耗时：" + (System.currentTimeMillis() - start));
                        i++;
                    } else {
                        newLine.append(trim);
                    }
                } else {
                    newLine.append(trim);
                }
                System.out.println("newLine.length：" + newLine.length());
                line = bufferedReader.readLine();
            }

            bufferedReader.close();
            bufferedWriter.close();
        } catch (Throwable e) {
            System.out.println("异常：" + newLine);
            e.printStackTrace();
        }
    }
}
