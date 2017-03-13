package parseData;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;

import java.io.*;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rembau on 2017/3/13.
 */
public class DubboDataUtil {

    public static <T extends MongoData> void bulkIn(TransportClient client, String source, String index, String type, Class<T> tClass) {
        try {

            File article = new File(source);
            FileReader fr=new FileReader(article);
            BufferedReader bfr=new BufferedReader(fr);
            String line=null;
            BulkRequestBuilder bulkRequest=client.prepareBulk();

            long start = System.currentTimeMillis();
            long _start = System.currentTimeMillis();
            int count=1;
            while((line=bfr.readLine())!=null) {

                T t = GsonUtil.gson.fromJson(line, tClass);
                IndexRequestBuilder requestBuilder = client.prepareIndex(index, type, t.get_id());
                t.set_id(null);

                String toJson = GsonUtil.gson.toJson(t);
                bulkRequest.add(requestBuilder.setSource(toJson));

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
        } catch (IOException e) {
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

                System.out.println("生成标准JSON数据，处理第"+i+"行完成，耗时：" + (System.currentTimeMillis() - start));
                i++;

                json = bufferedReader.readLine();
            }

            bufferedReader.close();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void remoreKonggeHuanhang(String source, String target) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(source));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(target));

            Pattern patternJson = Pattern.compile("");

            String line = bufferedReader.readLine();
            StringBuilder newLine = new StringBuilder();
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
                line = bufferedReader.readLine();
            }

            bufferedReader.close();
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
