package parseDubboMonitor;

import org.junit.Test;
import org.rembau.test.elasticsearch.tools.GsonUtil;

import java.io.*;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rembau on 2017/3/9.
 */
public class FormatJson {

    @Test
    public void addEsInfo() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\youyue\\Desktop\\tem\\dubbo-monitor-lines.json"));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("C:\\Users\\youyue\\Desktop\\tem\\dubbo-monitor-lines-es.json"));

            long start = System.currentTimeMillis();
            int i = 1;

            Pattern patternJson = Pattern.compile("\\{.*?\\}");

            Pattern patternId = Pattern.compile("ObjectId\\((\\S+)\\)");
            Pattern patternPort = Pattern.compile("NumberInt\\((\\w+)\\)");
            Pattern patternDate = Pattern.compile(",\"date\" : ISODate\\(\"(\\S+)\"\\)");


            String json = bufferedReader.readLine();
            String newLine = "";
            while (json != null) {
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

//                System.out.println(json);

                MonitorDataCollection monitorDataCollection = null;
                try {
                    monitorDataCollection = GsonUtil.fromJson(json, MonitorDataCollection.class);
                } catch (Exception e) {
                    System.out.println(json);
                    e.printStackTrace();
                    return;
                }
                Date date = new Date();
                Long aLong = Long.valueOf(monitorDataCollection.getTimestamp());
                date.setTime(aLong);
                monitorDataCollection.setDate(date);
//                System.out.println(GsonUtil.toJson(monitorDataCollection));

                newLine = "{ \"index\" : { \"_index\" : \"dubbo\", \"_type\" : \"dubboMonitor\", \"_id\": \""+monitorDataCollection.get_id()+"\" } }";

                bufferedWriter.write(newLine);
                bufferedWriter.newLine();
                bufferedWriter.write(GsonUtil.toJson(monitorDataCollection));
                bufferedWriter.newLine();

                System.out.println("处理第"+i+"行完成，耗时：" + (System.currentTimeMillis() - start));
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
    public void remoreKonggeHuanhang() {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("C:\\Users\\youyue\\Desktop\\tem\\dubbo-monitor.json"));
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("C:\\Users\\youyue\\Desktop\\tem\\dubbo-monitor-lines.json"));

            Pattern patternJson = Pattern.compile("");

            String line = bufferedReader.readLine();
            StringBuilder newLine = new StringBuilder();
            long start = System.currentTimeMillis();
            int i = 1;
            while (line != null) {
                String trim = line.trim();

                if (trim.startsWith("}")) {
                    newLine.append("}");
                    String str = String.valueOf(newLine);

                    if (!str.matches("\\{[^}]*?\\}")) {
                        System.out.println(str);
                        return;
                    }

                    bufferedWriter.write(str);
                    bufferedWriter.newLine();
                    newLine = new StringBuilder();
                    System.out.println("处理第"+i+"行完成，耗时：" + (System.currentTimeMillis() - start));
                    i++;
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
