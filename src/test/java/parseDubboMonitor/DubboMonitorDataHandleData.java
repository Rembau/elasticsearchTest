package parseDubboMonitor;

import parseData.HandleDataInf;
import parseData.MongoData;

/**
 * Created by rembau on 2017/3/14.
 */
public class DubboMonitorDataHandleData implements HandleDataInf {

    @Override
    public <T extends MongoData> void handle(T t) {
        MonitorDataCollection monitorDataCollection = (MonitorDataCollection) t;
        monitorDataCollection.setAvgElapsed(monitorDataCollection.getElapsed()/monitorDataCollection.getSuccess());
    }
}
