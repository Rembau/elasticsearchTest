package parseDubboMonitor;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by rembau on 2017/2/27.
 */
public class MonitorDataCollection implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 6188541888575496153L;
	private String _id;
    private String type;
    private String protocol;
    private String userName;
    private String password;
    private String host;
    private int port;
    private String path;//接口方法路径
    private String method;//方法
    private long concurrent;//并发数
    private String interfaceStr;//接口
    private long elapsed;//耗时
    private long avgElapsed;//耗时
    private long output;//输出
    private long maxConcurrent;//最大并发
    private long input;//输入
    private String application;//应用名称
    private String provider;//提供者
    private String consumer;//消费者
    private long success;//成功次数
    private long failure;//失败次数
    private long maxElapsed;//最大耗时
    private long maxInput;//最大输入
    private long maxOutput;//最大输出
    private String timestamp;
    private Date date;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public long getConcurrent() {
        return concurrent;
    }

    public void setConcurrent(long concurrent) {
        this.concurrent = concurrent;
    }

    public String getInterfaceStr() {
        return interfaceStr;
    }

    public void setInterfaceStr(String interfaceStr) {
        this.interfaceStr = interfaceStr;
    }

    public long getElapsed() {
        return elapsed;
    }

    public void setElapsed(long elapsed) {
        this.elapsed = elapsed;
    }

    public long getAvgElapsed() {
        return avgElapsed;
    }

    public void setAvgElapsed(long avgElapsed) {
        this.avgElapsed = avgElapsed;
    }

    public long getOutput() {
        return output;
    }

    public void setOutput(long output) {
        this.output = output;
    }

    public long getMaxConcurrent() {
        return maxConcurrent;
    }

    public void setMaxConcurrent(long maxConcurrent) {
        this.maxConcurrent = maxConcurrent;
    }

    public long getInput() {
        return input;
    }

    public void setInput(long input) {
        this.input = input;
    }

    public String getApplication() {
        return application;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

    public long getSuccess() {
        return success;
    }

    public void setSuccess(long success) {
        this.success = success;
    }

    public long getFailure() {
        return failure;
    }

    public void setFailure(long failure) {
        this.failure = failure;
    }

    public long getMaxElapsed() {
        return maxElapsed;
    }

    public void setMaxElapsed(long maxElapsed) {
        this.maxElapsed = maxElapsed;
    }

    public long getMaxInput() {
        return maxInput;
    }

    public void setMaxInput(long maxInput) {
        this.maxInput = maxInput;
    }

    public long getMaxOutput() {
        return maxOutput;
    }

    public void setMaxOutput(long maxOutput) {
        this.maxOutput = maxOutput;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
