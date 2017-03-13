package parsePvData;

import parseData.MongoData;

import java.util.Date;
import java.util.Map;

/**
 * Created by rembau on 2017/2/14.
 */
public class WebEventCollection extends MongoData {
    private String uuid;
    private String eventType;
    private Integer userId;
    private String account;
    private String eventId;
    private String url;
    private String openId;
    private Date eventTime;
    private Integer stayTime;   //ms
    private Map<String, Object> attrs;
    private String pvCookieId;
    private String sessionId;
    private String referer;
    private String refererAttr;
    private String ip;

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public Integer getStayTime() {
        return stayTime;
    }

    public void setStayTime(Integer stayTime) {
        this.stayTime = stayTime;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getOpenId() {
        return openId;
    }

    public void setOpenId(String openId) {
        this.openId = openId;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    public Map<String, Object> getAttrs() {
        return attrs;
    }

    public void setAttrs(Map<String, Object> attrs) {
        this.attrs = attrs;
    }

    public String getPvCookieId() {
        return pvCookieId;
    }

    public void setPvCookieId(String pvCookieId) {
        this.pvCookieId = pvCookieId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getReferer() {
        return referer;
    }

    public void setReferer(String referer) {
        this.referer = referer;
    }

    public String getRefererAttr() {
        return refererAttr;
    }

    public void setRefererAttr(String refererAttr) {
        this.refererAttr = refererAttr;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

}
