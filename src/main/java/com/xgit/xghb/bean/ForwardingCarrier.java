package com.xgit.xghb.bean;

import common.Utils.DateUtil;

import java.util.Date;
import java.util.List;

/**
 * @program: onLineTranspondProject
 * @description: 转发数据载体
 * @author: Mr.Wang
 * @create: 2019-11-07 11:50
 **/
public class ForwardingCarrier {

    /**
     * 写入请求数据
     */
    private String exchangeType;

    /**
     * 交换码
     */
    private String exchangeCode;

    /**
     * 请求时间
     */
    private String requestTime = DateUtil.dateToString(new Date(), "yyyyMMddHHmmss");;

    /**
     * body
     */
    private List<Body> body;

    /**
     * 版本号
     */
    private String version = "1.0";

    public String getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(String exchangeType) {
        this.exchangeType = exchangeType;
    }

    public String getExchangeCode() {
        return exchangeCode;
    }

    public void setExchangeCode(String exchangeCode) {
        this.exchangeCode = exchangeCode;
    }

    public String getRequestTime() {
        return requestTime;
    }

    public void setRequestTime(String requestTime) {
        this.requestTime = requestTime;
    }

    public List<Body> getBody() {
        return body;
    }

    public void setBody(List<Body> body) {
        this.body = body;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"exchangeType\":\"")
                .append(exchangeType).append('\"');
        sb.append(",\"exchangeCode\":\"")
                .append(exchangeCode).append('\"');
        sb.append(",\"requestTime\":\"")
                .append(requestTime).append('\"');
        sb.append(",\"body\":")
                .append(body);
        sb.append(",\"version\":\"")
                .append(version).append('\"');
        sb.append('}');
        return sb.toString();
    }
}
