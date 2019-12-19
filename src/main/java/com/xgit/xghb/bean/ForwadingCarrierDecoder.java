package com.xgit.xghb.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * @program: KafkaStudy
 * @description: 承载对象解码器
 * @author: Mr.Wang
 * @create: 2019-12-12 10:34
 **/
public class ForwadingCarrierDecoder implements Deserializer<ForwardingCarrier> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public ForwardingCarrier deserialize(String s, byte[] bytes) {
        ByteBuffer wrap = ByteBuffer.wrap(bytes);
        // 交换类型长度
        int exchangeTypeBytesLength = wrap.getInt();
        // 交换类型
        byte[] exchangeTypeBytes = new byte[exchangeTypeBytesLength];
        for (int i = 0; i < exchangeTypeBytesLength; i++) {
            exchangeTypeBytes[i] = wrap.get();
        }
        // 获取到交换类型
        String exchangeType = new String(exchangeTypeBytes);

        // 交换码长度
        int exchangeCodeBytesLength = wrap.getInt();
        // 交换码字节
        byte[] exchangeCodeBytes = new byte[exchangeCodeBytesLength];
        for (int i = 0; i < exchangeCodeBytesLength; i++) {
            exchangeCodeBytes[i] = wrap.get();
        }
        // 获取到交换码
        String exchangeCode = new String(exchangeCodeBytes);

        // 请求时间长度
        int requestTimeBytesLength = wrap.getInt();
        // 请求时间字节
        byte[] requestTimeBytes = new byte[requestTimeBytesLength];
        for (int i = 0; i < requestTimeBytesLength; i++) {
            requestTimeBytes[i] = wrap.get();
        }
        // 获取请求时间
        String requestTime = new String(requestTimeBytes);

        // body字节长度
        int bodyBytesLength = wrap.getInt();
        // body字节
        byte[] bodyBytes = new byte[bodyBytesLength];
        for (int i = 0; i < bodyBytesLength; i++) {
            bodyBytes[i] = wrap.get();
        }
        String bodyJson = new String(bodyBytes);
        // 获得body
        List<Body> body = JSONObject.parseArray(bodyJson, Body.class);

        // 版本号字节长度
        int versionBytesLength = wrap.getInt();
        // 版本号字节
        byte[] versionBytes = new byte[versionBytesLength];
        for (int i = 0; i < versionBytesLength; i++) {
            versionBytes[i] = wrap.get();
        }
        // 获得版本号
        String version = new String(versionBytes);

        // 填充对象
        ForwardingCarrier forwardingCarrier = new ForwardingCarrier();
        forwardingCarrier.setBody(body);
        forwardingCarrier.setExchangeCode(exchangeCode);
        forwardingCarrier.setRequestTime(requestTime);
        forwardingCarrier.setVersion(version);
        forwardingCarrier.setExchangeType(exchangeType);
        return forwardingCarrier;
    }

    @Override
    public void close() {

    }
}
