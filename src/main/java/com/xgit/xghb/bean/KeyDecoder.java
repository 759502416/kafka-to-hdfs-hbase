package com.xgit.xghb.bean;

import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @program: xghbreceive
 * @description: key的编码
 * @author: Mr.Wang
 * @create: 2019-12-17 10:35
 **/
public class KeyDecoder implements Deserializer<Byte> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Byte deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public void close() {

    }
}
