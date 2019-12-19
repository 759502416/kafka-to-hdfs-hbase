package com.xgit.xghb.excutor;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 存储类型
 * @author: Mr.Wang
 * @create: 2019-12-18 22:26
 **/
public enum StoreType {
    HDFS("hdfs","HDFS"),HBASE("hbase","HBASE");
    private String type;
    private String name;

    StoreType(String name,String type) {
        this.name = name;
        this.type = type;
    }
    public String getTypeWithName(){
        return type;
    }
}
