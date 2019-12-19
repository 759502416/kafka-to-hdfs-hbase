package com.xgit.xghb.hdfs;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: hdfs 配置文件
 * @author: Mr.Wang
 * @create: 2019-12-16 08:42
 **/
public class HdfsProperties {
    /**
     * hdfs路径
     */
    private String path;

    /**
     * 是否支持追加写入
     */
    private boolean dfsSupportAppend = true;

}
