package com.xgit.xghb.excutor;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 消费者的工具类
 * @author: Mr.Wang
 * @create: 2019-12-18 22:08
 **/
public class ConsumerUtil {

    /**
     * 拼出 topicName+hbase+分区号
     *
     * @param topicName   topic名称
     * @param storeType   存储类型
     * @param partitionNo 分区编号
     * @return
     */
    public static String getRedisKey(String topicName, String storeType, int partitionNo) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(topicName);
        stringBuffer.append("test03");
        stringBuffer.append(storeType);
        stringBuffer.append(partitionNo);
        return stringBuffer.toString();
    }

    /**
     * 拼出 topicName+hbase+分区号
     *
     * @param topicName   topic名称
     * @param storeType   存储类型
     * @param partitionNo 分区编号
     * @return
     */
    public static String getRedisKey(String topicName, String storeType, String partitionNo) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(topicName);
        stringBuffer.append("test03");
        stringBuffer.append(storeType);
        stringBuffer.append(partitionNo);
        return stringBuffer.toString();
    }
}
