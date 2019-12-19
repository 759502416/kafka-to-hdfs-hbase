package com.xgit.xghb.bean;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: topic名称和topic数量的载体
 * @author: Mr.Wang
 * @create: 2019-12-17 13:46
 **/
@Component
public class TopicNameAndPartitionNumberCarrier {

    /**
     * topic 名称
     */
    private String topicName;

    /**
     * Topic 分区数量
     */
    private int topicPartitionNumbers;

    public String getTopicName() {
        return topicName;
    }

    public void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    public int getTopicPartitionNumbers() {
        return topicPartitionNumbers;
    }

    public void setTopicPartitionNumbers(int topicPartitionNumbers) {
        this.topicPartitionNumbers = topicPartitionNumbers;
    }
}
