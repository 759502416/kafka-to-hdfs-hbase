package com.xgit.xghb.excutor;

import com.xgit.xghb.KafkaConsumerProperties;
import com.xgit.xghb.bean.ForwardingCarrier;
import common.bean.BeanFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 测试kafka多线程消费并保存偏移量
 * @author: Mr.Wang
 * @create: 2019-12-17 09:56
 **/
public class Test {
    public static void main(String[] args) {
        Properties props = new Properties();
        KafkaConsumerProperties consumerProperties = BeanFactory.getBean("consumerProperties");
        props.put("bootstrap.servers", consumerProperties.getBootstrapServers());
        props.put("group.id", consumerProperties.getGroupId());
        props.put("enable.auto.commit", consumerProperties.getEnableAutoCommit());
        props.put("key.deserializer", consumerProperties.getKeyDeserializer());
        props.put("value.deserializer", consumerProperties.getValueDeserializer());
        props.put("max.poll.records",consumerProperties.getMaxPollRecords());
        props.put("max.poll.interval.ms",consumerProperties.getMaxPollIntervalMs());
        KafkaConsumer<Byte, ForwardingCarrier> consumer = new KafkaConsumer<Byte, ForwardingCarrier>(props);


        String topicName = "testEnvXinXin";
        // 分配topic 和 partition
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition,0);
        int count = 0;
        while (true) {
            ConsumerRecords<Byte, ForwardingCarrier> records = consumer.poll(1000);
            for (ConsumerRecord<Byte, ForwardingCarrier> record : records) {
                System.out.println("value = " + record.value() + ", topic = " + record.topic() + ", partition = "
                        + record.partition() + ", offset = " + record.offset());
                count++;
                System.err.println(count);
            }
        }
    }
}
