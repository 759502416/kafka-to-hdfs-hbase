package com.xgit.xghb.excutor.consumer;

import com.xgit.xghb.bean.ForwardingCarrier;
import com.xgit.xghb.excutor.AbstractConsumerKafka;
import com.xgit.xghb.hbase.writer.ForwardingCarrierHbaseWriter;
import com.xgit.xghb.hdfs.writer.ForwardingCarrierHdfsWriter;
import common.bean.BeanFactory;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 徐工信息的topic的消费者
 * @author: Mr.Wang
 * @create: 2019-12-17 14:37
 * 注意事项：
 * ①需要实现一个保存偏移量的三方库，我选择redis，配的是rediscentence，可自行替换
 * ②需要实现一个消费线程
 **/
public class EnvXinXinConsumer extends AbstractConsumerKafka<Byte, ForwardingCarrier> {

    /**
     * 在此方法中实现设置 hbase表名 和  hdfs 路径前缀
     *
     * @param hbaseTableName  hbase表名
     * @param hdfsPathPrefix  hdfs 路径前缀
     */
    public EnvXinXinConsumer(String hbaseTableName, String hdfsPathPrefix) {
        super(hbaseTableName,hdfsPathPrefix);
        this.hbaseWriter = new ForwardingCarrierHbaseWriter(hbaseTableName);
        this.hdfsWriter = new ForwardingCarrierHdfsWriter(hdfsPathPrefix);
    }

    @Override
    protected void setKafkaConsumerProperties() {
        this.kafkaConsumerProperties = BeanFactory.getBean("consumerProperties");
    }

}
