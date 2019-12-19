package com.xgit.xghb;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: kafka配置文件
 * @author: Mr.Wang
 * @create: 2019-12-14 17:12
 **/
public class KafkaConsumerProperties {
    /**
     * kafka的服务地址们
     */
    private String bootstrapServers;
    /**
     * 消费kafka的分组Id
     */
    private String groupId;
    /**
     * 是否自动提交偏移量
     */
    private String enableAutoCommit = "false";
    /**
     * 消费策略
     * earliest
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费
     * latest
     * 当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
     * none
     * topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
     */
    private String autoOffsetreset = "latest";
    /**
     * key 的编码器
     */
    private String keyDeserializer = "com.xgit.xghb.bean.KeyDecoder";
    /**
     * value 的编码器
     */
    private String valueDeserializer = "com.xgit.xghb.bean.ForwadingCarrierDecoder";

    /**
     * max.poll.records 这个值的意思是每次poll拉取数据的最大任务数
     */
    private int maxPollRecords = 5;

    /**
     * max.poll.interval.ms 每俩次poll拉取数据时间间隔最大超时时间，超过这个值，broker就会认为你这个消费者挂了，并且重新平衡
     */
    private int maxPollIntervalMs = 300000;



    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public void setEnableAutoCommit(String enableAutoCommit) {
        this.enableAutoCommit = enableAutoCommit;
    }

    public String getAutoOffsetreset() {
        return autoOffsetreset;
    }

    public void setAutoOffsetreset(String autoOffsetreset) {
        this.autoOffsetreset = autoOffsetreset;
    }

    public String getKeyDeserializer() {
        return keyDeserializer;
    }

    public void setKeyDeserializer(String keyDeserializer) {
        this.keyDeserializer = keyDeserializer;
    }

    public String getValueDeserializer() {
        return valueDeserializer;
    }

    public void setValueDeserializer(String valueDeserializer) {
        this.valueDeserializer = valueDeserializer;
    }


    public String getEnableAutoCommit() {
        return enableAutoCommit;
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

    public int getMaxPollIntervalMs() {
        return maxPollIntervalMs;
    }

    public void setMaxPollIntervalMs(int maxPollIntervalMs) {
        this.maxPollIntervalMs = maxPollIntervalMs;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"bootstrapServers\":\"")
                .append(bootstrapServers).append('\"');
        sb.append(",\"groupId\":\"")
                .append(groupId).append('\"');
        sb.append(",\"enableAutoCommit\":\"")
                .append(enableAutoCommit).append('\"');
        sb.append(",\"autoOffsetreset\":\"")
                .append(autoOffsetreset).append('\"');
        sb.append(",\"keyDeserializer\":\"")
                .append(keyDeserializer).append('\"');
        sb.append(",\"valueDeserializer\":\"")
                .append(valueDeserializer).append('\"');
        sb.append(",\"maxPollRecords\":")
                .append(maxPollRecords);
        sb.append(",\"maxPollIntervalMs\":")
                .append(maxPollIntervalMs);
        sb.append('}');
        return sb.toString();
    }
}
