package com.xgit.xghb.excutor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xgit.xghb.KafkaConsumerProperties;
import com.xgit.xghb.bean.TopicNameAndPartitionNumberCarrier;
import com.xgit.xghb.hbase.AbstractHbaseWriter;
import com.xgit.xghb.hdfs.AbstractHdfsWriter;
import common.nosqldao.JedisDao;
import common.nosqldao.impl.jedis.JedisSentinelDaoImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * @param <K> Topic的Key的类型
 * @param <V> Topic的Value的类型
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 消费kafka信息
 * @author: Mr.Wang
 * @create: 2019-12-17 08:44
 **/
public abstract class AbstractConsumerKafka<K, V> {
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * 在此方法中实现设置 hbase表名 和  hdfs 路径前缀
     *
     * @param hbaseTableName
     * @param hdfsPathPrefix
     */
    public AbstractConsumerKafka(String hbaseTableName, String hdfsPathPrefix) {
    }

    /**
     * 此处是个人保存偏移量的手段，可自由重写
     * jedisDao 用于操作jedis
     */
    private static JedisDao jedisDao = new JedisSentinelDaoImpl();

    /**
     * 构造线程池
     */
    protected ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("ConsumerKafka-runner-%d").build();
    protected ExecutorService executorService = new ThreadPoolExecutor(15, 30, 1000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), namedThreadFactory);

    //TODO 存放hdfs消费队列的map   k是分区号    v是阻塞队列
    protected volatile Map<String, ArrayBlockingQueue<V>> hdfsQueneMap = new ConcurrentHashMap<>();

    //TODO 存放hbase消费队列的map  k是分区号    v是阻塞队列
    protected volatile Map<String, ArrayBlockingQueue<V>> hbaseQueneMap = new ConcurrentHashMap<>();


    /**
     * Kafka 消费者的属性配置
     */
    protected KafkaConsumerProperties kafkaConsumerProperties;

    /**
     * 写入hbase的writer
     */
    protected AbstractHbaseWriter hbaseWriter;

    /**
     * 写入hdfs的writer
     */
    protected AbstractHdfsWriter hdfsWriter;

    /**
     * 必须实现设置 kafkaConsumerProperties
     */
    protected abstract void setKafkaConsumerProperties();

    /**
     * Topic 名称 和 Topic 分区数 载体的对象
     */
    private TopicNameAndPartitionNumberCarrier topicNameAndPartitionNumberCarrier;


    /**
     * 轮空等待的最大轮次，超过此轮次会消费完一次通道，默认3次
     */
    public static int emptyMaxCount = 30;

    /**
     * 每次轮空后,线程的休眠时间，默认30秒
     */
    public static Long emptySleepTime = 30000L;

    /**
     * 设置消费存储系统的轮空等待的最大轮次，超过此轮次会消费完一次通道
     *
     * @param emptyMaxCount 轮空等待的最大轮次
     */
    public static void setEmptyMaxCount(int emptyMaxCount) {
        AbstractConsumerKafka.emptyMaxCount = emptyMaxCount;
    }

    /**
     * 设置消费存储系统的每次轮空后,线程的休眠时间
     *
     * @param emptySleepTime 线程的休眠时间
     */
    public static void setEmptySleepTime(Long emptySleepTime) {
        AbstractConsumerKafka.emptySleepTime = emptySleepTime;
    }

    /**
     * 获得kafka消费者
     *
     * @return
     * @throws Exception
     */
    protected KafkaConsumer<K, V> getKafkaConsumer(String groupId) throws Exception {
        if (null == this.kafkaConsumerProperties) {
            throw new Exception(" 请先设置 kafkaConsumerProperties!!!!");
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", this.kafkaConsumerProperties.getBootstrapServers());
        if (null != groupId) {
            props.put("group.id", this.kafkaConsumerProperties.getGroupId());
        }
        props.put("enable.auto.commit", this.kafkaConsumerProperties.getEnableAutoCommit());
        props.put("key.deserializer", this.kafkaConsumerProperties.getKeyDeserializer());
        props.put("value.deserializer", this.kafkaConsumerProperties.getValueDeserializer());
        props.put("max.poll.records", this.kafkaConsumerProperties.getMaxPollRecords());
        props.put("max.poll.interval.ms", this.kafkaConsumerProperties.getMaxPollIntervalMs());
        logger.info("配置信息为:%s", this.kafkaConsumerProperties.toString());
        return new KafkaConsumer<K, V>(props);
    }

    /**
     * 执行流程编号1.根据给定的Topic进行分区消费处理。
     *
     * @param topicNameAndPartitionNumberCarrier topic的名称，分区载体
     * @param executorService                    线程池
     */
    protected void consumerTopicWithPartitionAndOffset(TopicNameAndPartitionNumberCarrier topicNameAndPartitionNumberCarrier
            , ExecutorService executorService) {
        // 获得topic名称
        String topicName = topicNameAndPartitionNumberCarrier.getTopicName();
        // 获得topic有多少分区数量
        int topicPartitionNumbers = topicNameAndPartitionNumberCarrier.getTopicPartitionNumbers();

        //每个分区指定一个消费者进行消费
        for (int number = 0; number < topicPartitionNumbers; number++) {
            try {
                // 获得 kafkaConsumer
                KafkaConsumer<K, V> HbasekafkaConsumer = getKafkaConsumer("cusumer-A");
                executorService.execute(new ExcutorWithTopicPartition(StoreType.HBASE.getTypeWithName(), topicName, number, HbasekafkaConsumer));
                KafkaConsumer<K, V> HdfskafkaConsumer = getKafkaConsumer("cusumer-B");
                executorService.execute(new ExcutorWithTopicPartition(StoreType.HDFS.getTypeWithName(), topicName, number, HdfskafkaConsumer));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        /****执行消费hdfs数据队列***/
        executorService.execute(new ConsumeBlockArray(StoreType.HDFS.getTypeWithName()));
        /****执行消费hbase数据队列***/
        executorService.execute(new ConsumeBlockArray(StoreType.HBASE.getTypeWithName()));
    }

    /**
     * 执行流程编号2 执行分区消费的线程
     */
    protected class ExcutorWithTopicPartition implements Runnable {
        /**
         * 获得 logger 日志对象
         */
        private Logger logger = LoggerFactory.getLogger(ExcutorWithTopicPartition.class);

        /**
         * topic 的名称
         */
        private String topicName;

        /**
         * topic 的分区号
         */
        private int topicPatitionNo;

        /**
         * kafka 消费者
         */
        private KafkaConsumer<K, V> consumer;

        /**
         * 存储类型
         */
        private String consumerType;

        public ExcutorWithTopicPartition(String type, String topicName, int topicPatitionNo, KafkaConsumer<K, V> consumer) {
            this.consumerType = type;
            this.topicName = topicName;
            this.topicPatitionNo = topicPatitionNo;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            TopicPartition topicPartition = new TopicPartition(topicName, topicPatitionNo);
            consumer.assign(Arrays.asList(topicPartition));
            String key = null;
            key = ConsumerUtil.getRedisKey(this.topicName, consumerType, this.topicPatitionNo);
            Long oldOffset = getOldOffsetFromRedis(key);
            consumer.seek(topicPartition, oldOffset+1);
            ArrayBlockingQueue<V> consumerPartitionqQuene = new ArrayBlockingQueue(1000000);

            // 添加队列到currentMap中
            /***如果是hbase***/
            if (consumerType.equals(StoreType.HBASE.getTypeWithName())) {
                synchronized (hbaseQueneMap) {
                    hbaseQueneMap.put(topicPatitionNo + "", consumerPartitionqQuene);
                }
                /****如果是hdfs*****/
            } else if (consumerType.equals(StoreType.HDFS.getTypeWithName())) {
                synchronized (hdfsQueneMap) {
                    hdfsQueneMap.put(topicPatitionNo + "", consumerPartitionqQuene);
                }
            } else {
                logger.error("消费 队列阶段，错误的类型匹配");
                return;
            }
            System.err.println("添加消费队列成功的组");
            // 用来判断是否是第一次消费
            while (true) {
                ConsumerRecords<K, V> records = consumer.poll(1000);
                for (ConsumerRecord<K, V> record : records) {
                    V value = record.value();
                    /****加入到hdfs 对应分区的消费队列****/
                    boolean isAddHdfsSuccess = consumerPartitionqQuene.add(value);
                    if (isAddHdfsSuccess) {
                    } else {
                        logger.error("未能成功添加到{},消费队列中，Topic名称：{},分区号:{},偏移量:{}", consumerType, record.topic(), record.partition(), record.offset());
                    }
                }
            }
        }
    }

    /**
     * 供外部调用的执行消费方法
     *
     * @param topicNameAndPartitionNumberCarrier topic的名称，分区号载体
     */
    public void excutorConsumer(TopicNameAndPartitionNumberCarrier topicNameAndPartitionNumberCarrier) throws Exception {
        // 调用设置kafka消费配置方法
        setKafkaConsumerProperties();
        this.topicNameAndPartitionNumberCarrier = topicNameAndPartitionNumberCarrier;
        if (null == this.hdfsWriter || null == this.hbaseWriter || null == this.kafkaConsumerProperties) {
            throw new Exception("请先配置好 hdfsWriter,hbaseWriter,kafka的消费者配置信息！！！");
        }
        logger.info("预先配置信息已经配置完毕！");
        consumerTopicWithPartitionAndOffset(topicNameAndPartitionNumberCarrier, this.executorService);
    }

    /**
     * 执行流程编号4 调用hdfs的write方法,按当日时间滚动
     *
     * @param forwardingCarriers 要写入的对象
     */
    public synchronized void hdfsWriteMethod(List<V> forwardingCarriers) {
        try {
            String filePathPrefix = this.hdfsWriter.getFilePathPrefix();
            String fileName = new SimpleDateFormat("yyyyMMdd").format(new Date());
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append(filePathPrefix);
            stringBuffer.append(fileName);
            try {
                this.hdfsWriter.excuteWrite(stringBuffer.toString(), forwardingCarriers);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * 执行流程编号4 调用hbase的write方法
     *
     * @param forwardingCarriers 要写入的对象
     */
    public synchronized void hbaseWriteMethod(List<V> forwardingCarriers) {
        try {
            this.hbaseWriter.write(forwardingCarriers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 执行流程编号3  消费 队列，100条数据消费一次
     * <p>
     * 消费规则：① 数据量达到1000条
     * ② 三分钟内没有达到1000条，但是数据一直在产生
     * 以上条件，满足其一即可消费
     */
    public class ConsumeBlockArray implements Runnable {
        /**
         * 消费类型，是HDFS 或 Hbase
         */
        private String consumerType;

        /**
         * 轮空等待的最大轮次，超过此轮次会消费完一次通道
         */
        public int emptyMaxCount = AbstractConsumerKafka.emptyMaxCount;

        /**
         * 每次轮空后,线程的休眠时间
         */
        public Long emptySleepTime = AbstractConsumerKafka.emptySleepTime;

        public ConsumeBlockArray(String consumerType) {
            this.consumerType = consumerType;
        }

        @Override
        public void run() {
            int emptyCount = 0;
            int count = 0;
            List<V> forwardingCarriers = new ArrayList<>();
            // 用来存放本次消费了各分区多少数据
            Map<String, Long> offsetMap = new HashMap<>();
            while (true) {
                // TODO 组合所有分区的消费数据
                boolean canConsumeData = false;
                List<V> collectAllPartitionforwardingCarriers = new ArrayList<>();
                for (String partitionNo : hbaseQueneMap.keySet()) {
                    ArrayBlockingQueue<V> vquene = null;
                    if (this.consumerType.equals(StoreType.HBASE.getTypeWithName())) {
                        synchronized (hbaseQueneMap) {
                            vquene = hbaseQueneMap.get(partitionNo);
                        }
                    } else if (this.consumerType.equals(StoreType.HDFS.getTypeWithName())) {
                        synchronized (hdfsQueneMap) {
                            vquene = hdfsQueneMap.get(partitionNo);
                        }
                    } else {
                        logger.error("消费 队列阶段，错误的类型匹配");
                        return;
                    }
                    if (null == vquene) {
                        continue;
                    }
                    V v = vquene.poll();
                    if (null != v) {
                        collectAllPartitionforwardingCarriers.add(v);
                        if (!offsetMap.containsKey(partitionNo)) {
                            offsetMap.put(partitionNo, 0L);
                        }
                        // 在offsetMap中记录该分区队列获得的数据量
                        Long oldoff = offsetMap.get(partitionNo);
                        offsetMap.put(partitionNo, oldoff + 1);
                        canConsumeData = true;
                    }
                }
                // 如果可以获取到数据，则加入
                if (canConsumeData) {
                    forwardingCarriers.addAll(collectAllPartitionforwardingCarriers);
                }
                // 获得载体的数据量
                int size = forwardingCarriers.size();
                if (canConsumeData || size >= 1000) {
                    // 并且列表中有数据要消费
                    if (size >= 1000 || (emptyCount > 5 && size >1)) {
                        logger.info("本次执行写入{}开始,数据数量为:{}", this.consumerType, size);
                        /***如果是hbase***/
                        if (this.consumerType.equals(StoreType.HBASE.getTypeWithName())) {
                            hbaseWriteMethod(forwardingCarriers);
                            /****如果是hdfs*****/
                        } else if (this.consumerType.equals(StoreType.HDFS.getTypeWithName())) {
                            hdfsWriteMethod(forwardingCarriers);
                        }
                        // 写入成功
                        for (String partitionNo : hbaseQueneMap.keySet()) {
                            Long consumerCount = offsetMap.get(partitionNo);
                            // 有需要时则开启
                            //if (consumerCount != 0L) {
                            String key = ConsumerUtil.getRedisKey(topicNameAndPartitionNumberCarrier.getTopicName(), this.consumerType, partitionNo);
                            Long oldOffset = getOldOffsetFromRedis(key);
                            logger.info("Topic:{},类型：{},redis偏移量：{},此次成功消费数据量:{}，分区号：{}", topicNameAndPartitionNumberCarrier.getTopicName(),
                                    this.consumerType, oldOffset, consumerCount, partitionNo);
                            // 老的oldOffset+新消费的数量
                            // TODO redis 卡
                            jedisDao.set(key, Long.toString(oldOffset + consumerCount));
                            //redis 增加该分区hdfs写入偏移量
                            offsetMap.put(partitionNo, 0L);
                            //}
                        }
                        forwardingCarriers.clear();
                        // 已经成功消费，故空次再次归0
                        emptyCount = 0;
                        logger.info("本次执行写入{}结束，count归0", consumerType);
                    }
                } else {
                    try {
                        Thread.sleep(emptySleepTime);
                        emptyCount++;
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    /**
     * 根据key，从redis中获取偏移量
     *
     * @param key
     * @return
     */
    private synchronized Long getOldOffsetFromRedis(String key) {
        if (null == jedisDao.get(key)) {
            return 0L;
        }
        Long oldOffset = Long.parseLong(jedisDao.get(key));
        if (null == oldOffset) {
            oldOffset = 0L;
        }
        return oldOffset;
    }

}
