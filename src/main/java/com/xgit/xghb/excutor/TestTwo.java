package com.xgit.xghb.excutor;

import com.xgit.xghb.bean.TopicNameAndPartitionNumberCarrier;
import com.xgit.xghb.excutor.consumer.EnvXinXinConsumer;
import common.bean.BeanFactory;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description:
 * @author: Mr.Wang
 * @create: 2019-12-17 15:25
 **/
public class TestTwo {
    public static void main(String[] args) {
        EnvXinXinConsumer envGroupConsumer = new EnvXinXinConsumer(BeanFactory.getBean("testEnvGroupHbaseTableName"),BeanFactory.getBean("testEnvGroupHdfsPath"));
        TopicNameAndPartitionNumberCarrier testEnvGroup = BeanFactory.getBean("testEnvGroup");
        try {
            envGroupConsumer.excutorConsumer(testEnvGroup);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.err.println("集团的完毕");

        EnvXinXinConsumer envXinXinConsumer = new EnvXinXinConsumer(BeanFactory.getBean("testEnvXinXinHbaseTableName"),BeanFactory.getBean("testEnvXinXinHdfsPath"));
        TopicNameAndPartitionNumberCarrier testEnvXinXin = BeanFactory.getBean("testEnvXinXin");
        try {
            envXinXinConsumer.excutorConsumer(testEnvXinXin);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.err.println("信息公司的完毕");

        EnvXinXinConsumer envExternalConsumer = new EnvXinXinConsumer(BeanFactory.getBean("testEnvExternalHbaseTableName"),BeanFactory.getBean("testEnvExternalHdfsPath"));
        TopicNameAndPartitionNumberCarrier testEnvExternal = BeanFactory.getBean("testEnvExternal");
        try {
          // envExternalConsumer.excutorConsumer(testEnvExternal);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.err.println("外部平台的完毕");
    }
}
