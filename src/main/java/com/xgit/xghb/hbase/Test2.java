package com.xgit.xghb.hbase;

import com.xgit.xghb.bean.Body;
import com.xgit.xghb.bean.ForwardingCarrier;
import com.xgit.xghb.hbase.writer.ForwardingCarrierHbaseWriter;
import common.bean.BeanFactory;

import java.util.ArrayList;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description:
 * @author: Mr.Wang
 * @create: 2019-12-18 15:41
 **/
public class Test2 {
    public static void main(String[] args) {
        ForwardingCarrierHbaseWriter xinXinForwardingCarrierHbaseWriter = new ForwardingCarrierHbaseWriter(BeanFactory.getBean("testEnvXinXinHdfsPath"));
        String testEnvXinXinHbaseTableName = (String) BeanFactory.getBean("testEnvXinXinHbaseTableName");

        ArrayList<ForwardingCarrier> forwardingCarriers = new ArrayList<>();
        forwardingCarriers.add(getForwardingCarrier());
        forwardingCarriers.add(getForwardingCarrier());
        try {
            xinXinForwardingCarrierHbaseWriter.write(forwardingCarriers);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ForwardingCarrier getForwardingCarrier() {
        ForwardingCarrier forwardingCarrier = new ForwardingCarrier();
        Body body = new Body();
        body.setZDID("testzdid123456");
        body.setJD("123");
        body.setCS("789");
        body.setSJCJSCJ("2019-12-17 09:52:35");
        body.setSIMID("6666");
        Body body2 = new Body();
        body2.setJD("12378");
        body2.setCS("7899679");
        body2.setZDID("testzdid7789621");
        body2.setSJCJSCJ("2019-12-17 09:52:36");
        body2.setSIMID("66665979");
        ArrayList<Body> bodies = new ArrayList<>();
        bodies.add(body);
        bodies.add(body2);
        forwardingCarrier.setBody(bodies);
        forwardingCarrier.setExchangeType("1.0");
        forwardingCarrier.setVersion("3.999");
        forwardingCarrier.setExchangeCode("efwerwerwer");
        forwardingCarrier.setRequestTime("2019-09-11 12:33:56");
        return forwardingCarrier;
    }
}
