package com.xgit.xghb.hbase;

import com.xgit.xghb.bean.Body;
import com.xgit.xghb.bean.ForwardingCarrier;
import common.bean.BeanFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 测试写入 hbase
 * @author: Mr.Wang
 * @create: 2019-12-18 08:52
 **/
public class TestWriteToHbase {

    public static void main(String[] args) {
        Configuration config = new Configuration();
        config.addResource("core-site.xml");
        config.addResource("hdfs-site.xml");
        config.addResource("hbase-site.xml");
        Configuration conf = HBaseConfiguration.addHbaseResources(config);
        Connection conn = null;
        Admin admin = null;
        TableName tableName = TableName.valueOf((String) BeanFactory.getBean("testEnvXinXinHbaseTableName"));
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
            // TODO 获取配置的hbase 表名

            // 如果表中不存在 表
            if (!admin.tableExists(tableName)) {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("1"));
                hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }
            Table table = conn.getTable(tableName);
            ForwardingCarrier forwardingCarrier = getForwardingCarrier();
            List<Body> bodys = forwardingCarrier.getBody();
            // 新建ListPut 保存Put
            ArrayList<Put> puts = new ArrayList<>();
            for (Body body : bodys) {
                // 2019-12-17 09:52:35 这种格式
                String sjcjscj = body.getSJCJSCJ();
                StringBuffer stringBuffer = new StringBuffer(sjcjscj);
                stringBuffer.deleteCharAt(4);
                stringBuffer.deleteCharAt(6);
                stringBuffer.deleteCharAt(8);
                stringBuffer.deleteCharAt(10);
                stringBuffer.deleteCharAt(12);
                Long backwardsTime =Long.MAX_VALUE-Long.valueOf(stringBuffer.toString());
                String zdid = body.getZDID();
                String rowKey = zdid + backwardsTime;
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("exchangeType"), Bytes.toBytes(forwardingCarrier.getExchangeType()));
                put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("exchangeCode"), Bytes.toBytes(forwardingCarrier.getExchangeCode()));
                put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("requestTime"), Bytes.toBytes(forwardingCarrier.getRequestTime()));
                put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("version"), Bytes.toBytes(forwardingCarrier.getVersion()));
                if (null != body.getSJCJSCJ()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("sjcjsj"), Bytes.toBytes(body.getSJCJSCJ()));
                }
                if (null != body.getHbclId()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("hbclId"), Bytes.toBytes(body.getHbclId()));
                }
                if (null != body.getJXHBXH()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("jxhbxh"), Bytes.toBytes(body.getJXHBXH()));
                }
                if (null != body.getSIMID()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("simid"), Bytes.toBytes(body.getSIMID()));
                }
                if (null != body.getZDID()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("zdid"), Bytes.toBytes(body.getZDID()));
                }
                if (null != body.getDWZT()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("dwzt"), Bytes.toBytes(body.getDWZT()));
                }
                if (null != body.getJD()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("jd"), Bytes.toBytes(body.getJD()));
                }
                if (null != body.getWD()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("wd"), Bytes.toBytes(body.getWD()));
                }
                if (null != body.getHB()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("hb"), Bytes.toBytes(body.getHB()));
                }
                if (null != body.getCS()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("cs"), Bytes.toBytes(body.getCS()));
                }
                if (null != body.getACCZT()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("acczt"), Bytes.toBytes(body.getACCZT()));
                }
                if (null != body.getACCLJGZSC()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("accljgzsc"), Bytes.toBytes(body.getACCLJGZSC()));
                }
                if (null != body.getBJZHXX()) {
                    put.addColumn(Bytes.toBytes("1"), Bytes.toBytes("bjzhxx"), Bytes.toBytes(body.getBJZHXX()));
                }
                puts.add(put);
            }
            table.put(puts);
            IOUtils.closeQuietly(table);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
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
