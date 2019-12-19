package com.xgit.xghb.hbase.writer;

import com.xgit.xghb.bean.Body;
import com.xgit.xghb.bean.ForwardingCarrier;
import com.xgit.xghb.hbase.AbstractHbaseWriter;
import common.bean.BeanFactory;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: Hbase 写入执行者，针对ForwardingCarrier对象
 * @author: Mr.Wang
 * @create: 2019-12-17 14:46
 **/
public class ForwardingCarrierHbaseWriter extends AbstractHbaseWriter<ForwardingCarrier> {

    /**
     * 必须要有写入表名
     *
     * @param tableName
     */
    public ForwardingCarrierHbaseWriter(String tableName) {
        super(tableName);
    }

    @Override
    protected void insertIntoHbase(List<ForwardingCarrier> forwardingCarriers) {
        Table table = null;
        try {
            table = conn.getTable(this.tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (ForwardingCarrier forwardingCarrier : forwardingCarriers){
            if(null == forwardingCarrier || null == forwardingCarrier.getBody()){
                continue;
            }
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
                Long backwardsTime = Long.MAX_VALUE - Long.valueOf(stringBuffer.toString());
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
            try {
                table.put(puts);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        IOUtils.closeQuietly(table);
    }
}
