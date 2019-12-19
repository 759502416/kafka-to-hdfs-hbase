package com.xgit.xghb.hdfs.writer;

import com.xgit.xghb.bean.Body;
import com.xgit.xghb.bean.ForwardingCarrier;
import com.xgit.xghb.hdfs.AbstractHdfsWriter;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: hdfs写入执行者  针对ForwardingCarrier 对象
 * @author: Mr.Wang
 * @create: 2019-12-17 14:52
 **/
public class ForwardingCarrierHdfsWriter extends AbstractHdfsWriter<ForwardingCarrier> {

    /**
     * 必须要实现
     *
     * @param filePathPrefix 文件保存路径
     */
    public ForwardingCarrierHdfsWriter(String filePathPrefix) {
        super(filePathPrefix);
    }

    @Override
    protected void setFsAndavroSchema() {
        if (null == this.fs) {
            Configuration config = new Configuration();
            config.addResource("core-site.xml");
            config.addResource("hdfs-site.xml");
            try {
                // 设置文件系统对象
                this.fs = FileSystem.get(config);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null == this.avroSchema) {
            /**
             * 初始化avro格式信息
             */
            StringBuffer stringBuffer = new StringBuffer();
            stringBuffer.append("{\"type\": \"record\",\"name\": \"ForwardingCarrier\", \"fields\":  ");
            stringBuffer.append("[ ");
            stringBuffer.append("{\"name\":\"exchangetype\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"exchangecode\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"requesttime\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"version\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"sjcjsj\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"hbclId\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"jxhbxh\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"simid\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"zdid\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"dwzt\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"jd\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"wd\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"hb\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"cs\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"acczt\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"accljgzsc\",\"type\":[\"string\",\"null\"]}, ");
            stringBuffer.append("{\"name\":\"bjzhxx\",\"type\":[\"string\",\"null\"]} ");
            stringBuffer.append("] ");
            stringBuffer.append("} ");
            // 设置格式对象
            this.avroSchema = new Schema.Parser().parse(stringBuffer.toString());
        }

    }

    /**
     * 执行写入对象
     *
     * @param filePath 文件路径
     * @param object   消费对象
     */
    @Override
    protected void write(String filePath, Object object) {
        // 强转为 List<ForwardingCarrier>
        List<ForwardingCarrier> forwardingCarriers = (List<ForwardingCarrier>) object;
        if (forwardingCarriers.size() < 1) {
            logger.info("没有收到要写入到hdfs的对象信息");
            return;
        }
        String requestTime = forwardingCarriers.get(0).getRequestTime();
        DataFileWriter<Object> writer = null;
        try {
            // 获得写入对象
            writer = getWriter(filePath);
            for (ForwardingCarrier forwardingCarrier : forwardingCarriers) {
                for (Body body : forwardingCarrier.getBody()) {
                    GenericData.Record record = new GenericData.Record(this.avroSchema);
                    record.put("exchangetype", forwardingCarrier.getExchangeType());
                    record.put("exchangecode", forwardingCarrier.getExchangeCode());
                    record.put("requesttime", forwardingCarrier.getRequestTime());
                    record.put("version", forwardingCarrier.getVersion());
                    record.put("sjcjsj", body.getSJCJSCJ());
                    record.put("hbclId", body.getHbclId());
                    record.put("jxhbxh", body.getJXHBXH());
                    record.put("simid", body.getSIMID());
                    record.put("zdid", body.getZDID());
                    record.put("dwzt", body.getDWZT());
                    record.put("jd", body.getJD());
                    record.put("wd", body.getWD());
                    record.put("hb", body.getHB());
                    record.put("cs", body.getCS());
                    record.put("acczt", body.getACCZT());
                    record.put("accljgzsc", body.getACCLJGZSC());
                    record.put("bjzhxx", body.getBJZHXX());
                    // 添加进dataWriter
                    try {
                        writer.append(record);
                    } catch (IOException e) {

                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
