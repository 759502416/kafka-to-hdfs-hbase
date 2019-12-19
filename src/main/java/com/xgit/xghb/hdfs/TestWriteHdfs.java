package com.xgit.xghb.hdfs;

import com.xgit.xghb.bean.Body;
import com.xgit.xghb.bean.ForwardingCarrier;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import static org.apache.avro.data.Json.SCHEMA;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 测试写入hdfs
 * @author: Mr.Wang
 * @create: 2019-12-16 09:25
 **/
public class TestWriteHdfs {
    /**
     * 日志信息
     */
    static Logger logger = LoggerFactory.getLogger(TestWriteHdfs.class);

    /**
     * avro 格式
     */
    static Schema avroSchema;

    /**
     * 文件操作系统
     */
    public static FileSystem fs;

    static {
        Configuration config = new Configuration();
        config.addResource("core-site.xml");
        config.addResource("hdfs-site.xml");
        try {
            fs = FileSystem.get(config);
        } catch (IOException e) {
            e.printStackTrace();
        }

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
        avroSchema = new Schema.Parser().parse(stringBuffer.toString());
    }

    /**
     * 1. 先在hdfs根目录下创建了 /testEnv 目录
     * 2.
     *
     * @param args
     */
    public static void main(String[] args) {
        try {
            /*boolean exists = exists("/testEnv");
            System.err.println(exists);
            String modificationTime = getModificationTime("/testEnv");
            System.err.println(modificationTime);
            long pathSize = getPathSize("/app-logs/ambari-qa/logs-ifile/application_1573636320188_0001");
            System.err.println(pathSize);*/
            //boolean b = makeHdfsFile("/testEnv/test.avro");
            //System.err.println(b);
            whx("/testEnv/test.avro", getForwardingCarrier());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void whx(String avroFilepath, ForwardingCarrier forwardingCarrier) {
        DataFileWriter<Object> writer = null;
        try {
            writer = getWriter(avroFilepath);
             /*
               创建 Avro
             */
            for (Body body : forwardingCarrier.getBody()) {
                GenericData.Record record = new GenericData.Record(avroSchema);
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
                writer.append(record);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * 判断是否存在文件
     *
     * @param path
     * @return
     */
    public static boolean exists(String path) {
        boolean isExists = true;
        try {
            Path hdfsPath = new Path(path);
            isExists = fs.exists(hdfsPath);
        } catch (Exception e) {
            logger.error("[HDFS]判断文件是否存在失败", e);
        }
        return isExists;
    }

    /**
     * 获取文件路径最后修改时间
     *
     * @param path
     * @return
     */
    public static String getModificationTime(String path) {
        String modifyTime = null;
        try {
            Path hdfsPath = new Path(path);
            FileStatus fileStatus = fs.getFileStatus(hdfsPath);
            long modifyTimestamp = fileStatus.getModificationTime();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = new Date(modifyTimestamp);
            modifyTime = simpleDateFormat.format(date);
        } catch (Exception e) {
            logger.error("[HDFS]获取最近修改时间失败", e);
        }
        return modifyTime;
    }

    /**
     * 获取文件路径大小
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static long getPathSize(String path) {
        long size = -1L;
        try {
            Path hdfsPath = new Path(path);
            size = fs.getContentSummary(hdfsPath).getLength();
        } catch (Exception e) {
            logger.error("[HDFS]获取路径大小失败", e);
        }
        return size;
    }

    public static boolean makeHdfsFile(String path) {
        boolean newFile = false;
        try {
            newFile = fs.createNewFile(new Path(URI.create(path)));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return newFile;
        }
    }


    public static ForwardingCarrier getForwardingCarrier() {
        ForwardingCarrier forwardingCarrier = new ForwardingCarrier();
        Body body = new Body();
        body.setJD("123");
        body.setCS("789");

        body.setSIMID("6666");
        Body body2 = new Body();
        body2.setJD("12378");
        body2.setCS("7899679");
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


    /**
     * HDFS文件写入
     */
    public static DataFileWriter<Object> getWriter(String path) throws IOException {
        DataFileWriter<Object> writer = new DataFileWriter<>(
                new GenericDatumWriter<>());
        writer.setCodec(CodecFactory.snappyCodec());
        Path path1 = new Path(path);
        OutputStream os;
        if (!fs.exists(path1)) {
            os = fs.create(path1);
            writer.create(avroSchema, os);
        } else {
            os = fs.append(path1);
            writer.appendTo(new AvroFSInput(fs.open(path1), 1024), os);
        }
        return writer;
    }
}
