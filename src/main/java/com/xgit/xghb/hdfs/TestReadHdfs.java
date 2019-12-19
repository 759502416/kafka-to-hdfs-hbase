package com.xgit.xghb.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 测试从hdfs中读取数据
 * @author: Mr.Wang
 * @create: 2019-12-16 14:54
 **/
public class TestReadHdfs {

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

    public static void main(String[] args) {
        try {
            FsInput fsInput = new FsInput(new Path(URI.create("/testEnv/test.avro")), fs);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
            FileReader<GenericRecord> fileReader = DataFileReader.openReader(fsInput, reader);
            while (fileReader.hasNext()){
                GenericRecord next = fileReader.next();
                System.err.println(next);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
