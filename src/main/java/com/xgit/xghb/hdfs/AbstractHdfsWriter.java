package com.xgit.xghb.hdfs;

import com.xgit.xghb.bean.ForwardingCarrier;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 抽象的写入hdfs
 * @author: Mr.Wang
 * @create: 2019-12-17 08:48
 **/
public abstract class AbstractHdfsWriter<T> {

    /**
     * 设置日志对象
     */
    protected Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * avro的格式信息
     */
    protected Schema avroSchema;

    /**
     * hadoop 文件系统对象
     */
    protected FileSystem fs;

    /**
     * 存储文件路径前缀
     */
    protected String filePathPrefix;

    /**
     * 设置FileSystem文件系统 和 avro文件格式
     */
    protected abstract void setFsAndavroSchema();

    /**
     * 设置 存储文件路径前缀
     * @param filePathPrefix
     */
    public void setFilePathPrefix(String filePathPrefix) {
        this.filePathPrefix = filePathPrefix;
    }

    /**
     *  获得 存储文件路径前缀
     * @return
     */
    public String getFilePathPrefix() {
        return filePathPrefix;
    }

    /**
     * 必须有文件保存路径
     * @param filePathPrefix 文件保存路径
     */
    public AbstractHdfsWriter(String filePathPrefix) {
        this.filePathPrefix = filePathPrefix;
    }

    /**
     * hdfs的写入执行
     * @param filePath 文件路径
     * @param t 需要写入的对象
     */
    protected abstract void write(String filePath, Object t);


    /**
     * hdfs 的供调用执行对象
     * @param filePath
     * @param t
     */
    public void excuteWrite(String filePath, Object t) throws Exception{
        setFsAndavroSchema();
        if(null == fs || null == avroSchema || null == filePathPrefix){
            throw new Exception("请先设置FileSystem文件系统 和 avro文件格式 和文件存储路径前缀");
        }
        // 调用写入方法
        write(filePath,t);
    }

    /**
     * 创建 Hdfs 文件
     * @param path 文件路径
     * @return
     */
    protected boolean makeHdfsFile(String path) {
        boolean newFile = false;
        try {
            newFile = fs.createNewFile(new Path(URI.create(path)));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return newFile;
        }
    }

    /**
     * 获得Hdfs文件写入对象
     *
     * @param path 文件路径
     * @return
     * @throws IOException
     */
    protected DataFileWriter<Object> getWriter(String path){
        DataFileWriter<Object> writer = new DataFileWriter<>(
                new GenericDatumWriter<>());
        writer.setCodec(CodecFactory.snappyCodec());
        Path path1 = new Path(path);
        OutputStream os;
        try {
            if (!fs.exists(path1)) {
                os = fs.create(path1);
                writer.create(avroSchema, os);
            } else {
                os = fs.append(path1);
                writer.appendTo(new AvroFSInput(fs.open(path1), 1024), os);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return writer;
    }



    /**
     * 获取文件路径最后修改时间
     *
     * @param path
     * @return
     */
    public String getModificationTime(String path) {
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
     * 获取文件大小
     *
     * @param path 文件路径
     * @return
     */
    public long getPathSize(String path) {
        long size = -1L;
        try {
            Path hdfsPath = new Path(path);
            size = fs.getContentSummary(hdfsPath).getLength();
        } catch (Exception e) {
            logger.error("[HDFS]获取路径大小失败", e);
        }
        return size;
    }

    /**
     * 判断是否存在该文件
     *
     * @param path 文件路径
     * @return
     */
    public  boolean exists(String path) {
        boolean isExists = true;
        try {
            Path hdfsPath = new Path(path);
            isExists = fs.exists(hdfsPath);
        } catch (Exception e) {
            logger.error("[HDFS]判断文件是否存在失败", e);
        }
        return isExists;
    }
}
