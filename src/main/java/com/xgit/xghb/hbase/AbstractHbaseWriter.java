package com.xgit.xghb.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @program: FromKafkaTopicWriteHdfsAndHbase
 * @description: 抽象地写入Hbase
 * @author: Mr.Wang
 * @create: 2019-12-17 08:49
 **/
public abstract class AbstractHbaseWriter<T> {
    /**
     * 必须要有写入表名
     * @param tableName
     */
    public AbstractHbaseWriter(String tableName) {
        this.tableName = TableName.valueOf(tableName);
    }

    /**
     * 获得日志对象
     */
    Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Hbase连接
     */
    protected Connection conn = null;

    /**
     * Hbase管理者
     */
    protected Admin admin = null;

    /**
     * 表名
     */
    protected TableName tableName = null;


    /**
     * habse的写入执行
     * 1.先加载Hbase配置，创建admin 和 conn对象
     * 2.设置表名
     * 2.检查表名
     * 3.执行写入操作
     * 4.如果写入失败，线程休眠10秒后，再次进行尝试
     */
    public void write(List<T> ts) throws Exception {
        loadHbaseConfig();
        if(null == this.tableName){
            throw new Exception("请设置要写入的表名!");
        }
        veriTyTableName();
        try {
            insertIntoHbase(ts);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("数据处理失败，10秒后再次进行尝试");
            Thread.sleep(10000);
            write(ts);
        }
        logger.info("批量数据处理完成");
    }

    /**
     * 加载Hbase 配置，设置Hbase连接和Hbase管理者
     */
    protected void loadHbaseConfig(){
        Configuration config = new Configuration();
        config.addResource("core-site.xml");
        config.addResource("hdfs-site.xml");
        config.addResource("hbase-site.xml");
        Configuration conf = HBaseConfiguration.addHbaseResources(config);
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }

    /**
     * 如果不存在该Hbase表，则创建
     * 如果存在该Hbase表，但不存在family为"1",抛出异常
     */
    protected void veriTyTableName() throws Exception{
        // 如果表中不存在 表
        try {
            if (!admin.tableExists(tableName)) {
                logger.info("Hbase不存在表{},新建该表，列簇为'1'，压缩格式为GZ",tableName.toString());
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("1"));
                hColumnDescriptor.setCompressionType(Compression.Algorithm.GZ);
                hTableDescriptor.addFamily(hColumnDescriptor);
                admin.createTable(hTableDescriptor);
            }else{
                HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);
                Set<byte[]> familiesKeys = tableDescriptor.getFamiliesKeys();
                if(!familiesKeys.contains(Bytes.toBytes("1"))){
                    throw new Exception("不存在列簇为‘1’,不允许进行操作");
                }else {
                    logger.info("存在该表{},亦存在列簇为‘1’",tableName.toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(null!=admin){
                admin.close();
            }
        }
    }

    /**
     * 执行插入数据操作
     */
    protected abstract void insertIntoHbase(List<T> t);
}
