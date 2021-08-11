package com.jxt.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @title: HbaseDemo
 * @Author Tang Xiaojiang
 * @Date: 2021/8/10 23:33
 * @Version 1.0
 */
public class HbaseDemo_1 {
    public static Configuration conf;
    public static Connection connection = null;
    public static Admin admin = null;

    static {
        try {
            // 获取配置信息
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "hdp03,hdp04,hdp05");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            // 创建连接对象
            connection = ConnectionFactory.createConnection(conf);
            // 创建Admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    /**
     * 创建表
     *
     * @param tableName
     * @param cfs
     * @throws IOException
     */
    public static void createTable(String tableName, String... cfs) throws IOException {
        // 判断是否有列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }
        // 判断表是否存在
        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已经存在！");
            return;
        }
        // 创建表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 循环添加列族信息
        for (String cf : cfs) {
            // 创建列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            // 添加具体的列族
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        // 创建表
        admin.createTable(hTableDescriptor);

    }

    /**
     * 删除表
     *
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {
        // 判断表是否存在
        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在");
            return;
        }
        // 下线表
        admin.disableTable(TableName.valueOf(tableName));
        // 删除表
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 创建命名空间
     *
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace) {
        // 创建命名空间描述器
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
        // 创建命名空间
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println("命名空间" + nameSpace + "已经存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     *
     * @param tableName
     * @param rowKey
     * @param cf
     * @param cn
     * @param value
     */
    public static void putData(String tableName, String rowKey, String cf, String cn, String value) throws IOException {
        // 获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));
        // 创建put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        // 给put对象赋值
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        // 插入数据
        table.put(put);
        // 关闭链接
        table.close();
    }

    /**
     * 关闭资源
     *
     * @throws IOException
     */
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
/*        boolean tableExist = isTableExist("stu6");
        System.out.println(tableExist);*/
        // 创建表
        System.out.println(isTableExist("stu7"));
        createTable("0810:stu7", "info1", "info2");
        System.out.println(isTableExist("0810:stu7"));
        // 删除表
        /*dropTable("stu6");
        System.out.println(isTableExist("stu6"));*/
        // 创建命名空间
//        createNameSpace("0810");
        // 关闭
        close();
    }
}
