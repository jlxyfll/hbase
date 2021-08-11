package com.jxt.utils;

import com.jxt.constants.Constants;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Arrays;

public class HBaseUtil {

    /**
     * 创建命名空间
     *
     * @param nameSpace 命名空间
     */
    public static void createNameSpace(String nameSpace) {
        // 获取connection对象
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
            // 获取Admin对象
            admin = connection.getAdmin();
            // 购建命名空间描述器
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();
            // 创建命名空间
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭资源
                assert admin != null;
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     * @return 是否存在
     */
    private static boolean isExist(String tableName) {
        // 获取connection对象
        Connection connection = null;
        Admin admin = null;
        boolean isExist = false;
        try {
            connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
            // 获取Admin对象
            admin = connection.getAdmin();
            isExist = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭资源
                assert admin != null;
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return isExist;
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     */
    private static void createTable(String tableName, int versions, String[] cfs) {
        // 判断是否有列族信息
        if (cfs.length <= 0) {
            System.out.println("请设置列族信息！");
            return;
        }
        // 判断表是否已经存在
        if (isExist(tableName)) {
            System.out.println("表:" + tableName + "已经存在！");
            return;
        }
        // 获取connection对象
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(Constants.CONFIGURATION);
            // 获取Admin对象
            admin = connection.getAdmin();
            // 购建表描述器
            //HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
            // 循环添加列族信息
            /*for (String cf : cfs) {
                // 列族描述器
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                // 设置版本
                hColumnDescriptor.setMaxVersions(versions);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }*/
            Arrays.stream(cfs).forEach(cf -> {
                ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.of(cf);
                ColumnFamilyDescriptorBuilder.newBuilder(columnFamilyDescriptor).setMaxVersions(versions);
                tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
            });
            // 创建表操作
            //admin.createTable(hTableDescriptor);
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                // 关闭资源
                assert admin != null;
                admin.close();
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
