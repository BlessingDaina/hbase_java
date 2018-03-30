package com.changfubai.hbase.dbPool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.Vector;

/**
 * Created by changfubai on 2018/3/30
 */

public class HBaseConnectionPoolImpl implements HBaseConnetionPool{

    private static int initConnectionNum = 0;
    private int maxConnectionNum = 0;
    private int increConnectionNum = 2;
    private static Configuration conf = null;
    private Vector pools = null;
    private Vector poolsUsed = null;
    private static HBaseConnectionPoolImpl hBaseConnectionPool = null;
    private int size = 0;

    static {

        initConnectionNum = 10;
        conf = HBaseConfiguration.create();

    }

    private HBaseConnectionPoolImpl() {
        pools = new Vector(initConnectionNum);
        poolsUsed = new Vector(initConnectionNum);
        createConnectionPool(initConnectionNum);
        System.out.println("数据库连接池初始化完成");
    }

    public static HBaseConnectionPoolImpl newInstance() {

        if (hBaseConnectionPool == null) {
            hBaseConnectionPool = new HBaseConnectionPoolImpl();
        }
        return hBaseConnectionPool;

    }

    public static void setInitConnectionNum(int initNum) {
        initConnectionNum = initNum;
    }

    public static void setConf(String name, String value) {
        conf.set(name, value);
    }

    public Configuration getConf() {
        return conf;
    }

    private synchronized void createConnectionPool(int numConnections) {
        for (int i = 0; i < numConnections; i++) {
            try {
                if (this.maxConnectionNum > 0 &&
                        size >= this.maxConnectionNum) {
                    break;
                }

                pools.addElement(ConnectionFactory.createConnection(conf));
                size++;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Connection getConnection(){
        if (pools == null) return null;
        Connection conn = getFreeConnection();
        while (conn == null) {
            try {
                wait(250);
                conn = getFreeConnection();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return conn;
    }

    private Connection getFreeConnection() {
        Connection conn = findFreeConnection();
        if (conn == null) {
            createConnectionPool(increConnectionNum);
            System.out.println("当前连接池数量为：————" + size);
            conn = findFreeConnection();
        }
        return conn;
    }

    private synchronized Connection findFreeConnection() {
        if (pools.size() > 0) {
            Connection conn = (Connection) pools.remove(0);
            poolsUsed.addElement(conn);
            return conn;
        }
        return null;
    }

    @Override
    public void setMaxConnectionNum(int num) {
        if (num >= 0) {
            this.maxConnectionNum = num;
        }

    }

    @Override
    public void setIncreConnectionNum(int num) {
        this.increConnectionNum = num;
    }

    @Override
    public synchronized void freeConnection(Connection connection) {
        poolsUsed.remove(connection);
        pools.addElement(connection);
    }

    @Override
    public void showPool() {
        System.out.println("欢迎使用小白的自定义数据库连接池-------------");
        System.out.println("当前数据库连接池共有连接： " + size);
        System.out.println("----------------可用连接： " + pools.size());
        System.out.println("----------------已用连接： " + poolsUsed.size());
        System.out.println("----------------最大连接： " + (this.maxConnectionNum == 0 ? Integer.MAX_VALUE : this.maxConnectionNum));
        System.out.println("----------------增长步长： " + this.increConnectionNum);
    }


}
