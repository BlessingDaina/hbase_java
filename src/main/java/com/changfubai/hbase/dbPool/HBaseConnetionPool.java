package com.changfubai.hbase.dbPool;

import org.apache.hadoop.hbase.client.Connection;

/**
 * Created by changfubai on 2018/3/30
 */
interface HBaseConnetionPool {

    Connection getConnection();

    void setMaxConnectionNum(int num);

    void setIncreConnectionNum(int num);

    void freeConnection(Connection connection);

    void showPool();


}
