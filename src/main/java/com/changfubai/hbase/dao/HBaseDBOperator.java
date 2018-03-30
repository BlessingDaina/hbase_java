package com.changfubai.hbase.dao;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.filter.Filter;

/**
 * Created by changfubai on 2018/3/30
 */
public interface HBaseDBOperator {

    boolean isTableExist();

    void createTable(String[] columnFamily);

    void deleteTable();

    void addRow(String rowName, String columnFamily, String column, String value);

    void delRow(String row);

    void delRows(String[] rows);

    String getRow(String row);

    String getAllRows();

    String getSomeRows(Filter filter);

    String getSomeRowsbyFamily(Filter filter, byte[] family);

    void show();

    Connection getConnection();

}
