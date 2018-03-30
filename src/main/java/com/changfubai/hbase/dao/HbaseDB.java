package com.changfubai.hbase.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.changfubai.hbase.dbPool.HBaseConnectionPoolImpl;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Date;

/**
 * Created by changfubai on 2018/3/29
 */
@Repository
public class HbaseDB implements HBaseDBOperator{

    private static HBaseConnectionPoolImpl hBasePool;
    private static Admin admin = null;
    private Connection connection = null;
    private static TableName tableName = null;
    private HTable hTable = null;

    static {
        tableName = TableName.valueOf(Config.DEPT);
        HBaseConnectionPoolImpl.setConf(HConstants.ZOOKEEPER_QUORUM, Config.IP_REMOTE);
        HBaseConnectionPoolImpl.setInitConnectionNum(5);
        hBasePool = HBaseConnectionPoolImpl.newInstance();
    }

    private ResultScanner results;

    public HbaseDB() {
        show();
        connection = hBasePool.getConnection();
        try {
            admin = connection.getAdmin();

            hTable = new HTable(hBasePool.getConf(), tableName);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void show() {
        hBasePool.showPool();
    }

    @Override
    public Connection getConnection() {
        return connection;
    }


    @Override
    public boolean isTableExist() {
        boolean b = false;
        try {
            b = admin.tableExists(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return b;
    }

    @Override
    public void createTable(String[] columnFamily) {

        if (isTableExist()) {
            System.out.println("该表已经存在!------" + tableName.getNameAsString());
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(tableName);
            if (columnFamily.length == 0) {
            } else {
                for (int i = 0; i < columnFamily.length; i++) {
                    descriptor.addFamily(new HColumnDescriptor(columnFamily[i]));
                }
            }
            try {
                admin.createTable(descriptor);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void deleteTable() {
        if (isTableExist()) {
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String getAllRows() {
        Scan scan = new Scan();
        try {
            results = hTable.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getAllRes(results);
    }

    @Override
    public String getSomeRows(Filter filter) {
        Scan scan = new Scan();
        scan.setFilter(filter);
        try {
            results = hTable.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getAllRes(results);
    }

    @Override
    public void addRow(String rowName, String columnFamily,
                       String column, String value) {
        Put put = new Put(rowName.getBytes());
        put.addColumn(columnFamily.getBytes(), column.getBytes(), value.getBytes());
        try {
            hTable.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void delRow(String row) {
        Delete del = new Delete(row.getBytes());
        try {
            hTable.delete(del);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delRows(String[] rows) {
        for (String row : rows) {
            delRow(row);
        }
    }

    public String getTableName() {
        return tableName.getNameAsString();
    }

    @Override
    public String getRow(String row) {
        Get get = new Get(row.getBytes());
        Result result = null;
        try {
            result = hTable.get(get);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return getRes(result);
    }

    private String getAllRes(ResultScanner results) {
        JSONArray array = new JSONArray();
        for (Result result : results) {
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                array.add(convert(cell));
            }
        }
        return array.toJSONString();
    }

    @Override
    public String getSomeRowsbyFamily(Filter filter, byte[] family) {
        Scan scan = new Scan();
        scan.setFilter(filter);
        scan.addFamily(family);
        try {
            results = hTable.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getAllRes(results);
    }

    private String getRes(Result result) {
        if (result == null) {
            return null;
        }
        Cell[] cells = result.rawCells();
        JSONArray array = new JSONArray();
        for (Cell cell : cells) {
            array.add(convert(cell));
        }
        return array.toJSONString();
    }

    private JSONObject convert(Cell cell) {
        JSONObject rowJson = new JSONObject();
        String row = new String(cell.getFamily());
        Date time = new Date(cell.getTimestamp());
        String family = new String(cell.getFamily());
        String column = new String(cell.getQualifier());
        String value = new String(cell.getValue());
        rowJson.put("row", row);
        rowJson.put("time", time.toString());
        rowJson.put("family", family);
        rowJson.put("column", column);
        rowJson.put("value", value);
        return rowJson;
    }

    public void setTableName(String tableName) {
        this.tableName = TableName.valueOf(tableName);
        try {
            hTable = new HTable(hBasePool.getConf(), tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
