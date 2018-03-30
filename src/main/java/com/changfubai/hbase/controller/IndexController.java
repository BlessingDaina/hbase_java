package com.changfubai.hbase.controller;

import com.changfubai.hbase.dao.HbaseDB;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by changfubai on 2018/3/29
 */
@RestController
public class IndexController {

    @Autowired
    HbaseDB hbaseDB;

    @RequestMapping("/")
    public String index() {
        String res = hbaseDB.getAllRows();

        System.out.println(res);
        return res;
    }

    @RequestMapping("/insert")
    public void insert() {
        String tableName = "dept1";
        String[] columnFamilys = {"base", "subdept"};
        hbaseDB.setTableName(tableName);
        hbaseDB.createTable(columnFamilys);
        if (hbaseDB.isTableExist()) {
            hbaseDB.addRow("0_001", "base", "name", "network");
            hbaseDB.addRow("1_001", "base", "name", "developer");
            hbaseDB.addRow("1_002", "base", "name", "test");
            hbaseDB.addRow("1_001", "base", "f_pid", "0_001");
            hbaseDB.addRow("1_002", "base", "f_pid", "0_002");
            hbaseDB.addRow("2_001", "base", "name", "developer01");
            hbaseDB.addRow("2_002", "base", "name", "developer02");
            hbaseDB.addRow("2_003", "base", "name", "developer03");
            hbaseDB.addRow("2_001", "base", "f_pid", "1_001");
            hbaseDB.addRow("2_002", "base", "f_pid", "1_001");
            hbaseDB.addRow("2_003", "base", "f_pid", "1_001");

            for (int i = 4; i < 200; i++) {
                String now = StringUtils.leftPad("" + i, 3, "0");
                hbaseDB.addRow("2_" + now, "base", "name", "test" + now);
                hbaseDB.addRow("2_" + now, "base", "f_pid", "1_002");
                hbaseDB.addRow("1_002", "subdept", "2_" + now, "test" + now);
            }

            hbaseDB.addRow("0_001", "subdept", "1_001", "developer");
            hbaseDB.addRow("0_001", "subdept", "1_002", "test");
            hbaseDB.addRow("1_001", "subdept", "2_001", "developer01");
            hbaseDB.addRow("1_001", "subdept", "2_002", "developer02");
            hbaseDB.addRow("1_001", "subdept", "2_003", "developer03");

            //	查询所有一级部门(没有上级部门的部门)
            System.out.println("查询所有一级部门(没有上级部门的部门)");
            List<Filter> filters = new ArrayList<>();
            filters.add(new PrefixFilter("0".getBytes()));
            filters.add(new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("name"))));
            FilterList filterList = new FilterList(filters);
            hbaseDB.getSomeRows(filterList);

            //	已知rowkey，查询该部门的所有(直接)子部门信息
            System.out.println("已知rowkey，查询该部门的所有(直接)子部门信息");
            filters.clear();
            filters.add(new PrefixFilter("1_001".getBytes()));
            filterList = new FilterList(filters);
            String res = hbaseDB.getSomeRowsbyFamily(filterList, "subdept".getBytes());
            System.out.println(res);

            //	已知rowkey，向该部门增加一个子部门
            hbaseDB.addRow("1_002", "subdept", "2_001", "developer01");

            //	已知rowkey（且该部门存在子部门），删除该部门信息，该部门所有(直接)子部门被调整到其他部门中
            System.out.println("已知rowkey（且该部门存在子部门），删除该部门信息，该部门所有(直接)子部门被调整到其他部门中");
            filters.clear();
            filters.add(new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("1_001"))));
            filterList = new FilterList(filters);
            res = hbaseDB.getSomeRowsbyFamily(filterList, "subdept".getBytes());
            System.out.println(res);
            hbaseDB.delRow("1_001");
        } else {
            System.out.println(tableName + "此数据库表不存在！");
        }
    }

    @RequestMapping("/delete")
    public String delete(@RequestParam("name") String name) {
        hbaseDB.setTableName(name);
        hbaseDB.deleteTable();
        return hbaseDB.isTableExist() ? "删除失败！" : "删除成功！";
    }
    @RequestMapping("/add")
    public String add(@RequestParam("name") String name) {
        hbaseDB.setTableName(name);
        hbaseDB.createTable(new String[]{"top", "sub"});
        return hbaseDB.isTableExist() ? "添加成功！" : "添加失败！";
    }

}
