package com.alibaba.datax.plugin.reader.impalareader;

import java.sql.*;
import java.util.List;
import java.util.Map;

/**
 * @className: TestJdbc
 * @Authors: yuguo
 * @Date: 2019/9/27
 * @Description:
 **/
public class TestJdbc {


    public final static String IMPALA_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";
    public final static String URL = "jdbc:impala://192.168.239.102:21050/yuguo_test";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {


        ImpalaJdbc jdbc = new ImpalaJdbc(URL);

        List<Map<String, Object>> maps = jdbc.executeQuery("select * from dim_channel");

        for (Map<String, Object> item : maps) {
            System.out.println(item);
        }

    }


}
