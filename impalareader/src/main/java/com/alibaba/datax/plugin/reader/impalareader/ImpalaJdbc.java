package com.alibaba.datax.plugin.reader.impalareader;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @className: ImpalaJdbc
 * @Authors: yuguo
 * @Date: 2019/9/27
 * @Description:
 **/
public class ImpalaJdbc {

    private final static String IMPALA_DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

    public String jdbcUrl;

    public ImpalaJdbc(String jdbcUrl) {

        this.jdbcUrl = jdbcUrl;
    }

    private Connection getConnect() {

        Connection connection = null;
        try {
            Class.forName(IMPALA_DRIVER_NAME);
            connection = DriverManager.getConnection(this.jdbcUrl);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public List<Map<String, Object>> executeQuery(String sql) {
        List<Map<String, Object>> rows = new ArrayList<>();
        Connection connect = getConnect();
        PreparedStatement prep = null;
        ResultSet set = null;
        try {
            prep = connect.prepareStatement(sql);
            set = prep.executeQuery();
            int columnCount = set.getMetaData().getColumnCount();

            while (set.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object o = set.getObject(i);
                    String name = set.getMetaData().getColumnName(i);
                    row.put(name, o);
                }
                rows.add(row);
            }

            return rows;

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                set.close();
                prep.close();
                connect.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

        }

        return rows;
    }


}
