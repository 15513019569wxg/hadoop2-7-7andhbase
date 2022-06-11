package com.capensis;/*
    @author wxg
    @date 2021/7/14-23:58
    */

import java.sql.*;

public class Test {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        //  1、定义参数
        String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
        String url = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";
        //  2、加载驱动
        Class.forName(driver);
        //  3、创建连接
        Connection connection = DriverManager.getConnection(url);
        System.out.println(connection);
        //  4、预编译sql
        PreparedStatement preparedStatement = connection.prepareStatement(" select *  from poputation");
        //  5、执行
        ResultSet resultSet = preparedStatement.executeQuery();
        //  6、打印
        while(resultSet.next()){
            System.out.println(resultSet.getString(1) + '\t' + resultSet.getString(2) + resultSet.getString(3));
        }
        //  7、关闭资源
        resultSet.close();
        preparedStatement.close();
        connection.close();
    }
}
