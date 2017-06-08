package org.exhibitor.reports;

import java.io.*;
import java.net.URLClassLoader;
import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.net.URL;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.ArrayList;

/**
 * Created by fpombeiro on 6/7/17.
 */
public class Redshift {
    private String dbURL;
    private String MasterUsername;
    private String MasterUserPassword;

    public static Connection dbconnect(String dbURL, String MasterUsername, String MasterUserPassword) {
        Connection conn = null;
        Statement stmt = null;
        try {
            //Dynamically load driver at runtime.
            //Redshift JDBC 4.1 driver: com.amazon.redshift.jdbc41.Driver
            //Redshift JDBC 4 driver: com.amazon.redshift.jdbc4.Driver
            Class.forName("com.amazon.redshift.jdbc42.Driver");

            //Open a connection and define properties.
            System.out.println("Connecting to database...");
            Properties props = new Properties();

            //Uncomment the following line if using a keystore.
            //props.setProperty("ssl", "true");
            props.setProperty("user", MasterUsername);
            props.setProperty("password", MasterUserPassword);
            conn = DriverManager.getConnection(dbURL, props);
        } catch (Exception ex) {
            //For convenience, handle all errors here.
            ex.printStackTrace();
        }
        return conn;
    }


    public static String readFile(String directoryPath, String reportName) {

        StringBuilder sqlStatement = new StringBuilder("");
        final String FILENAME = directoryPath + "/" + reportName;
        System.out.println(FILENAME);
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cl).getURLs();
        for(URL url: urls){
            System.out.println(url.getFile());
        }
        File file = new File(cl.getResource(FILENAME).getFile());
        try {
            Scanner scanner = new Scanner(file);
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                sqlStatement.append(line);
            }

            scanner.close();

        }
        catch (IOException e) {
            e.printStackTrace();
        }

        return sqlStatement.toString();

    }

    public static PreparedStatement prepStatement(String query, Connection conn, String[] args) {
        PreparedStatement stmt = null;
        try{
            stmt = conn.prepareStatement(query);
            for(int i=0; i < args.length; i++) {
                stmt.setString(i+1, args[i]);
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
            return stmt;
    }

    public static List<HashMap<String,Object>> rsQuery(PreparedStatement stmt) throws SQLException {
        List<HashMap<String,Object>> result = new ArrayList<HashMap<String,Object>>();
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        while(rs.next()) {
            HashMap<String,Object> row = new HashMap<String, Object>(columnCount);
            for (int i=1; i <=columnCount; i++){
                row.put(rsmd.getColumnName(i), rs.getObject(i));
            }
            result.add(row);
        }
        System.out.println(result);
        return result;
    }
}

/*
            System.out.println("Listing system tables...");
                    stmt = conn.createStatement();
                    String sql;
                    sql = "select * from information_schema.tables;";
                    ResultSet rs = stmt.executeQuery(sql);

                    //Get the data from the result set.
                    while(rs.next()){
                    //Retrieve two columns.
                    String catalog = rs.getString("table_catalog");
                    String name = rs.getString("table_name");

                    //Display values.
                    System.out.print("Catalog: " + catalog);
                    System.out.println(", Name: " + name);
                    }
                    rs.close();
                    stmt.close();
                    conn.close();
                    */