package org.exhibitor.reports;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;

/**
 * Hello world!
 *
 */
public class Main
{
    public static Map<String,String> readFileProperties(String properties) {
        final Properties prop = new Properties();
        InputStream input = null;
        Map<String, String> inputData = null;

        try {
            input = new FileInputStream(properties);
            prop.load(input);
            inputData = new HashMap<String, String>() {
                {
                    put("dburl", prop.getProperty("dburl"));
                    put("user", prop.getProperty("user"));
                    put("password", prop.getProperty("password"));
                    put("sqlfiles", prop.getProperty("sqlfiles"));
                }
            };
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return inputData;
    }
    public static void main(String[] args){
        Map<String,String> connectionData = readFileProperties("config.properties");
        System.out.println(connectionData);
        String directory = connectionData.get("sqlfiles");
        String reportName = "weekly_exhibitor.sql";
        String query = Redshift.readFile(directory, reportName);
        System.out.println(query);
        String[] vars = {"2017-06-06", "2017-06-07", "AMC"};
        Connection conn = Redshift.dbconnect(connectionData.get("dburl"), connectionData.get("user"), connectionData.get("password"));
        PreparedStatement stmt = Redshift.prepStatement(query, conn, vars);
        try {
            System.out.println(Redshift.rsQuery(stmt));
        } catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
