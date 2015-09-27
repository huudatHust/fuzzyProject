package com.fuzzy.model;

import com.fuzzy.load.LoadFunction;
import com.mysql.jdbc.Connection;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author TRAN HUU DAT
 */
public class ConnectDB {
    private Connection conn = null;

    public Connection getConnecttion(){
        try {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            conn = (Connection) DriverManager.getConnection(getUrlDB());
            return conn;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | SQLException ex) {
            Logger.getLogger(ConnectDB.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println(ex.getMessage());
            
        } catch (IOException ex) {
            Logger.getLogger(ConnectDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
    private  String getUrlDB() throws FileNotFoundException, IOException{
        LoadFunction load = new LoadFunction();
        BufferedReader input = load.loadStream("db.properties");
        String line = input.readLine();
        String localhost = line.split(":")[1].trim();
        line = input.readLine();
        String port = line.split(":")[1].trim();
        line = input.readLine();
        String dbName = line.split(":")[1].trim();
        line = input.readLine();                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
        String userName= line.split(":")[1].trim();
        line = input.readLine();
        String password = line.split(":")[1].trim();
        input.close();      
        String result = "jdbc:mysql://" + localhost + ":" + port + "/" +dbName + "?user=" + userName + "&password=" + password;
        return result;
    }
    public void closeConnect() throws SQLException{
        if(conn != null)
            conn.close();
    }
    
}
