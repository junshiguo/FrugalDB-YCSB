package frugaldb.utility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;

public class DBManager {
	
	public static void main(String[] args){
		connectDB("jdbc:mysql://10.171.5.62:3306", "kevin", "123456");
	}
	
	public static Connection connectDB(String url, String username, String password){
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = DriverManager.getConnection(url,username,password);
			return conn;
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
			System.exit(0);
			return null;
		}
	}
	
	public static Client connectVoltdb(String serverlist){
        String[] servers = serverlist.split(",");

        org.voltdb.client.Client myApp;
        myApp = ClientFactory.createClient();
        for (String server: servers) { 
            try {
				myApp.createConnection(server);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
        }
        return myApp;
	}
	
}
