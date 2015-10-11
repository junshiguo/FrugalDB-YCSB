package frugaldb.server.loader.utility;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import frugaldb.server.loader.LoadConfig;

import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;

public class DBManager {
	
	public static void main(String[] args){
		connectDB("jdbc:mysql://10.171.5.62:3306", "kevin", "123456");
	}
	
	public static Connection checkMysqlConn(Connection conn, String url, String username, String password){
		try {
			if(conn == null || conn.isClosed()){
				return connectDB(url, username, password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static Client checkVoltdbConn(Client vc, String server){
		if(vc == null){
			return connectVoltdb(server);
		}
		return vc;
	}
	
	public static Connection checkMysqlConn(Connection conn){
		try {
			if(conn == null || conn.isClosed()){
				return connectDB(LoadConfig.url, LoadConfig.username, LoadConfig.password);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	public static Client checkVoltdbConn(Client vc){
		if(vc == null){
			return connectVoltdb(LoadConfig.voltdbServer);
		}
		return vc;
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
