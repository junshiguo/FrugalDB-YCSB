package frugaldb.utility;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class CopyData {
	public static String driver = "com.mysql.jdbc.Driver";
	public static String server = "10.20.2.28";
	public static String db = "ycsb_icde";
	public static String url;
	public static String user = "remote";
	public static String passwd = "remote";
	
	public static Connection conn;
	public static Statement stmt;
	
	public static void main(String[] args){
		int startId = 1;
		int copyNumber = 1;
		if(args.length > 0){
			server = args[0];
		}
		if(args.length > 1){
			db = args[1];
		}
		if(args.length > 2){
			startId = Integer.parseInt(args[2]);
		}
		if(args.length > 3){
			copyNumber = Integer.parseInt(args[3]);
		}
		url= "jdbc:mysql://"+server+"/"+db;
		
//		for(int id = startId; id < startId + copyNumber; id++){
//			copy("usertable0", "userl"+id);
//		}
//		copy("users1", "users0");
//		copy("userm1", "userm0");
//		copy("userl1", "userl0");
//		for(int id = 730; id < 740; id++){
//			copy("users0", "users"+id);
//		}
//		for(int id = 2000; id < 3000; id++){
//			copy("userm0", "userm"+id);
//		}
		for(int id = 2370; id < 2500; id++){
			copy("userl0", "userl"+id);
		}
		
//		recopy(1);
//		for(int id = 4; id < 1500; id++){
//			copy(id);
//		}
//		recopy(2);
//		for(int id = 1500; id < 2400; id++){
//			copy(id);
//		}
//		recopy(3);
//		for(int id = 2400; id < 3000; id++){
//			copy(id);
//		}
//		recopy(1);
//		for(int id = 2; id < 4; id++){
//			copy(id);
//		}
	}
	
	/**
	 * copy from usertable0 to usertable+id
	 * @param id
	 */
	public static void copy(int id){
		try {
			checkConnection();
			Long start = System.currentTimeMillis();
			stmt.execute("DROP TABLE IF EXISTS usertable"+id);
			stmt.execute("CREATE TABLE usertable"+id+" ( ycsb_key VARCHAR (255) PRIMARY KEY, field0 TEXT, field1 TEXT, field2 TEXT, "
					+ "field3 TEXT, field4 TEXT, field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT) Engine=InnoDB;");
			stmt.execute("INSERT INTO usertable"+id+" SELECT * FROM usertable0");
			Long end = System.currentTimeMillis();
			System.out.println("copy tables for tenant " + id
					+ ". Time spent: " + (end - start) / 1000F);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void copy(String tablefrom, String tableto){
		try {
			checkConnection();
			Long start = System.currentTimeMillis();
			stmt.execute("DROP TABLE IF EXISTS "+tableto);
			stmt.execute("CREATE TABLE "+tableto+" ( ycsb_key VARCHAR (255) PRIMARY KEY, field0 TEXT, field1 TEXT, field2 TEXT, "
					+ "field3 TEXT, field4 TEXT, field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT) Engine=InnoDB;");
			stmt.execute("INSERT INTO "+tableto+" SELECT * FROM "+tablefrom);
			Long end = System.currentTimeMillis();
			System.out.println("copy tables for " + tableto
					+ ". Time spent: " + (end - start) / 1000F);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * copy from usertable+id to usertable 0
	 * @param id
	 */
	public static void recopy(int id){
		try {
			checkConnection();
			Long start = System.currentTimeMillis();
			stmt.execute("TRUNCATE TABLE usertable0");
			stmt.execute("INSERT INTO usertable0 SELECT * FROM usertable"+id);
			Long end = System.currentTimeMillis();
			System.out.println("recopy tables " + id
					+ ". Time spent: " + (end - start) / 1000F);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}
	}
	
	public static void checkConnection() throws ClassNotFoundException, SQLException{
		if(conn == null || conn.isClosed()){
			Class.forName(driver);
			conn = DriverManager.getConnection(url, user, passwd);
			stmt = conn.createStatement();
			System.out.println("db connected!");
		}else if(stmt == null){
			stmt = conn.createStatement();
			System.out.println("db connected!");
		}
	}

}
