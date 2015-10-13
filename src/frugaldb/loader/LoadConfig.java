package frugaldb.loader;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class LoadConfig {
	public static String url = "jdbc:mysql://10.20.2.28/ycsb_icde_compare";
	public static String username = "remote", password = "remote";
	public static String mysqlServer = "10.20.2.28";
	public static String voltdbServer = "10.20.2.30"; //need change
	public static String clientServer = "10.20.2.20";
	public static String dbname = "ycsb_icde_compare";
	public static String csvPath = "/tmp/tmp";
	public static int batch = 200;
	public static int VTableNumber = 50; //not configured by file.
	public static int M2VConcurrency = 3;
	public static int V2MConcurrency = 10;
	public static int M2MConcurrency = 3;
	public static int loadStart = 1500;
	public static int loadNumber = 10;
	public static int M2VMode = 0; //0: BulkPro linear,  1: BulkPro, 2: Bulk, 3: Threading, 4: Basic
	
	public static void configure() throws FileNotFoundException, IOException{
		Properties prop = new Properties();
		prop.load(new FileInputStream("load.conf"));
		voltdbServer = prop.getProperty("vserver", "127.0.0.1");
		dbname = prop.getProperty("dbname", "tpcc3000");
		mysqlServer = prop.getProperty("mserver");
		clientServer = prop.getProperty("cserver");
		url = "jdbc:mysql://"+mysqlServer+"/"+dbname;
		username = prop.getProperty("MUsername", "remote");
		password = prop.getProperty("MPassword", "remote");
		csvPath = prop.getProperty("csvPath", "/tmp");
		batch = Integer.parseInt(prop.getProperty("M2VBatch", "200"));
		M2VConcurrency = Integer.parseInt(prop.getProperty("M2VConcurrency", "3"));
		V2MConcurrency = Integer.parseInt(prop.getProperty("V2MConcurrency", "10"));
		loadStart = Integer.parseInt(prop.getProperty("loadStart", "0"));
		loadNumber = Integer.parseInt(prop.getProperty("loadNumber", "10"));
		M2VMode = Integer.parseInt(prop.getProperty("M2VMode", "0"));
		M2MConcurrency = Integer.parseInt(prop.getProperty("M2MConcurrency", "3"));
	}

}
