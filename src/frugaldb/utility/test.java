package frugaldb.utility;

import java.sql.Connection;
import java.text.DecimalFormat;

import frugaldb.loader.m2mloader.Mysql2Mysql;
import frugaldb.server.loader.utility.DBManager;

public class test {
	public static void main(String[] args){
		Connection connLocal = DBManager.checkMysqlConn(null, "jdbc:mysql://127.0.0.1/ycsb_icde_compare", "remote", "remote");
		Connection connRemote = DBManager.checkMysqlConn(null, "jdbc:mysql://10.20.2.28/ycsb_icde_compare", "remote", "remote");
		Mysql2Mysql loader = new Mysql2Mysql(10, connLocal, connRemote);
		long start = System.nanoTime();
		loader.load();
		long end = System.nanoTime();
		DecimalFormat df = new DecimalFormat(".0000");
		System.out.println("Tenant "+10+" MySQL ---> MySQL! Time spent: "+df.format((end-start)/1000000000.0)+" seconds!");
	}

}
