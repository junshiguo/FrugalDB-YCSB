package frugaldb.loader.m2mloader;

import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;

import frugaldb.server.loader.utility.DBManager;

public class M2mloadThread extends Thread {
	public static ArrayList<Integer> toLoad = new ArrayList<Integer>(); //a list of ids that are to be offloaded
	public static void setToLoad(ArrayList<Integer> to){
		toLoad.addAll(to); // = new ArrayList<Integer>(to);
	}
	public static synchronized int nextToLoad(){
		int ret = -1;
		if(toLoad.isEmpty() == false){
			ret = toLoad.get(0);
			toLoad.remove(0);
		}
		return ret;
	}
	
	public M2mloadThread(String server){
		this.localServer = "127.0.0.1";
		this.remoteServer = server;
	}
	
	public String localServer;
	public String remoteServer;
	public Connection connLocal;
	public Connection connRemote;
	
	public void run(){
		int next;
		while((next = M2mloadThread.nextToLoad()) != -1){
			connLocal = DBManager.checkMysqlConn(connLocal, localServer, "remote", "remote");
			connRemote = DBManager.checkMysqlConn(connRemote, remoteServer, "remote", "remote");
			Mysql2Mysql loader = new Mysql2Mysql(next, connLocal, connRemote);
			long start = System.nanoTime();
			loader.load();
			long end = System.nanoTime();
			DecimalFormat df = new DecimalFormat(".0000");
			System.out.println("Tenant "+next+" MySQL ---> MySQL! Time spent: "+df.format((end-start)/1000000000.0)+" seconds!");
		}
	}
	
}
