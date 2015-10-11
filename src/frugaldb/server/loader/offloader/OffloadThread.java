package frugaldb.server.loader.offloader;

import java.io.IOException;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.voltdb.client.Client;

import frugaldb.server.FServer;
import frugaldb.server.loader.Tomove;
import frugaldb.server.loader.utility.DBManager;

public class OffloadThread extends Thread {
	public static ArrayList<Tomove> toLoad = new ArrayList<Tomove>(); //a list of ids that are to be offloaded
	public static void setToLoad(ArrayList<Tomove> to){
		toLoad.addAll(to); // = new ArrayList<Integer>(to);
	}
	public static synchronized Tomove nextToLoad(){
		Tomove ret = null;
		if(toLoad.isEmpty() == false){
			ret = toLoad.get(0);
			toLoad.remove(0);
		}
		return ret;
	}
	
	public Connection conn;
	public Client voltdbConn;
	@Override
	public void run(){
		Tomove next;
		while((next = OffloadThread.nextToLoad()) != null){
			conn = DBManager.checkMysqlConn(conn);
			voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
			OffloadInBulk loader = new OffloadInBulk(next.Mid, next.Vid, conn, voltdbConn);
			long start = System.nanoTime();
			loader.load();
			long end = System.nanoTime();
			DecimalFormat df = new DecimalFormat(".0000");
			System.out.println("Tenant "+next+" MySQL ---> VoltDB! Time spent: "+df.format((end-start)/1000000000.0)+" seconds!");
			try {
				FServer.socketSend.sendM2V(next.Mid, next.Vid);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
