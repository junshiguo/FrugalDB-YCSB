package frugaldb.server.loader.offloader;

import java.io.IOException;
import java.sql.Connection;
import java.text.DecimalFormat;
import java.util.ArrayList;

import org.voltdb.client.Client;

import frugaldb.server.FServer;
import frugaldb.server.FTenant;
import frugaldb.server.loader.VMMatch;
import frugaldb.server.loader.utility.DBManager;

public class OffloadThread extends Thread {
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
	
	public Connection conn;
	public Client voltdbConn;
	@Override
	public void run(){
		int next;
		while((next = OffloadThread.nextToLoad()) != -1){
			conn = DBManager.checkMysqlConn(conn);
			voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
			int vid = VMMatch.findVolumn();
			if(vid == -1){
				continue;
			}
			VMMatch.addMatch(vid, next);
			OffloadInBulk loader = new OffloadInBulk(next, vid, conn, voltdbConn);
			long start = System.nanoTime();
			loader.load();
			((FTenant) FServer.tenants.get(next)).set2Voltdb(vid);
			long end = System.nanoTime();
			DecimalFormat df = new DecimalFormat(".0000");
			System.out.println("Tenant "+next+" MySQL ---> VoltDB! Time spent: "+df.format((end-start)/1000000000.0)+" seconds!");
			try {
				FServer.socketSend.sendM2V(next, vid);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
