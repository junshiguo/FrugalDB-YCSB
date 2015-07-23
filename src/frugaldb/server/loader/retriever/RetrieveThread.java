package frugaldb.server.loader.retriever;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.client.Client;

import frugaldb.server.FServer;
import frugaldb.server.FTenant;
import frugaldb.server.loader.VMMatch;
import frugaldb.server.loader.utility.DBManager;
import frugaldb.utility.IdMatch;

public class RetrieveThread extends Thread {
	public static List<Integer> toRetrive = new ArrayList<Integer>();
	public static void setToRetrive(ArrayList<Integer> to){
		toRetrive = new ArrayList<Integer>(to);
	}
	public static synchronized int nextToRetrive(){
		int ret = -1;
		if(toRetrive.isEmpty() == false){
			ret = toRetrive.get(0);
			toRetrive.remove(0);
		}
		return ret;
	}
	
	public Connection conn;
	public Client voltdbConn;
	public void run(){
		int next;
		while((next = RetrieveThread.nextToRetrive()) != -1){
			int vid = VMMatch.findTenant(next);
			conn = DBManager.checkMysqlConn(conn);
			voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
			Voltdb2Mysql m = new Voltdb2Mysql(next, vid, conn, voltdbConn);
			long start = System.nanoTime();
			m.run();
			((FTenant) FServer.tenants.get(IdMatch.getThreadId(next))).set2Mysql();
			long end = System.nanoTime();
			System.out.println("Tenant "+next+" VoltDB ---> MySQL! Time: "+(end - start)/1000000000.0+" seconds...");
			try {
				FServer.socketSend.sendV2M(next);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
