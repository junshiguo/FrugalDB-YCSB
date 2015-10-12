package frugaldb.loader.retriever;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.client.Client;

import frugaldb.loader.Tomove;
import frugaldb.server.FServer;
import frugaldb.server.loader.utility.DBManager;

public class RetrieveThread extends Thread {
	public static boolean SOCKET_ACTIVE = false;
	
	public static List<Tomove> toRetrive = new ArrayList<Tomove>();
	public static void setToRetrive(ArrayList<Tomove> to){
		toRetrive = new ArrayList<Tomove>(to);
	}
	public static synchronized Tomove nextToRetrive(){
		Tomove ret = null;
		if(toRetrive.isEmpty() == false){
			ret = toRetrive.get(0);
			toRetrive.remove(0);
		}
		return ret;
	}
	
	public RetrieveThread(String  mserver){
		this.mserver = mserver;
	}
	
	public String mserver;
	public Connection conn;
	public Client voltdbConn;
	@Override
	public void run(){
		Tomove next;
		while((next = RetrieveThread.nextToRetrive()) != null){
			conn = DBManager.checkMysqlConn(conn, mserver, "remote", "remote");
			voltdbConn = DBManager.checkVoltdbConn(voltdbConn, "127.0.0.1");
			Voltdb2Mysql m = new Voltdb2Mysql(next.Mid, next.Vid, conn, voltdbConn);
			long start = System.nanoTime();
			m.load();
			long end = System.nanoTime();
			System.out.println("Tenant "+next+" VoltDB ---> MySQL! Time: "+(end - start)/1000000000.0+" seconds...");
			if (SOCKET_ACTIVE) {
				try {
					FServer.socketSend.sendV2M(next.Mid);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
