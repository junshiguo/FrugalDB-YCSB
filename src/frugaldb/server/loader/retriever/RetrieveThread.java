package frugaldb.server.loader.retriever;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import org.voltdb.client.Client;

import frugaldb.server.FServer;
import frugaldb.server.loader.Tomove;
import frugaldb.server.loader.utility.DBManager;

public class RetrieveThread extends Thread {
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
	
	public Connection conn;
	public Client voltdbConn;
	@Override
	public void run(){
		Tomove next;
		while((next = RetrieveThread.nextToRetrive()) != null){
			conn = DBManager.checkMysqlConn(conn);
			voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
			Voltdb2Mysql m = new Voltdb2Mysql(next.Mid, next.Vid, conn, voltdbConn);
			long start = System.nanoTime();
			m.load();
			long end = System.nanoTime();
			System.out.println("Tenant "+next+" VoltDB ---> MySQL! Time: "+(end - start)/1000000000.0+" seconds...");
			try {
				FServer.socketSend.sendV2M(next.Mid);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
