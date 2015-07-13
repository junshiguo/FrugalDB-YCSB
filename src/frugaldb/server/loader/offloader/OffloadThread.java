package frugaldb.server.loader.offloader;

import java.io.IOException;
import java.sql.Connection;
import java.util.ArrayList;

import org.voltdb.client.Client;

import frugaldb.server.FServer;
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
	public void run(){
		int next;
		while((next = OffloadThread.nextToLoad()) != -1){
			conn = DBManager.checkMysqlConn(conn);
			voltdbConn = DBManager.checkVoltdbConn(voltdbConn);
			int vid = VMMatch.findVolumn();
			Mysql2Voltdb m = new Mysql2Voltdb(conn, voltdbConn, next, vid);
			m.run();
			try {
				FServer.socketSend.send(true, next, vid);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
