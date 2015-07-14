package frugaldb.server.loader.retriever;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import frugaldb.server.FServer;
import frugaldb.server.loader.VMMatch;

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
	
	public void run(){
		int next;
		while((next = RetrieveThread.nextToRetrive()) != -1){
			int vid = VMMatch.findTenant(next);
			Voltdb2Mysql m = new Voltdb2Mysql(next, vid);
			m.run();
			try {
				FServer.socketSend.sendV2M(next);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
