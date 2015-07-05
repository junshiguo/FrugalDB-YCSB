package frugaldb.server.loader.retriver;

import java.util.ArrayList;
import java.util.List;

import frugaldb.server.loader.VMMatch;

public class RetriveThread extends Thread {
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
		while((next = RetriveThread.nextToRetrive()) != -1){
			Voltdb2Mysql m = new Voltdb2Mysql(next, VMMatch.findTenant(next));
			m.run();
		}
	}
}
