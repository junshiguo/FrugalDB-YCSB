package frugaldb.server;

import java.io.IOException;
import java.util.ArrayList;

import newhybrid.util.OffloadTenant;
import frugaldb.server.loader.LoaderMain;

public class LoadThread extends Thread {
	public static int INTERVAL;
	/**
	 * interval id counts from 0
	 * @return
	 */
	public static int getIntervalId() {
		return INTERVAL;
	}
	public static void setIntervalId(int intervalId) {
		INTERVAL = intervalId;
	}
	
	//semaphore, the times to check to offload
	private int _offload;
	public int getSemaphore(){
		return checkOffload(0);
	}
	public void addSemaphore(){
		checkOffload(1);
	}
	public void decreaseSemaphore(){
		checkOffload(-1);
	}
	public synchronized int checkOffload(int var){
		_offload += var;
		return _offload;
	}
	
	@Override
	public void run(){
		INTERVAL = 0;
		_offload = 0;
		while(true){
			if(getSemaphore() > 0){
				doOffload(FServer.getOffloader().getOffloaderTenants());
				decreaseSemaphore();
			}else{
				try {
					sleep(10*1000);
				} catch (InterruptedException e) {
				}
			}
		}
	}
	
	/**
	 * retrieve first, then load.
	 * socket messages are sent in OffloadThread and RetrieveThread.
	 * @param offloadTenants
	 */
	public void doOffload(ArrayList<OffloadTenant> offloadTenants){
		ArrayList<Integer> toLoad = new ArrayList<Integer>();
		ArrayList<Integer> toRetrive = new ArrayList<Integer>();
		for(OffloadTenant tenant : offloadTenants){
			if(tenant.isToVoltdb()){
				toLoad.add(tenant.getID());
			}else{
				toRetrive.add(tenant.getID());
			}
		}
		LoaderMain.retrive(toRetrive);
		try {
			LoaderMain.cleanTmpFile();
			LoaderMain.load(toLoad);
			LoaderMain.cleanTmpFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
