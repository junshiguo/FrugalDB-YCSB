package frugaldb.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import frugaldb.loader.LoadConfig;
import frugaldb.loader.LoaderMain;
import frugaldb.loader.Tomove;

public class LoadThread extends Thread {
	//semaphore, the times to check to offload
	private int _offload = 0;
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
		readToLoad();
		while(true){
			if(getSemaphore() > 0){
				doDataLoad();
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
	public void doDataLoad(){
		if(FServer.isMServer){
			ArrayList<Tomove> tmp = m2v.get(0);
			m2v.remove(0);
			LoaderMain.offload(tmp, LoadConfig.voltdbServer);
		}else{
			ArrayList<Tomove> tmp = v2m.get(0);
			v2m.remove(0);
			LoaderMain.retrieve(tmp, LoadConfig.mysqlServer);
			tmp = v2c.get(0);
			v2c.remove(0);
			LoaderMain.retrieve(tmp, LoadConfig.clientServer);
		}
	}
	
	ArrayList<ArrayList<Tomove>> m2v, v2m, v2c;
//	ArrayList<ArrayList<Integer>> m2c;
	/**
	 * discard the first line
	 */
	public void readToLoad(){
		BufferedReader reader = null;
		String line;
		String[] elements;
		try {
			if(FServer.isMServer){
				reader = new BufferedReader(new FileReader("M2V.txt"));
				reader.readLine();
				m2v = new ArrayList<>();
				while((line = reader.readLine()) != null){
					ArrayList<Tomove> tmp = new ArrayList<>();
					elements = line.trim().split("\\s+");
					for(int i = 0; i < elements.length; i+=2){
						tmp.add(new Tomove(Integer.parseInt(elements[i]), Integer.parseInt(elements[i+1])));
					}
					m2v.add(tmp);
				}
				reader.close();
				
//				reader = new BufferedReader(new FileReader("M2C.txt"));
//				reader.readLine();
//				m2c = new ArrayList<>();
//				while((line = reader.readLine()) != null){
//					ArrayList<Integer> tmp = new ArrayList<>();
//					elements = line.trim().split("\\s+");
//					for(int i = 0; i < elements.length; i++){
//						tmp.add(Integer.parseInt(elements[i]));
//					}
//					m2c.add(tmp);
//				}
//				reader.close();
			}else{ //voltdb server
				reader = new BufferedReader(new FileReader("V2M.txt"));
				reader.readLine();
				v2m = new ArrayList<>();
				while((line = reader.readLine()) != null){
					ArrayList<Tomove> tmp = new ArrayList<>();
					elements = line.trim().split("\\s+");
					for(int i = 0; i < elements.length; i+=2){
						tmp.add(new Tomove(Integer.parseInt(elements[i]), Integer.parseInt(elements[i+1])));
					}
					v2m.add(tmp);
				}
				reader.close();
				
				reader = new BufferedReader(new FileReader("V2C.txt"));
				reader.readLine();
				v2c = new ArrayList<>();
				while((line = reader.readLine()) != null){
					ArrayList<Tomove> tmp = new ArrayList<>();
					elements = line.trim().split("\\s+");
					for(int i = 0; i < elements.length; i+=2){
						tmp.add(new Tomove(Integer.parseInt(elements[i]), Integer.parseInt(elements[i+1])));
					}
					v2c.add(tmp);
				}
				reader.close();
			}
		} catch (IOException e) {
		}
	}

}
