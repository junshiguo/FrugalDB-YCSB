package com.yahoo.ycsb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import frugaldb.loader.LoadConfig;
import frugaldb.loader.LoaderMain;
import frugaldb.loader.Tomove;

public class ServerLoadThread extends Thread {
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
						sleep(3*1000);
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
				ArrayList<Integer> tmp = c2m.get(0);
				c2m.remove(0);
				LoaderMain.m2mload(tmp, LoadConfig.mysqlServer);
				
				ArrayList<Tomove> tmp2 = c2v.get(0);
				c2v.remove(0);
				LoaderMain.offload(tmp2, LoadConfig.voltdbServer);
		}
		
		ArrayList<ArrayList<Tomove>> c2v;
		ArrayList<ArrayList<Integer>> c2m;
//		ArrayList<ArrayList<Integer>> m2c;
		/**
		 * discard the first line
		 */
		public void readToLoad(){
			BufferedReader reader = null;
			String line;
			String[] elements;
			try {
					reader = new BufferedReader(new FileReader("C2M.txt"));
					c2m = new ArrayList<>();
					while((line = reader.readLine()) != null){
						ArrayList<Integer> tmp = new ArrayList<>();
						if(line.trim().equals("") == false){
							elements = line.trim().split("\\s+");
							for(int i = 0; i < elements.length; i++){
								tmp.add(Integer.parseInt(elements[i]));
							}
						}
						c2m.add(tmp);
					}
					reader.close();
					
					reader = new BufferedReader(new FileReader("C2V.txt"));
					reader.readLine(); //important! the first line is for pre load
					c2v = new ArrayList<>();
					while((line = reader.readLine()) != null){
						ArrayList<Tomove> tmp = new ArrayList<>();
						if(line.trim().equals("") == false){
							elements = line.trim().split("\\s+");
							for(int i = 0; i < elements.length; i+=2){
								tmp.add(new Tomove(Integer.parseInt(elements[i]), Integer.parseInt(elements[i+1])));
							}
						}
						c2v.add(tmp);
					}
					reader.close();
			} catch (IOException e) {
			}
		}
}
