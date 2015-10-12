package frugaldb.loader;

import java.io.IOException;
import java.util.ArrayList;

import org.voltdb.client.ProcCallException;

import frugaldb.loader.m2mloader.M2mloadThread;
import frugaldb.loader.offloader.OffloadThread;
import frugaldb.loader.retriever.RetrieveThread;

public class LoaderMain extends Thread {
	
	public static void main(String[] args) throws IOException, ProcCallException, InterruptedException{
		if(args.length > 0){
			long start = System.nanoTime();
			CleanVoltdb.clean();
			long end = System.nanoTime();
			System.out.println("Clean Voltdb in "+(end-start)/1000000000.0+" seconds.");
			return;
		}
		init();
		cleanTmpFile();
		int number = LoadConfig.loadNumber;
		ArrayList<Tomove> list = new ArrayList<Tomove>();
		int index = 0;
		for(int i = 0; i < number; i++){
			list.add(new Tomove(i + LoadConfig.loadStart, (++index)%50));
		}
		long start = System.nanoTime();
		offload(list, "10.20.2.28");
		long end = System.nanoTime();
		System.out.println("\nLoad "+number+" tenants. Total time: "+(end-start)/1000000000.0+" s.");
		cleanTmpFile();
	}
	
	public static void init(){
		try {
			LoadConfig.configure();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void cleanTmpFile() throws IOException{
		String[] cmd = {"/bin/sh", "-c", "rm -r "+LoadConfig.csvPath+"/*.csv"};
		Runtime.getRuntime().exec(cmd);
	}
	
	/**
	 * load data from mysql to voltdb. 
	 * @param toLoad a list of tenant ids to be loaded
	 */
	public static void offload(ArrayList<Tomove> toLoad, String vserver){
		init();
		OffloadThread.setToLoad(toLoad);
		OffloadThread[] loaders = new OffloadThread[LoadConfig.M2VConcurrency];
		for(int i = 0; i < LoadConfig.M2VConcurrency; i++){
			loaders[i] = new OffloadThread(vserver);
			loaders[i].start();
		}
		for(int i = 0; i < LoadConfig.M2VConcurrency; i++){
			try {
				loaders[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * retrive data from voltdb to mysql
	 * @param toRetrieve a list of tenant ids to be retrived
	 */
	public static void retrieve(ArrayList<Tomove> toRetrieve, String mserver){
		init();
		RetrieveThread.setToRetrive(toRetrieve);
		RetrieveThread[] retrivers = new RetrieveThread[LoadConfig.V2MConcurrency];
		for(int i = 0; i < LoadConfig.V2MConcurrency; i++){
			retrivers[i] = new RetrieveThread(mserver);
			retrivers[i].start();
		}
		for(int i = 0; i < LoadConfig.V2MConcurrency; i++){
			try {
				retrivers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void m2mload(ArrayList<Integer> toLoad, String rserver){
		init();
		M2mloadThread.setToLoad(toLoad);
		M2mloadThread[] retrivers = new M2mloadThread[LoadConfig.M2MConcurrency];
		for(int i = 0; i < LoadConfig.M2MConcurrency; i++){
			retrivers[i] = new M2mloadThread(rserver);
			retrivers[i].start();
		}
		for(int i = 0; i < LoadConfig.M2MConcurrency; i++){
			try {
				retrivers[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
