package frugaldb.server.loader;

import java.io.IOException;
import java.util.ArrayList;

import org.voltdb.client.ProcCallException;

import frugaldb.server.loader.offloader.OffloadThread;
import frugaldb.server.loader.retriever.RetrieveThread;

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
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(int i = 0; i < number; i++){
			list.add(i + LoadConfig.loadStart);
		}
		long start = System.nanoTime();
		load(list);
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
	public static void load(ArrayList<Integer> toLoad){
		init();
		OffloadThread.setToLoad(toLoad);
		OffloadThread[] loaders = new OffloadThread[LoadConfig.M2VConcurrency];
		for(int i = 0; i < LoadConfig.M2VConcurrency; i++){
			loaders[i] = new OffloadThread();
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
	 * @param toRetrive a list of tenant ids to be retrived
	 */
	public static void retrive(ArrayList<Integer> toRetrive){
		init();
		RetrieveThread.setToRetrive(toRetrive);
		RetrieveThread[] retrivers = new RetrieveThread[LoadConfig.V2MConcurrency];
		for(int i = 0; i < LoadConfig.V2MConcurrency; i++){
			retrivers[i] = new RetrieveThread();
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

}
