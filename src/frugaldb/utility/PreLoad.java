package frugaldb.utility;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import frugaldb.loader.LoadConfig;
import frugaldb.loader.LoaderMain;
import frugaldb.loader.Tomove;

/**
 * load data into voltdb
 * first line and c2v 2nd line
 * to dispatch interval 1 state
 * @author guojunshi
 *
 */
public class PreLoad {
	public static void main(String[] args) throws NumberFormatException, IOException {
		LoadConfig.configure();
		BufferedReader reader = new BufferedReader(new FileReader("dispatch.txt"));
		String line;
		boolean first = true;
		ArrayList<Tomove> list = new ArrayList<>();
		int vid = 0;
		while((line = reader.readLine()) != null){
			if(line.trim().equals("") || line.startsWith("#")){
				continue;
			}
			String[] elements = line.trim().split("\\s+");
			if(first){
				first = false;
			}else{
				for(int i = 0; i < 2000; i++){
					int store = Integer.parseInt(elements[i]);
					if(store == -1){
						list.add(new Tomove(i, vid%50));
						vid++;
					}
				}
				break;
			}
		}
		System.out.println("pre load start, total "+vid+" tenants to offload..."); 
		LoaderMain.offload(list, LoadConfig.voltdbServer);
		System.out.println("pre load done!");
	}

}
