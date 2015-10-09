package frugaldb.workload;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class FMeasurement {
	public static ArrayList<FMeasurement> Measure = new ArrayList<FMeasurement>();
	public static void exportMeasure(String exportFile) throws IOException{
		int[] vq = new int[Measure.get(0).opcount.size()-5];
		int[] vt = new int[Measure.get(0).opcount.size()-5];
		ArrayList<ArrayList<Long>> latencies = new ArrayList<ArrayList<Long>>();
		for(int i = 0; i < vq.length; i++){
			latencies.add(new ArrayList<Long>());
		}
		for(FMeasurement measure : Measure){
			for(int i = 5; i < measure.opcount.size(); i++){
				int tmp = measure.opcount.get(i) - measure.opsdone.get(i);
				if(tmp > 0){
					vq[i-5] += tmp;
					vt[i-5]++;
				}
			}
			for(int i = 5; i < measure.allLatency.size(); i++){
				latencies.get(i-5).addAll(measure.allLatency.get(i));
			}
		}
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(exportFile+".violation", false));
		for(int i = 0; i < vq.length; i++){
			writer.write(""+(i+1)+" "+vt[i]+" "+vq[i]+"\n");
		}
		writer.close();
		
		writer = new BufferedWriter(new FileWriter(exportFile+".allLatencies", false));
		int min = 1;
		for(ArrayList<Long> list : latencies){
			writer.write(""+min);
			for(Long l : list){
				writer.write(" "+l);
			}
			writer.newLine();
			writer.flush();
			min++;
		}
		writer.close();
		
		writer = new BufferedWriter(new FileWriter(exportFile+"._Latency", false));
		ArrayList<Long> all = new ArrayList<Long>();
		for(ArrayList<Long> list : latencies)
			all.addAll(list);
		Collections.sort(all);
		int totalNumber = all.size();
		if(totalNumber == 0){
			for(int i = 0; i < 10; i++){
				writer.write(""+(50+i*5)+" 0\n");
			}
		}else{
			for(int i = 0; i < 10; i++){
				writer.write(""+(50+i*5)+" "+all.get((int) (totalNumber*(50+i*5)/100.0)));
				writer.newLine();
			}
//			writer.write("100 "+all.get(totalNumber -1));
			writer.newLine();
		}
		writer.close();
	}
	
	private ArrayList<Integer> opcount = new ArrayList<Integer>();
	private ArrayList<Integer> opsdone = new ArrayList<Integer>();
	private ArrayList<ArrayList<Long>> allLatency = new ArrayList<ArrayList<Long>>();
	private ArrayList<Long> latency = new ArrayList<Long>();
	
	public FMeasurement(){
//		Measure.add(this);
	}
	
	public void measurement(int opcount, int opsdone){
		this.opcount.add(opcount);
		this.opsdone.add(opsdone);
		allLatency.add(latency);
		latency = new ArrayList<Long>();
	}
	
	public void measurement(long latency){
		this.latency.add(latency);
	}
	
}
