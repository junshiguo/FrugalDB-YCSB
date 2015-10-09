package frugaldb.server;

import newhybrid.util.AbstractTenant;

public class FTenant implements AbstractTenant {
	private int id;
	private int id_V;
	private int datasize;
	private boolean isInVoltdb;
	private int[] workload;
	
	public void set2Voltdb(int id){
		this.id_V = id;
		this.isInVoltdb = true;
	}
	public void set2Mysql(){
		this.id_V = -1;
		this.isInVoltdb = false;
	}
	
	public FTenant(int id, int datasize, int intervalNumber){
		this.id = id;
		this.datasize = datasize;
		this.isInVoltdb = false;
		this.workload = new int[intervalNumber];
	}
	public void setWorkload(int[] w){
		if(w.length < workload.length){
			System.err.println("wrong workload array");
			return;
		}
		for(int i = 0; i < workload.length; i++){
			workload[i] = w[i];
		}
	}
	public void setWorkload(int index, int value){
		if(index < workload.length){
			workload[index] = value;
		}
	}

	@Override
	public int getID() {
		return this.id;
	}

	@Override
	public int getDataSize() {
		return this.datasize;
	}
	
	/**
	 * setIntervalId before using this function.
	 */
	@Override
	public int getWorkloadAhead() {
		int interval = LoadThread.getIntervalId();
		if(interval < workload.length - 2){
			return workload[interval+2];
		}
		return 0;
	}

	@Override
	public boolean isInVoltdbAhead() {
		return this.isInVoltdb;
	}

	@Override
	public boolean isInMysqlAhead() {
		return ! this.isInVoltdb;
	}
	
}
