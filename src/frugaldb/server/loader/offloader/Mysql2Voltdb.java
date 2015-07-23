package frugaldb.server.loader.offloader;

import java.sql.Connection;
import java.text.DecimalFormat;

import org.voltdb.client.Client;

import frugaldb.server.loader.VMMatch;

public class Mysql2Voltdb {
	public Connection conn;
	public Client voltdbConn;
	public int tenantId;
	public int volumnId;
	
//	public static void main(String[] args){
//		new Mysql2Voltdb("jdbc:mysql://127.0.0.1/test", "remote", "remote", "127.0.0.1", 0).start();
//	}

	public Mysql2Voltdb(){}
	public Mysql2Voltdb(Connection conn, Client voltdbConn, int tenantId, int volumnId){
		this.conn = conn;
		this.voltdbConn = voltdbConn;
		this.tenantId = tenantId;
		this.volumnId = volumnId;
	}
	
	public void run(){
		int emptyVolumn = volumnId;
		if(emptyVolumn != -1){
			long start = System.nanoTime();
			VMMatch.addMatch(emptyVolumn, tenantId);
			this.Mysql2VoltdbBulkProLine(tenantId, emptyVolumn);
			long end = System.nanoTime();
			DecimalFormat df = new DecimalFormat(".0000");
			System.out.println("Tenant "+tenantId+" MySQL ---> VoltDB! Time spent: "+df.format((end-start)/1000000000.0)+" seconds!");
			System.out.println(""+df.format(time1)+"\t"+df.format(time2)+"\t"+df.format(time3)+" ("+df.format(time5)+")\t"+df.format(time4));
		}
	}
	
	/**
	 * currently in use. a linear version of Mysql2VoltdbBulkPro
	 * @param tenantId
	 * @param volumnId
	 */
	public double time1, time2, time3, time4, time5;
	public void Mysql2VoltdbBulkProLine(int tenantId, int volumnId){
		time1 = time2= time3 = time4 = 0;
		OffloadInBulk loader = new OffloadInBulk(tenantId, volumnId, conn, voltdbConn);
		loader.load();
		time1 += loader.time1; //time to export data from mysql into csv files
		time2 += loader.time2; //time to empty voltdb table (in case of primary key exist error)
		time3 += loader.time3; //time to insert dsata into voltdb
		time4 += loader.time4; //time to delete tmp csv files
		time5 += loader.time5; //time to do voltdb transaction, included in time3
	}
	
}
