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
		}
	}
	
	/**
	 * currently in use. a linear version of Mysql2VoltdbBulkPro
	 * @param tenantId
	 * @param volumnId
	 */
	public void Mysql2VoltdbBulkProLine(int tenantId, int volumnId){
		OffloadInBulk loader = new OffloadInBulk(tenantId, volumnId, conn, voltdbConn);
		loader.load();
	}
	
}
