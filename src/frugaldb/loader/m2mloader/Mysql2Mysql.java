package frugaldb.loader.m2mloader;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import frugaldb.server.loader.LoadConfig;

public class Mysql2Mysql {
	public int tenantId;
	public Connection connLocal;
	public Connection connRemote;
	
	public Mysql2Mysql(int tenantId, Connection connLocal, Connection connRemote){
		this.tenantId = tenantId;
		this.connLocal = connLocal;
		this.connRemote = connRemote;
	}
	
	public String getExportSQL(){
		String ret = "select * from usertable"+tenantId+" into outfile '"+LoadConfig.csvPath+"/usertable"+tenantId+".csv'"
				+ " FIELDS TERMINATED BY \'','\' ENCLOSED BY \'\"\';";
		return ret;
	}
	public String getImportSQL(){
		String ret = "LOAD DATA LOCAL INFILE '"+LoadConfig.csvPath+"/usertable"+tenantId+".csv'"
				+ " INTO TABLE usertable"+tenantId+" FIELDS TERMINATED BY \'','\' ENCLOSED BY \'\"\';";
		return ret;
	}
	
	public void load(){
		try {
			Statement stmt = connLocal.createStatement();
			
			//delete file before export to csv
//			File file = new File(LoadConfig.csvPath+"/usertable"+tenantId+".csv");
//			file.delete();
			stmt.execute(getExportSQL());
			
			Statement stmtRemote = connRemote.createStatement();
			stmtRemote.execute(getImportSQL());
//			File file = new File(LoadConfig.csvPath+"/usertable"+tenantId+".csv");
//			file.delete();
			stmt.close();
			stmtRemote.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
