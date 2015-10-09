package frugaldb.server.loader;

import java.io.IOException;

import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import frugaldb.server.loader.utility.DBManager;

public class CleanVoltdb {
	public static Client client;
	
	public static void main(String[] args) throws NoConnectionsException, IOException, ProcCallException, InterruptedException{
		clean();
	}
	
	public static void clean() throws NoConnectionsException, IOException, ProcCallException, InterruptedException{
		client = DBManager.connectVoltdb(LoadConfig.voltdbServer);
		int tenantNumber = LoadConfig.VTableNumber;
		for(int i = 0; i < tenantNumber; i++){
			client.callProcedure("@AdHoc", "TRUNCATE TABLE usertable"+i);
		}
		client.close();
	}

}
