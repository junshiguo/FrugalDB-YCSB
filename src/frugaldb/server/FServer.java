package frugaldb.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import frugaldb.server.loader.LoaderMain;
import newhybrid.util.AbstractTenant;
import newhybrid.util.OffloadTenant;

public class FServer {
	public static int SocketPort = 8899;
	public static FSocketTask socketReceive;
	public static FSocketTask socketSend;
	public static int IntervalId = 0;
	public static int getIntervalId() {
		return IntervalId;
	}
	public static void setIntervalId(int intervalId) {
		IntervalId = intervalId;
		checkOffload(+1);
	}
	
	public static int _checkOffload = 0;
	public static synchronized int checkOffload(int i){
		_checkOffload += i;
		return _checkOffload;
	}
	
	public static boolean ACTIVE = true;
	public static synchronized boolean checkActive(int i){
		if(i == -1)	ACTIVE = false;
		else if(i == 1) ACTIVE	= true;
		return ACTIVE;
	}
	public static boolean checkActive(){
		return checkActive(0);
	}

	public static ArrayList<AbstractTenant> tenants = new ArrayList<AbstractTenant>();
	
	public static void main(String [] args) throws IOException{
		String loadfile = "load.txt";
		if(args.length > 0){
			loadfile = args[0];
		}
		//initial tenants
		int intervalNumber;
		BufferedReader reader = new BufferedReader(new FileReader(loadfile));
		String line = reader.readLine().trim();
		String[] elements = line.split("\\s+");
		intervalNumber = Integer.parseInt(elements[0]);
		reader.readLine(); //slo line
		line = reader.readLine().trim();
		elements = line.split("\\s+");
		for(int i = 0; i < 3000; i++){
			tenants.add(new FTenant(i, Integer.parseInt(elements[i]), intervalNumber));
		}
		for(int i = 0; i < intervalNumber; i++)
			reader.readLine();
		for(int i = 0; i < intervalNumber; i++){
			line = reader.readLine().trim();
			elements = line.split("\\s+");
			for(int j = 1; j < 3001; j++){
				((FTenant) tenants.get(j)).setWorkload(i, Integer.parseInt(elements[j]));
			}
			for(int j = 0; j < 4; j++)
				reader.readLine();
		}
		reader.close();
		
		FOffloader decision = new FOffloader(tenants);
		
		//set up sockets
		ServerSocket serverSocket = new ServerSocket(SocketPort);
		serverSocket = lauchSocket(serverSocket);
		serverSocket = lauchSocket(serverSocket);
		
		//do loading work
		//receive socket get interval id, set FServer.intervalId; set all tenants' itervalId 
		while(true){
			if(checkOffload(0) != 0){
				checkOffload(-1);
				doOffload(decision.getOffloaderTenants());
			}else{
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if(checkActive() == false){
				break;
			}
		}
		
		try {
			socketSend.clean();
			socketReceive.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		serverSocket.close();
	}
	
	/**
	 * retrieve first, then load.
	 * socket messages are sent in OffloadThread and RetrieveThread.
	 * @param offloadTenants
	 */
	public static void doOffload(ArrayList<OffloadTenant> offloadTenants){
		ArrayList<Integer> toLoad = new ArrayList<Integer>();
		ArrayList<Integer> toRetrive = new ArrayList<Integer>();
		for(OffloadTenant tenant : offloadTenants){
			if(tenant.isToVoltdb()){
				toLoad.add(tenant.getID());
			}else{
				toRetrive.add(tenant.getID());
			}
		}
		LoaderMain.retrive(toRetrive);
		try {
			LoaderMain.cleanTmpFile();
			LoaderMain.load(toLoad);
			LoaderMain.cleanTmpFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static ServerSocket lauchSocket(ServerSocket serverSocket) throws IOException{
		Socket socket = serverSocket.accept();
		BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
		OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
		String line = reader.readLine();
		if(line.trim().equalsIgnoreCase("client to server")){
			socketReceive = new FSocketTask(socket, reader, writer);
			socketReceive.start();
		}else{
			socketSend = new FSocketTask(socket, reader, writer);
		}
		return serverSocket;
	}

}
