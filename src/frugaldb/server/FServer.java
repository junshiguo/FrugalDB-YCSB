package frugaldb.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import frugaldb.server.loader.VMMatch;
import newhybrid.util.AbstractTenant;

public class FServer {
	public static boolean IS_MYSQL_TEST = true;
	public static int SocketPort = 8899;
	public static FSocketTask socketReceive;
	public static FSocketTask socketSend;
	public static FOffloader offloader;
	public static LoadThread loadThread;
	
	public static void main(String [] args) throws IOException{
		loadThread = new LoadThread();
		loadThread.start();
		
		//set up sockets 
		ServerSocket serverSocket = new ServerSocket(SocketPort);
		Socket socket;
		while((socket = serverSocket.accept()) != null){
			lauchSocket(socket);
		}
		serverSocket.close();
	}
	
	public static void resetVMMacth(){
		VMMatch.init();
	}
	
	public static void setOffloader(String loadfile){
		try {
			offloader = readLoad(loadfile);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static ArrayList<AbstractTenant> tenants = new ArrayList<AbstractTenant>();
	/**
	 * read from loadfile, initial a new offloader decision
	 * 
	 * called when FSocketTask receives "loadfile filename"
	 * @param loadfile
	 * @throws IOException
	 */
	public static FOffloader readLoad(String loadfile) throws IOException{
		//initial tenants
		int intervalNumber, totalTenant;
		BufferedReader reader = new BufferedReader(new FileReader(loadfile));
		String line = reader.readLine().trim();
		String[] elements = line.split("\\s+");
		totalTenant = Integer.parseInt(elements[0]);
		intervalNumber = Integer.parseInt(elements[1]);
		String[] ids = reader.readLine().split("\\s+"); //id line
		int[] idss = new int[totalTenant];
		for(int i = 0; i < totalTenant; i++){
			idss[i] = Integer.parseInt(ids[i]) - 1;
		}

		reader.readLine(); //slo line
		line = reader.readLine().trim(); //ds
		elements = line.split("\\s+");
		tenants = new ArrayList<AbstractTenant>();
		for(int i = 0; i < totalTenant; i++){
			tenants.add(new FTenant(Integer.parseInt(ids[i]) - 1, Integer.parseInt(elements[i]), intervalNumber));
		}
		for(int i = 0; i < intervalNumber; i++)
			reader.readLine();
		for(int i = 0; i < intervalNumber; i++){
			line = reader.readLine().trim();
			elements = line.split("\\s+");
			for(int j = 0; j < totalTenant; j++){
				((FTenant) tenants.get(j)).setWorkload(i, Integer.parseInt(elements[j+1]));
			}
			for(int j = 0; j < 4; j++)
				reader.readLine();
		}
		reader.close();
		
		return new FOffloader(tenants);
	}
	
	/**
	 * lauch socket task; 
	 * only one instance of socketReceive and socketSend exist; 
	 * normally, the two socket tasks will appear in pair
	 * @param socket
	 * @throws IOException
	 */
	public static void lauchSocket(Socket socket) throws IOException{
		BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
		OutputStreamWriter writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
		String line = reader.readLine();
		if(line.trim().equalsIgnoreCase("client to server")){
			socketReceive = new FSocketTask(socket, reader, writer);
			socketReceive.start();
			System.out.println("client to server socket up...");
		}else{
			socketSend = new FSocketTask(socket, reader, writer);
			System.out.println("server to client socket up...");
		}
	}

}
