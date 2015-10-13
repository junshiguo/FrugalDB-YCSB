package frugaldb.server;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import frugaldb.loader.offloader.OffloadThread;
import frugaldb.loader.retriever.RetrieveThread;
import newhybrid.util.AbstractTenant;

public class FServer {
	public static int SocketPort = 8899;
	public static FSocketTask socketReceive;
	public static FSocketTask socketSend;
	public static LoadThread loadThread;
	
	public static boolean isMServer = true;
	
	public static void main(String [] args) throws IOException{
		if(args.length > 0){
			if(args[0].trim().equals("voltdb"))
				isMServer = false;
			else if(args[0].trim().equals("mysql"))
				isMServer = true;
		}
		OffloadThread.SOCKET_ACTIVE = true;
		RetrieveThread.SOCKET_ACTIVE = true;
		
		loadThread = new LoadThread();
		loadThread.start();
		
		//set up sockets 
		ServerSocket serverSocket = new ServerSocket(SocketPort);
		System.out.println("Server started...");
		Socket socket;
		while((socket = serverSocket.accept()) != null){
			lauchSocket(socket);
		}
		serverSocket.close();
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
