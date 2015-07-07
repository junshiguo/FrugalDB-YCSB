package frugaldb.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class FServer {
	public static int SocketPort = 8899;
	
	public static void main(String [] args) throws IOException{
		//initial some parameters
		//get sockets prepared
		//do something according data received
		
		ServerSocket serverSocket = new ServerSocket(SocketPort);
		Socket socket = serverSocket.accept();
	}

}
