package com.yahoo.ycsb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * TODO: client part of socket communication
 * @author guojunshi
 */
public class SocketTask extends Thread {
	public static int SocketPort = 8899;
	public static int TYPE_SEND = 0;
	public static int TYPE_RECEIVE = 1;

	public static SocketTask[] socketReceive;
	public static SocketTask[] socketSend;
	/**
	 * the Client class is too crowded, thus socket relevant functions are placed here
	 * @param server
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public static void lauchSockets(String mserver, String vserver) throws UnknownHostException, IOException{
		socketReceive = new SocketTask[2];
		socketReceive[0] = new SocketTask(mserver, SocketTask.TYPE_RECEIVE);
		socketReceive[1] = new SocketTask(vserver, SocketTask.TYPE_RECEIVE);
		socketReceive[0].start();
		socketReceive[1].start();
		socketSend = new SocketTask[2];
		socketSend[0] = new SocketTask(mserver, SocketTask.TYPE_SEND);
		socketSend[1] = new SocketTask(vserver, SocketTask.TYPE_SEND);
		socketSend[0].start();
		socketSend[1].start();
	}
	
	private int type;
	private Socket socket;
	private Writer writer;
	private BufferedReader reader;
	
	public SocketTask(String server, int type) throws UnknownHostException, IOException{
		socket = new Socket(server, SocketPort);
		reader = new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));
		writer = new OutputStreamWriter(socket.getOutputStream(),"UTF-8");
		this.type = type;
		if(this.type == TYPE_SEND){
			writer.write("client to server\n");
			writer.flush();
		}else{
			writer.write("server to client\n");
			writer.flush();
		}
	}
	
	//send task
	public void send(String line) throws IOException{
		writer.write(line+"\n");
		writer.flush();
	}
	public void sendSemaphore(int interval) throws IOException{
		send("dodataload "+interval);
	}
	public void sendReadLoad() throws IOException{
		send("readload");
	}
	public void sendEnd() throws IOException{
		send("end");
	}
	/**
	 * @param type should be mysql or frugaldb
	 * @throws IOException
	 */
	public void sendTestType(String type) throws IOException{
		writer.write("test "+type+"\n");
		writer.flush();
	}
	public void clean() throws IOException{
		sendEnd();
		writer.close();
		reader.close();
		socket.close();
	}
	
	//receive task
	@Override
	public void run(){
		try {
			String message = null;
			String[] info = null;
			while((message = reader.readLine()) != null){
				info = message.trim().split("\\s+");
				if(info[0].equalsIgnoreCase("end")){
					break;
				}else{
					if(info[0].equals("V2C")){
						Client.setReady(Integer.parseInt(info[0]), false);
					}else{
						Client.setVoltdb(Integer.parseInt(info[1]), Integer.parseInt(info[2]));
					}
				}
			}
			reader.close();
			writer.close();
			socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
