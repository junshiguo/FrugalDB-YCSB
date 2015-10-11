package frugaldb.server;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.Socket;

/**
 * receive the list of tenant ids to be loaded or to be written back
 * @author guojunshi
 *
 */
public class FSocketTask extends Thread {
	private Socket socket;
	private BufferedReader reader;
	private Writer writer;
	
	public FSocketTask(Socket socket, BufferedReader reader, Writer writer) throws UnsupportedEncodingException, IOException{
		this.socket = socket;
		this.reader = reader;
		this.writer = writer;
	}
	
	//send task
	public void sendM2V(int mid, int vid) throws IOException{
		writer.write("M2V "+mid+" "+vid+"\n");
		writer.flush();
	}
	public void sendV2M(int mid) throws IOException{
		writer.write("V2M "+mid+" -1\n");
		writer.flush();
	}
	public void sendEnd() throws IOException{
		writer.write("end\n");
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
			while ((message = reader.readLine()) != null) {
				info = message.trim().split("\\s+");
				System.out.println("Receive: "+message);
				if (info[0].equalsIgnoreCase("interval")) {
					//set interval id in LoadThread, and add semaphore to check to offload
					LoadThread.setIntervalId(Integer.parseInt(info[1]));
					if(FServer.IS_MYSQL_TEST == false){
						FServer.loadThread.addSemaphore();
					}
					System.out.println("Interval set to "+info[1]);
				}else if(info[0].equalsIgnoreCase("loadfile")){
					//read load file and set offloader in FServer
					BufferedWriter loadWriter = new BufferedWriter(new FileWriter(info[1]));
					while((message = reader.readLine()) != null){
						if(message.equalsIgnoreCase("eof"))	break;
						loadWriter.write(message);
						loadWriter.newLine();
					}
					loadWriter.flush();
					loadWriter.close();
					FServer.setOffloader(info[1]);
					System.out.println("using load file: "+info[1]);
				}else if(info[0].equalsIgnoreCase("test")){
					if(info[1].equalsIgnoreCase("mysql")){
						FServer.IS_MYSQL_TEST = true;
					}else{
						FServer.IS_MYSQL_TEST = false;
					}
				}else if(info[0].equalsIgnoreCase("voltdbspace")){
					FOffloader.setTotSpaceInVoltdb(Integer.parseInt(info[1]));
					System.out.println("Voltdb space set to "+info[1]);
				}else{
					//end this socket, does not end the FServer process
					System.out.println("receive: "+message+". Current test ends...");
					break;
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
