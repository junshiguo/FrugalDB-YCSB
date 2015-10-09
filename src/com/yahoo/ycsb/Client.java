/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;


import java.io.*;
import java.util.*;

import com.yahoo.ycsb.workloads.CoreWorkload;

import frugaldb.db.FrugalDBClient;
import frugaldb.utility.IdMatch;
import frugaldb.workload.FMeasurement;
import frugaldb.workload.FrugalDBWorkload;

//import org.apache.log4j.BasicConfigurator;

/**
 * Main class for executing YCSB.
 */
public class Client
{

	public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

	public static final String WORKLOAD_PROPERTY="workload";
	
	public static final String WORKLOAD_FILE_FOR_FRUGALDB = "workloadfile_F";
	public static final String RESULT_FILE_FRUGALDB = "resultfile_F";
	
	public static final String TOTAL_INTERVAL_FRUGALDB = "totalinterval_F";
	public static final String TOTAL_INTERVAL_FRUGALDB_DEFAULT = "1";
	
	public static final String MINUTE_PER_INTERVAL_FRUGALDB = "minuteperinterval_F";
	public static final String MINUTE_PER_INTERVAL_FRUGALDB_DEFAULT = "1";
	
	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";
	
	/**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";


	public static void usageMessage()
	{
		System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
		System.out.println("Options:");
		System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
				"              \"threadcount\" property using -p");
		System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
				"             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
		System.out.println("  -t:  run the transactions phase of the workload (default)");
		System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
				"              can also be specified as the \"db\" property using -p");
		System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out.println("                   be specified, and will be processed in the order specified");
		System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out.println("  -s:  show status during run (default: no status)");
		System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out.println("  "+WORKLOAD_PROPERTY+": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)");
		System.out.println("");
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
	}

	public static boolean checkRequiredProperties(Properties props)
	{
		if (props.getProperty(WORKLOAD_PROPERTY)==null)
		{
			System.out.println("Missing property: "+WORKLOAD_PROPERTY);
			return false;
		}

		return true;
	}


	public static boolean START_TEST = false;
	public synchronized static boolean checkStart(boolean toggle){
		if(toggle){
			START_TEST = !START_TEST;
		}
		return START_TEST;
	}
	public static Vector<Thread> threads=new Vector<Thread>();
	public static void setVoltdb(int mid, int vid){
		((ClientThread)threads.get(IdMatch.getThreadId(mid))).setVoltdb(vid);
	}
	@SuppressWarnings("unchecked")
	public static void main(String[] args)
	{
		String dbname;
		Properties props=new Properties();
		Properties fileprops=new Properties();
		boolean dotransactions=true;
		int threadcount=1;
		int target=0;
		boolean status=false;
		String label="";

		//parse arguments
		int argindex=0;

		if (args.length==0)
		{
			usageMessage();
			System.exit(0);
		}

		while (args[argindex].startsWith("-"))
		{
			if (args[argindex].compareTo("-threads")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int tcount=Integer.parseInt(args[argindex]);
				props.setProperty("threadcount", tcount+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-target")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int ttarget=Integer.parseInt(args[argindex]);
				props.setProperty("target", ttarget+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-load")==0)
			{
				dotransactions=false;
				argindex++;
			}
			else if (args[argindex].compareTo("-t")==0)
			{
				dotransactions=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-s")==0)
			{
				status=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-db")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("db",args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-l")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				label=args[argindex];
				argindex++;
			}
			else if (args[argindex].compareTo("-measure")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("measure", args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-testtype")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("testtype", args[argindex].trim());
				argindex++;
			}
			else if (args[argindex].compareTo("-P")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				String propfile=args[argindex];
				argindex++;

				Properties myfileprops=new Properties();
				try
				{
					myfileprops.load(new FileInputStream(propfile));
				}
				catch (IOException e)
				{
					System.out.println(e.getMessage());
					System.exit(0);
				}

				//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
				for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
				{
				   String prop=(String)e.nextElement();
				   
				   fileprops.setProperty(prop,myfileprops.getProperty(prop));
				}

			}
			else if (args[argindex].compareTo("-p")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int eq=args[argindex].indexOf('=');
				if (eq<0)
				{
					usageMessage();
					System.exit(0);
				}

				String name=args[argindex].substring(0,eq);
				String value=args[argindex].substring(eq+1);
				props.put(name,value);
				//System.out.println("["+name+"]=["+value+"]");
				argindex++;
			}
			else
			{
				System.out.println("Unknown option "+args[argindex]);
				usageMessage();
				System.exit(0);
			}

			if (argindex>=args.length)
			{
				break;
			}
		}

		if (argindex!=args.length)
		{
			usageMessage();
			System.exit(0);
		}

		//set up logging
		//BasicConfigurator.configure();

		//overwrite file properties with properties from the command line

		//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
		for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
		   String prop=(String)e.nextElement();
		   
		   fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;

		if (!checkRequiredProperties(props))
		{
			System.exit(0);
		}
		
		//get number of threads, target and db
		threadcount=Integer.parseInt(props.getProperty("threadcount","2000"));
		dbname=props.getProperty("db","com.yahoo.ycsb.BasicDB");
		target=Integer.parseInt(props.getProperty("target","0"));
		
		//compute the target throughput
		double targetperthreadperms=-1;
//		if (target>0)
//		{
//			double targetperthread=((double)target)/((double)threadcount);
//			targetperthreadperms=targetperthread/1000.0;
//		}	 

		System.out.println("YCSB Client 0.1");
		System.out.print("Command line:");
		for (int i=0; i<args.length; i++)
		{
			System.out.print(" "+args[i]);
		}
		System.out.println();
		System.err.println("Loading workload...");
		
		//run the workload

		System.err.println("Starting test.");

		int opcount;
		if (dotransactions)
		{
			opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));
		}
		else
		{
			if (props.containsKey(INSERT_COUNT_PROPERTY))
			{
				opcount=Integer.parseInt(props.getProperty(INSERT_COUNT_PROPERTY,"0"));
			}
			else
			{
				opcount=Integer.parseInt(props.getProperty(RECORD_COUNT_PROPERTY,"0"));
			}
		}

//		IdMatch.init(threadcount);
		Workload workload = null;
		int user5 = Integer.parseInt(props.getProperty("user5", "0"));
		int user50 = Integer.parseInt(props.getProperty("user50", "0"));
		int user500 = Integer.parseInt(props.getProperty("user500", "0"));
		threadcount = user5 + user50 + user500;
		for (int threadid=0; threadid<threadcount; threadid++)
		{
			DB db=new FrugalDBClient();
			db.setProperties(props);
			try {
				workload = new FrugalDBWorkload();
				props.put("operationcount", "0");
				workload.init(props);
			} catch (WorkloadException e) {
				e.printStackTrace();
				System.exit(0);
			}
			Thread t=new ClientThread(db,dotransactions,workload,threadid,threadcount,props,opcount/threadcount,targetperthreadperms);
			threads.add(t);
			t.start();
			try {
				Thread.sleep(5);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		int returnstatus = 10;
		if(dotransactions){
			try {
//				SocketTask.lauchSockets(props.getProperty("voltdbserver", "127.0.0.1"));
//				String loadfile = props.getProperty(WORKLOAD_FILE_FOR_FRUGALDB, "load.txt");
//				SocketTask.socketSend.sendLoadfile(loadfile);
//				BufferedReader loadReader = new BufferedReader(new FileReader(loadfile));
//				String loadline;
//				while((loadline = loadReader.readLine()) != null){
//					SocketTask.socketSend.send(loadline);
//				}
//				SocketTask.socketSend.send("eof");
//				loadReader.close();
//				SocketTask.socketSend.sendTestType(props.getProperty("testtype", "mysql"));
//				SocketTask.socketSend.sendVoltdbSpace(Integer.parseInt(props.getProperty("voltdbspace", "2000")));
				
//				BufferedReader reader = new BufferedReader(new FileReader(loadfile));
				int total_interval = Integer.parseInt(props.getProperty(TOTAL_INTERVAL_FRUGALDB, TOTAL_INTERVAL_FRUGALDB_DEFAULT));
				int minute_per_interval = Integer.parseInt(props.getProperty(MINUTE_PER_INTERVAL_FRUGALDB, MINUTE_PER_INTERVAL_FRUGALDB_DEFAULT));
				//start test signal
				for(Thread t : threads){
					((ClientThread)t).checkOpcount(0);
				}
				
//				String firstLine = reader.readLine();
//				String[] firsts = firstLine.split("\\s+");
//				total_interval = Integer.parseInt(firsts[1]);
//				firsts = reader.readLine().trim().split("\\s+");
//				int[] ids = new int[threadcount];
//				for(int i = 0; i < threadcount; i++)
//					ids[i] = Integer.parseInt(firsts[i]) - 1;
//				IdMatch.initIdMatch(ids);
//				for(int i = 0; i < 2+total_interval; i++){
//					reader.readLine();
//				}
				
				boolean measure = true;
				if(props.getProperty("measure","true").equals("true")){
					measure = true;
				}else{
					measure = false;
				}
				CoreWorkload.setMeasure(measure);
				Client.checkStart(true);
				Thread.sleep(3000); //wait for potential initialization work
				System.out.println("Starting FrugalDB test. total interval: "+total_interval);
				for(int interval = 0; interval < total_interval; interval++){
//					SocketTask.socketSend.sendInterval(interval);
					long vtSum = 0, vqSum = 0, vm = 0;
					for(int minute = 0; minute < minute_per_interval; minute++){
						//update opcount to workload, update opdone to 0
//						String line = reader.readLine();
//						if(line == null){
//							System.out.println("Fail to read from load file! Stopping...");
//							reader.close();
//							System.exit(1);
//						}
//						String[] load = line.split("\\s+");
						for(int i = 0; i < threads.size(); i++){
							if(i < user5){
								((ClientThread) threads.get(i)).checkOpcount(5);
							}else if (i < user5 + user50){
								((ClientThread) threads.get(i)).checkOpcount(50);
							}else{
								((ClientThread) threads.get(i)).checkOpcount(500);
							}
							((ClientThread) threads.get(i)).checkOpsdone(-1);
						}
						//sleep while client threads do transactions 
						Thread.sleep(60*1000);
						//summary measurements and write to file
						int vq = 0, vt = 0;
						for(Thread t : threads){
							((ClientThread) t)._workload.measure.measurement(((ClientThread) t).checkOpcount(-1), ((ClientThread) t).getOpsDone());
							int tmp = ((ClientThread) t).checkOpcount(-1) - ((ClientThread) t).getOpsDone();
							if(tmp > 0){
								vq += tmp;
								vt ++;
							}
						}
						vtSum += vt;
						vqSum += vq;
						if(vt > 0)	vm++;
						System.out.println("Minute "+(interval*minute_per_interval+minute+1)+" finished! Violation: "+vt+" tenants and "+vq+" queries.");
					}
//					if(interval != 0 && (vqSum > 100 || vtSum > 10) && vm > 1){
					if((vqSum > 200 && vtSum > 10) || vm > 1){						
						System.out.println("too many violations, setting return status to 1...");
						returnstatus = 1;
//						System.exit(1);
					}
				}
//				reader.close();
				Thread.sleep(3000);
				for(Thread t : threads){
					FMeasurement.Measure.add(((ClientThread) t)._workload.measure);
				}
				FMeasurement.exportMeasure(props.getProperty(RESULT_FILE_FRUGALDB, "."));
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}else{
			Client.checkStart(true);
		}
		
		for(Thread t : threads){
			((ClientThread) t).getWorkload().requestStop();
			t.interrupt();
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
    
    System.out.println("joining threads...");
    for(Thread t : threads){
    	try {
			t.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
		
    System.out.println("threads joined ...");
		System.exit(returnstatus);
	}
}
