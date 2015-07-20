package com.yahoo.ycsb;

import java.util.Properties;

import com.yahoo.ycsb.workloads.CoreWorkload;

import frugaldb.db.FrugalDBClient;
import frugaldb.utility.IdMatch;


/**
 * A thread for executing transactions or data inserts to the database.
 * 
 * @author cooperb
 *
 */
class ClientThread extends Thread
{
	DB _db;
	boolean _dotransactions;
	Workload _workload;
	int _opcount;
	/**
	 * the _target variable controls query speed. now it will read through load file
	 */
	double _target;

	int _opsdone;
	int _threadid;
	int _threadcount;
	Object _workloadstate;
	Properties _props;
	
	public void setVoltdb(int id){
		((FrugalDBClient) _db).setInVoltdb(id);
	}
	public Workload getWorkload(){
		return _workload;
	}

	/**
	 * Constructor.
	 * 
	 * @param db the DB implementation to use
	 * @param dotransactions true to do transactions, false to insert data
	 * @param workload the workload to use
	 * @param threadid the id of this thread 
	 * @param threadcount the total number of threads 
	 * @param props the properties defining the experiment
	 * @param opcount the number of operations (transactions or inserts) to do
	 * @param targetperthreadperms target number of operations per thread per ms
	 */
	public ClientThread(DB db, boolean dotransactions, Workload workload, int threadid, int threadcount, Properties props, int opcount, double targetperthreadperms)
	{
		_db=db;
		_dotransactions=dotransactions;
		_workload=workload;
		_opcount=opcount;
		_opsdone=0;
		_target=targetperthreadperms;
		_threadid=threadid;
		_threadcount=threadcount;
		_props=props;
		//System.out.println("Interval = "+interval);
	}

	public int getOpsDone()
	{
		return checkOpsdone(0);
	}
	/**
	 * action = -1: reset _opsdone to 0; 
	 * _opsdone += action otherwise.
	 * @param action
	 * @return
	 */
	public synchronized int checkOpsdone(int action){
		if(action == -1){
			_opsdone = 0;
		}else{
			_opsdone += action;
		}
		return _opsdone;
	}
	/**
	 * if load is less than 0, the _opcount will be returned; _opcount is set to load otherwise.
	 * @param load per minute
	 * @return
	 */
	public synchronized int checkOpcount(int load){
		if(load >= 0){
			this._opcount = load;
			this._target = this._opcount * 1.0 / 55000;
		}
		return _opcount;
	}

	public void run()
	{
		try
		{
			_db.init();
			_db.init(IdMatch.getId(_threadid));
		}
		catch (DBException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}
		
		System.out.println("Thread "+this._threadid+" started ...");

		//wait until all threads are initialized, start operations together
		while(Client.checkStart(false) == false){
			try {
				sleep(3000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		//spread the thread operations out so they don't all hit the DB at the same time
		try
		{
		   //GH issue 4 - throws exception if _target>1 because random.nextInt argument must be >0
		   //and the sleep() doesn't make sense for granularities < 1 ms anyway
		   if ( (_target>0) && (_target<=1.0) ) 
		   {
		      sleep(Utils.random().nextInt((int)(1.0/_target)));
		   }
		}
		catch (InterruptedException e)
		{
		  // do nothing.
		}
		
		try
		{
			if (_dotransactions)
			{
				long st=System.currentTimeMillis();

				//TODO: the first condition is not necessary,_opcount is set by setLoad(int), sleep and interrupt
//				while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
				while (!_workload.isStopRequested())
				{
					
					//newly added
					if(this.checkOpsdone(0) == this.checkOpcount(-1)){
						int freetime = 0;
						while(this.checkOpsdone(0) == this.checkOpcount(-1)){
							try
							{
								sleep(1000);
								freetime++;
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}
							if(_workload.isStopRequested()){
								System.out.println("thread "+this._threadid+" stopping...");
								return;
							}
							if(freetime >= 60){
								((FrugalDBClient) this._db).closeConnection();
							}
						}
						st=System.currentTimeMillis();
					}

					if (!_workload.doTransaction(_db,_workloadstate))
					{
						break;
					}

					this.checkOpsdone(1);

					//throttle the operations
					if (_target>0)
					{
						//this is more accurate than other throttling approaches we have tried,
						//like sleeping for (1/target throughput)-operation latency,
						//because it smooths timing inaccuracies (from sleep() taking an int, 
						//current time in millis) over many operations
						while (System.currentTimeMillis()-st<((double)this.checkOpsdone(0))/_target)
						{
							try
							{
								sleep(1);
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}

						}
					}else{
						while(this.checkOpcount(-1) == 0){
							try
							{
								sleep(1000);
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}
							if(_workload.isStopRequested()){
								System.out.println("thread "+this._threadid+" stopping...");
								return;
							}
						}
					}
				}
				System.out.println("thread "+this._threadid+" stopping...");
			}
			else
			{
				long st=System.currentTimeMillis();
				while (((_opcount == 0) || (_opsdone < _opcount)) && !_workload.isStopRequested())
				{

					if (!_workload.doInsert(_db,_workloadstate))
					{
						break;
					}

					_opsdone++;

					//throttle the operations
					if (_target>0)
					{
						//this is more accurate than other throttling approaches we have tried,
						//like sleeping for (1/target throughput)-operation latency,
						//because it smooths timing inaccuracies (from sleep() taking an int, 
						//current time in millis) over many operations
						while (System.currentTimeMillis()-st<((double)_opsdone)/_target)
						{
							try 
							{
								sleep(1);
							}
							catch (InterruptedException e)
							{
							  // do nothing.
							}
						}
					}
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			_db.cleanup();
		}
		catch (DBException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			return;
		}
	}
}