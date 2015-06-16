package frugaldb.workload;

import java.util.Properties;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.workloads.CoreWorkload;

/**
 * currently all functions use super's implementation
 * @author guojunshi
 *
 */
public class FrugalDBWorkload extends CoreWorkload {
	
	public int getFieldCount(){
		return this.fieldcount;
	}

	/**
     * Initialize the scenario. Create any generators and other shared objects here.
     * Called once, in the main client thread, before any operations are started.
     */
	public void init(Properties p) throws WorkloadException {
		super.init(p);
	}

	/**
     * Initialize any state for a particular client thread. Since the scenario object
     * will be shared among all threads, this is the place to create any state that is specific
     * to one thread. To be clear, this means the returned object should be created anew on each
     * call to initThread(); do not return the same object multiple times. 
     * The returned object will be passed to invocations of doInsert() and doTransaction() 
     * for this thread. There should be no side effects from this call; all state should be encapsulated
     * in the returned object. If you have no state to retain for this thread, return null. (But if you have
     * no state to retain for this thread, probably you don't need to override initThread().)
     * 
     * @return false if the workload knows it is done for this thread. 
     * Client will terminate the thread. Return true otherwise. 
     * Return true for workloads that rely on operationcount. 
     * For workloads that read traces from a file, return true when there are more to do, false when you are done.
     */
	public Object initThread(Properties p, int mythreadid, int threadcount)
			throws WorkloadException {
		// TODO Auto-generated method stub
		return super.initThread(p, mythreadid, threadcount);
	}

	/**
     * Cleanup the scenario. Called once, in the main client thread, after all operations have completed.
     */
	public void cleanup() throws WorkloadException {
		// TODO Auto-generated method stub
		super.cleanup();
	}

	/**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this 
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
     * synchronized, since each thread has its own threadstate instance.
     */
	public boolean doInsert(DB db, Object threadstate) {
		// TODO Auto-generated method stub
		return super.doInsert(db, threadstate);
	}

	/**
     * Do one transaction operation. Because it will be called concurrently from multiple client threads, this 
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each 
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations and mutations on threadstate. Mutations to threadstate do not need to be
     * synchronized, since each thread has its own threadstate instance.
     * 
     * @return false if the workload knows it is done for this thread. Client will terminate the thread. Return true otherwise. Return true for workloads that rely on operationcount. For workloads that read traces from a file, return true when there are more to do, false when you are done.
     */
	public boolean doTransaction(DB db, Object threadstate) {
		// TODO Auto-generated method stub
		return super.doTransaction(db, threadstate);
	}

}
