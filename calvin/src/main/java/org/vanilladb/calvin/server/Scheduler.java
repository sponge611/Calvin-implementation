package org.vanilladb.calvin.server;

import java.io.Serializable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.vanilladb.bench.server.param.micro.MicroTxnProcParamHelper;
import org.vanilladb.bench.server.param.tpcc.NewOrderProcParamHelper;
import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.bench.server.procedure.micro.MicroTxnProc;
import org.vanilladb.bench.server.procedure.tpcc.NewOrderProc;
import org.vanilladb.calvin.groupcomm.GroupCommModule;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.StoredProcedure;

public class Scheduler {
	private static Executor executor = Executors.newFixedThreadPool(50);
	
	
	static class TpccWork implements Runnable{
		NewOrderProc sp;
		public TpccWork(NewOrderProc sp) {
			this.sp = sp;
		}

		@Override
		public void run() {
			SpResultSet rs = sp.execute();
			int clientId = sp.getParamHelper().getClientId();
			GroupCommModule.groupCommServer.sendP2pMessage(ProcessType.CLIENT, clientId, rs);
			
		}
		
		
	}
	static class MicroWork implements Runnable{
		MicroTxnProc sp;
		public MicroWork(MicroTxnProc sp) {
			this.sp = sp;
		}

		@Override
		public void run() {
			SpResultSet rs = sp.execute();
			int clientId = sp.getParamHelper().getClientId();
			GroupCommModule.groupCommServer.sendP2pMessage(ProcessType.CLIENT, clientId, rs);
			
		}
		
		
	}
	//For NewOrder Txn now
	public static void analyzeTheTpccMessage(Serializable message) {
		//Analyze the serialized message
		Object[] deserialized = (Object[]) message;
		for(int i=0;i<deserialized.length;) {
			Object[] pars = new Object[51];
			for(int j=0;j<51;j++) {
				pars[j] = deserialized[i];
				i++;
			}
			NewOrderProc sp = new NewOrderProc();
			sp.prepare(pars);
			executor.execute(new TpccWork(sp));
		}	
	}
	public static void analyzeTheMicroMessage(Serializable message) {
		//Analyze the serialized message
		Object[] deserialized = (Object[]) message;
		for(int i=0;i<deserialized.length;) {
			Object[] pars = new Object[33];
			for(int j=0;j<33;j++) {
				pars[j] = deserialized[i];
				i++;
			}
			MicroTxnProc sp = new MicroTxnProc();
			sp.prepare(pars);
			
			//Conservative locking
			for (int idx = 0; idx < sp.getParamHelper().getReadCount(); idx++) {
				int iid = sp.getParamHelper().getReadItemId(idx);
				Scan s = StoredProcedureHelper.executeQuery(
					"SELECT i_name, i_price FROM item WHERE i_id = " + iid,
					sp.getTransaction()
				);
				s.beforeFirst();
				if (s.next()) {
					s.getSlock();	
					s.getRecordXlock();
				} 
				s.close();
			}

			
			
			executor.execute(new MicroWork(sp));
		}
	}

}
