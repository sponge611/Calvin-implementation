package org.vanilladb.calvin.server;

import java.io.Serializable;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.vanilladb.bench.server.procedure.StoredProcedureHelper;
import org.vanilladb.bench.server.procedure.micro.MicroTxnProc;
import org.vanilladb.bench.server.procedure.tpcc.NewOrderProc;
import org.vanilladb.calvin.groupcomm.GroupCommModule;
import org.vanilladb.comm.view.ProcessType;
import org.vanilladb.comm.view.ProcessView;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.remote.storedprocedure.SpResultSet;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;

public class Scheduler {
	private static Executor executor = Executors.newFixedThreadPool(50);
	private static HashMap<Long, Integer> MetaData = new HashMap<Long, Integer>();
	public static CopyOnWriteArrayList<SpResultSet> Cache = new CopyOnWriteArrayList<SpResultSet>();
	private static int serverCount = ProcessView.buildServersProcessList(-1).getSize();
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
			SpResultRecord record = (SpResultRecord)rs.getRecord();
			//System.out.println(record.getFldValueMapSize());
			//System.out.println(MetaData.get(sp.getTransaction().getTransactionNumber()));
			if(record.getFldValueMapSize() == 21) {
				sp.getTransaction().commit();
				rs.setCommitted();
				int clientId = sp.getParamHelper().getClientId();
				GroupCommModule.groupCommServer.sendP2pMessage(ProcessType.CLIENT, clientId, rs);
			}
			else if(MetaData.get(sp.getTransaction().getTransactionNumber())!=null) {
				//System.out.println("Here");
				rs.setTxNum(sp.getTransaction().getTransactionNumber());
				sp.getTransaction().commit();
				rs.setCommitted();
				GroupCommModule.groupCommServer.sendP2pMessage(ProcessType.SERVER, MetaData.get(sp.getTransaction().getTransactionNumber()), rs);
				MetaData.remove(sp.getTransaction().getTransactionNumber());
			}
			else {
				while(record.getFldValueMapSize() != 21) {
					try {
						Thread.sleep(1000);
						//System.out.println("Yeeeeeee");
						for(int i=0; i<Cache.size();i++) {
							SpResultSet temp = Cache.get(i);
							//System.out.println("Temp Tx Num: " + temp.getTxNum());
							//System.out.println("This Tx Num: " + sp.getTransaction().getTransactionNumber());
							if(temp.getTxNum() == sp.getTransaction().getTransactionNumber()) {
								SpResultRecord temp_rec = (SpResultRecord)temp.getRecord();
								System.out.println(temp_rec.getMap().size());
								temp_rec.getMap().remove("rc");
								for (String k : temp_rec.getMap().keySet()) {
									System.out.println(k);
									System.out.println(temp_rec.getMap().get(k));
									record.getMap().put(k,temp_rec.getMap().get(k));
								}
								
							}
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//System.out.println(record.getFldValueMapSize());
				}
				sp.getTransaction().commit();
				rs.setCommitted();
				int clientId = sp.getParamHelper().getClientId();
				GroupCommModule.groupCommServer.sendP2pMessage(ProcessType.CLIENT, clientId, rs);
			}
			
		}
		
		
	}
	//For only NewOrder Txn now
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
			
			if(serverCount == 1) {
				conservativeLocking(sp);
				executor.execute(new MicroWork(sp));
			}
			else if(serverCount == 2){
				
				for(int idx=0;idx<10;idx++) {
					//System.out.println(sp.getParamHelper().getReadItemId(idx));
					if(GroupCommModule.moduleId != 0 && sp.getParamHelper().getReadItemId(idx)<=100000) {
						MetaData.put(sp.getTransaction().getTransactionNumber(), 0);
					}
				}
				conservativeLocking(sp);
				executor.execute(new MicroWork(sp));
			}
			else {
				for(int idx=0;idx<10;idx++) {
					if(GroupCommModule.moduleId != 0 && sp.getParamHelper().getReadItemId(idx)<=100000) {
						MetaData.put(sp.getTransaction().getTransactionNumber(), 0);
					}
					else if(GroupCommModule.moduleId == 2 && sp.getParamHelper().getReadItemId(idx) > 100000 && sp.getParamHelper().getReadItemId(idx) <=200000 
							&& MetaData.get(sp.getTransaction().getTransactionNumber()) == null) 
					{
						MetaData.put(sp.getTransaction().getTransactionNumber(), 1);
					}
					else {
						//do nothing
					}
				}
				conservativeLocking(sp);
				executor.execute(new MicroWork(sp));
			}
		}
	}
	//Conservative locking
	private static void conservativeLocking(MicroTxnProc sp) {
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
		
	}

}
