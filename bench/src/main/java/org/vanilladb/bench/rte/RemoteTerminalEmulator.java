/*******************************************************************************
 * Copyright 2016, 2018 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.bench.rte;

import java.util.concurrent.atomic.AtomicInteger;

import org.vanilladb.bench.BenchTransactionType;
import org.vanilladb.bench.StatisticMgr;
import org.vanilladb.bench.TxnResultSet;
import org.vanilladb.bench.remote.SutConnection;
import org.vanilladb.comm.client.VanillaCommClient;
import org.vanilladb.comm.client.VanillaCommClientListener;
import org.vanilladb.comm.view.ProcessView;

public abstract class RemoteTerminalEmulator<T extends BenchTransactionType> extends Thread implements VanillaCommClientListener{

	private static AtomicInteger rteCount = new AtomicInteger(0);

	private volatile boolean stopBenchmark;
	private volatile boolean isWarmingUp = true;
	private SutConnection conn;
	private StatisticMgr statMgr;
	private int selfId;
	private VanillaCommClient client;
	private int serverCount = ProcessView.buildServersProcessList(-1).getSize();
	private int targetServerId;
	public RemoteTerminalEmulator(SutConnection conn, StatisticMgr statMgr) {
		this.conn = conn;
		this.statMgr = statMgr;
		
		// Set the thread name
		setName("RTE-" + rteCount.getAndIncrement());
		this.selfId = rteCount.get();
		this.targetServerId = selfId % serverCount;
		this.client = new VanillaCommClient(this.selfId, this);
		new Thread(client).start();
	}

	@Override
	public void run() {
		while (!stopBenchmark) {
			TxnResultSet rs = executeTxnCycle();
			if (!isWarmingUp)
				statMgr.processTxnResult(rs);
		}
	}

	public void startRecordStatistic() {
		statMgr.setRecordStartTime();
		isWarmingUp = false;
	}

	public void stopBenchmark() {
		stopBenchmark = true;
	}

	protected abstract T getNextTxType();
	
	protected abstract TransactionExecutor<T> getTxExeutor(T type);

	/*private TxnResultSet executeTxnCycle(SutConnection conn) {
		T txType = getNextTxType();
		TransactionExecutor<T> executor = getTxExeutor(txType);
		return executor.execute(conn);
	}*/
	private TxnResultSet executeTxnCycle() {
		T txType = getNextTxType();
		TransactionExecutor<T> executor = getTxExeutor(txType);
		return executor.execute(this.client,this.targetServerId);
	}
}