package org.vanilladb.calvin.groupcomm;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.calvin.server.Scheduler;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;

public class GroupCommModule implements VanillaCommServerListener{
	private static Logger logger = Logger.getLogger(GroupCommModule.class.getName());
	private static final BlockingQueue<Serializable> msgQueue =
			new LinkedBlockingDeque<Serializable>();
	private static BlockingQueue<Serializable> clientList = new LinkedBlockingDeque<Serializable>();
	public static VanillaCommServer groupCommServer;
	private static List<Serializable>messages;
	private static int moduleId;
	private static long epochStart;
	public static void startGroupComm(int selfId) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Initializing the Group Communication Module...");
		moduleId = selfId;
		groupCommServer = new VanillaCommServer(selfId, new GroupCommModule());
		new Thread(groupCommServer).start();
		createClientRequestHandler();
		//clientHandler();
		
		
	}
	private static void createClientRequestHandler() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				epochStart = System.currentTimeMillis();
				while (true) {
					try {	
						if(System.currentTimeMillis() - epochStart >= 10 && !msgQueue.isEmpty()) {
							messages = new ArrayList<Serializable>();
							while(!msgQueue.isEmpty()) {
								messages.add(msgQueue.take());
							}
							groupCommServer.sendTotalOrderMessages(messages);
							epochStart = System.currentTimeMillis();
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

		}).start();
	}

	@Override
	public void onServerReady() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Server is ready!");
		
	}

	@Override
	public void onServerFailed(int failedServerId) {
		if (logger.isLoggable(Level.SEVERE))
			logger.severe("Group Communication Module " + failedServerId + " failed");
		
	}

	@Override
	public void onReceiveP2pMessage(ProcessType senderType, int senderId, Serializable message) {
		if (senderType == ProcessType.CLIENT) {
			try {
				msgQueue.put(message);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	//We need to modify here. Maybe using another thread to run Scheduler.analyzeTheMicroMessage(message). 
	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		Scheduler.analyzeTheMicroMessage(message);
		
	}
	
	public static void clientHandler() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				int i = 0;
				while (true) {
					try {
						Serializable message = clientList.take();
						Object[] analysis = (Object[])message;
						String str = "Got it from module" + moduleId + " num: " + i;
						Serializable ack = (Serializable)  str;
						groupCommServer.sendP2pMessage(ProcessType.CLIENT, (Integer)analysis[0], ack);
						i++;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

		}).start();
		
	}
		
	@Override
	public void appendIntoClientList(Serializable message) {
		clientList.add(message);
		
	}
	

}
