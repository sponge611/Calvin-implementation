package org.vanilladb.calvin.groupcomm;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.vanilladb.comm.server.VanillaCommServer;
import org.vanilladb.comm.server.VanillaCommServerListener;
import org.vanilladb.comm.view.ProcessType;

public class GroupCommModule implements VanillaCommServerListener{
	private static Logger logger = Logger.getLogger(GroupCommModule.class.getName());
	private static final BlockingQueue<Serializable> msgQueue =
			new LinkedBlockingDeque<Serializable>();
	private static BlockingQueue<Serializable> clientList = new LinkedBlockingDeque<Serializable>();
	private static VanillaCommServer groupCommServer;
	public static void startGroupComm(int selfId) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Initializing the Group Communication Module...");
		groupCommServer = new VanillaCommServer(selfId, new GroupCommModule());
		new Thread(groupCommServer).start();
		createClientRequestHandler();
		clientHandler();
		if (logger.isLoggable(Level.INFO))
			logger.info("Group Communication Module is ready!");
		
		
	}
	private static void createClientRequestHandler() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						Serializable message = msgQueue.take();
						groupCommServer.sendTotalOrderMessage(message);
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
			logger.info("Group Communication Module is ready!");
		
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
	
	//We need to modify here. Now just analyze the message print the analyzed result. 
	@Override
	public void onReceiveTotalOrderMessage(long serialNumber, Serializable message) {
		Object[] pars = (Object[])message;
		int readCount;
		int writeCount;
		int[] readItemId;
		int[] writeItemId;
		double[] newItemPrice;
		String[] itemName;
		double[] itemPrice;
		int indexCnt = 2;

		readCount = (Integer) pars[indexCnt++];
		readItemId = new int[readCount];
		itemName = new String[readCount];
		itemPrice = new double[readCount];

		for (int i = 0; i < readCount; i++)
			readItemId[i] = (Integer) pars[indexCnt++];

		writeCount = (Integer) pars[indexCnt++];
		writeItemId = new int[writeCount];
		for (int i = 0; i < writeCount; i++)
			writeItemId[i] = (Integer) pars[indexCnt++];
		newItemPrice = new double[writeCount];
		for (int i = 0; i < writeCount; i++)
			newItemPrice[i] = (Double) pars[indexCnt++];
		System.out.println("SrialNumber: " + serialNumber);
		for (int i=0; i<writeCount; i++)
			System.out.println(writeItemId[i]);
		
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
						String str = "Got it! num: " + i;
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
