package org.vanilladb.comm.server;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.vanilladb.comm.view.ProcessType;

public class ServerDemo implements VanillaCommServerListener {
	private static Logger logger = Logger.getLogger(ServerDemo.class.getName());
	
	private static final BlockingQueue<Serializable> msgQueue =
			new LinkedBlockingDeque<Serializable>();
	private static BlockingQueue<Serializable> clientList = new LinkedBlockingDeque<Serializable>();
	public static void main(String[] args) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Initializing the server...");
		
		int selfId = Integer.parseInt(args[0]);
		VanillaCommServer server = new VanillaCommServer(selfId, new ServerDemo());
		new Thread(server).start();
		createClientRequestHandler(server);
		clientHandler(server);
	}

	private static void createClientRequestHandler(
			final VanillaCommServer server) {
		new Thread(new Runnable() {

			@Override
			public void run() {
				while (true) {
					try {
						Serializable message = msgQueue.take();
						server.sendTotalOrderMessage(message);
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
			logger.info("The server is ready!");
	}

	@Override
	public void onServerFailed(int failedServerId) {
		if (logger.isLoggable(Level.SEVERE))
			logger.severe("Server " + failedServerId + " failed");
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
	
	public static void clientHandler(final VanillaCommServer server) {
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
						server.sendP2pMessage(ProcessType.CLIENT, (Integer)analysis[0], ack);
						i++;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}

		}).start();
		
	}
	
	public void appendIntoClientList(Serializable message) {
		clientList.add(message);
	}
	
}
