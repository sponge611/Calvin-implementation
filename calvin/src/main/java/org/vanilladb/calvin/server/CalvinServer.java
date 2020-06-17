package org.vanilladb.calvin.server;

import org.vanilladb.calvin.groupcomm.GroupCommModule;

public class CalvinServer {
	public static void main(String[] args) {
		GroupCommModule.startGroupComm(Integer.parseInt(args[0]));
		
		
	}

}
