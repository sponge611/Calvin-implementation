package org.vanilladb.calvin.server;

import org.vanilladb.bench.server.SutStartUp;
import org.vanilladb.bench.server.VanillaDbSpStartUp;
import org.vanilladb.calvin.groupcomm.GroupCommModule;

public class CalvinServerStartUp {
	public static void main(String[] args) {
		GroupCommModule.startGroupComm(Integer.parseInt(args[0]));
		SutStartUp sut = null;
		sut = new VanillaDbSpStartUp();
		if (sut != null)
			sut.startup(args);
		
	}

}
