package com.findwise.hydra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JvmHydraKiller extends Thread implements HydraKiller {

	Logger logger = LoggerFactory.getLogger(JvmHydraKiller.class);

	private long killDelay = 0;

	@Override
	public void run() {
		try {
			logger.debug("Hydra will be killed in " + killDelay + "ms unless it is shut down gracefully before then");
			Thread.sleep(killDelay);
			logger.warn("Failed to shutdown hydra gracefully within configured shutdown timeout. Killing Hydra now");
			System.exit(1);
		} catch (Throwable e) {
			logger.error("Caught exception in HydraKiller thread. Killing Hydra right away!", e);
			System.exit(1);
		}
	}

	@Override
	public void kill(long killDelay) {
		this.killDelay = killDelay;
		setDaemon(true);
		start();
	}
}
