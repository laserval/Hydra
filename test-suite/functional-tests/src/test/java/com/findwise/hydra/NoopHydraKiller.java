package com.findwise.hydra;

public class NoopHydraKiller implements HydraKiller {
	@Override
	public void kill(long killDelay) {
		// no op
	}
}
