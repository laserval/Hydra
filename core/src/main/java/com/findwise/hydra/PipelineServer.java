package com.findwise.hydra;

import java.io.IOException;

public interface PipelineServer extends Runnable {

	boolean blockingStart();

	boolean hasError();

	Exception getError();

	void shutdown() throws IOException;
}
