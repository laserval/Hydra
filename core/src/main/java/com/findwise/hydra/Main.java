package com.findwise.hydra;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.net.SimpleSocketServer;

import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoType;
import com.findwise.hydra.net.HttpRESTHandler;
import com.findwise.hydra.net.RESTServer;

public final class Main implements ShutdownHandler {

	private static final long KILL_DELAY = TimeUnit.SECONDS.toMillis(30);
	private final CoreConfiguration coreConfiguration;

	public Main(CoreConfiguration coreConfiguration) {
		this.coreConfiguration = coreConfiguration;
	}

	private static Logger logger = LoggerFactory.getLogger(Main.class);
	private SimpleSocketServer loggingServer = null;
	private RESTServer server = null;
	private NodeMaster<MongoType> nodeMaster = null;

	private volatile boolean shuttingDown = false;

	private HydraKiller killer = new JvmHydraKiller();
	
	public static void main(String[] args) {
		if (args.length > 1) {
			logger.error("Some parameters on command line were ignored.");
		}
		
		CoreConfiguration conf;
		if (args.length > 0) {
			conf = getConfiguration(args[0]);
		} else {
			conf = getConfiguration(null);
		}

		new Main(conf).startup();
	}

	public void startup() {
		ShuttingDownOnUncaughtException uncaughtExceptionHandler = new ShuttingDownOnUncaughtException(this);
		Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler);
		loggingServer = new SimpleSocketServer((LoggerContext) LoggerFactory.getILoggerFactory(), coreConfiguration.getLoggingPort());
		loggingServer.setName(String.format("%s-%d", loggingServer.getClass().getSimpleName(), coreConfiguration.getLoggingPort()));
		loggingServer.start();

		logger.info("Hydra Core creating connector, {}='{}', {}='{}'",
				DatabaseConfiguration.DATABASE_URL_PARAM, coreConfiguration.getDatabaseUrl(),
				DatabaseConfiguration.DATABASE_NAMESPACE, coreConfiguration.getNamespace());
		
		DatabaseConnector<MongoType> backing = new MongoConnector(coreConfiguration);
		try {
			backing.connect();
		} catch (IOException e) {
			logger.error("Unable to start", e);
			return;
		}

		Cache<MongoType> cache;
		if (coreConfiguration.isCacheEnabled()) {
			cache = new MemoryCache<MongoType>();
		} else {
			cache = new NoopCache<MongoType>();
		}

		CachingDocumentNIO<MongoType> caching = new CachingDocumentNIO<MongoType>(
				backing, 
				cache, 
				coreConfiguration.isCacheEnabled(),
				coreConfiguration.getCacheTimeout());

		nodeMaster = new NodeMaster<MongoType>(
				coreConfiguration,
				caching,
				new Pipeline(), 
				this);

		server = new RESTServer(coreConfiguration,
				new HttpRESTHandler<MongoType>(
						nodeMaster.getDocumentIO(),
						backing.getPipelineReader(), 
						null,
						coreConfiguration.isPerformanceLogging()));

		if (!server.blockingStart()) {
			if (server.hasError()) {
				logger.error("Failed to start REST server: ", server.getError());
			} else {
				logger.error("Failed to start REST server");
			}
			shutdown();
		}

		try {
			nodeMaster.blockingStart();
		} catch (IOException e) {
			logger.error("Unable to start nodemaster... Shutting down.");
			shutdown();
		}
	}

	public void shutdown() {
		logger.info("Got shutdown request...");
		shuttingDown = true;
		killUnlessShutdownWithin(KILL_DELAY);

		logger.info("Shutting down NodeMaster");
		if (nodeMaster != null) {
			nodeMaster.shutdown();
		} else {
			logger.trace("NodeMaster was null");
		}

		logger.info("Closing logging server");
		if (loggingServer != null) {
			try {
				loggingServer.close();
			} catch (Exception e) {
				logger.debug("Caught exception while shutting down loggingServer. Was it not started?", e);
			}
		} else {
			logger.trace("loggingServer was null");
		}

		logger.info("Closing stage connection server");
		if (server != null) {
			try {
				server.shutdown();
				return;
			} catch (Exception e) {
				logger.debug("Caught exception while shutting down the server. Was it not started?", e);
				System.exit(1);
			}
		} else {
			logger.trace("server was null");
		}

		logger.info("Closing database connector");
		if (nodeMaster != null) {
			MongoConnector mongoConnector = (MongoConnector) nodeMaster.getDocumentIO().getDatabaseConnector();
			mongoConnector.disconnect();
		}
	}

	public boolean isShuttingDown() {
		return shuttingDown;
	}

	private void killUnlessShutdownWithin(long killDelay) {
		
		if (killDelay < 0) {
			return;
		}
		killer.kill(killDelay);
	}
	
	protected static CoreConfiguration getConfiguration(String fileName) {
		try {
			if (fileName != null) {
				return new FileConfiguration(fileName);
			} else {
				return new FileConfiguration();
			}
		} catch (ConfigurationException e) {
			logger.error("Unable to read configuration", e);
			return null;
		}
	}

	public void setKiller(HydraKiller killer) {
		this.killer = killer;
	}

	private class ShuttingDownOnUncaughtException implements UncaughtExceptionHandler {

		private final ShutdownHandler shutdownHandler;

		public ShuttingDownOnUncaughtException(ShutdownHandler shutdownHandler) {
			this.shutdownHandler = shutdownHandler;
		}

		@Override
		public void uncaughtException(Thread t, Throwable e) {
			if (!shutdownHandler.isShuttingDown()) {
				logger.error("Got an uncaught exception. Shutting down Hydra", e);
				shutdownHandler.shutdown();
			} else {
				logger.error("Got exception while shutting down", e);
			}
		}
		
	}

}
