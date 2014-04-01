package com.findwise.hydra.admin.configuration;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.findwise.hydra.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.findwise.hydra.admin.database.AdminServiceType;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationServiceTest {

	@Mock
	private DatabaseConnector<AdminServiceType> connector;

	@Mock
	private PipelineReader pipelineReader;
	
	@Mock
	private PipelineWriter pipelineWriter;

	@Mock
	private StatusReader<AdminServiceType> statusReader;

	@Mock
	private PipelineStatus<AdminServiceType> pipelineStatus;
	
	@Mock
	private Pipeline pipeline;

	@Mock
	private Pipeline debugPipeline;
	
	@Mock
	private DocumentReader<AdminServiceType> documentReader;
	
	@Mock
	private DocumentWriter<AdminServiceType> documentWriter;

	private ConfigurationService<AdminServiceType> service;

	@Before
	public void setUp() throws Exception {
		when(documentReader.getActiveDatabaseSize()).thenReturn(1L);
		when(documentReader.getInactiveDatabaseSize()).thenReturn(1L);
		
		when(pipelineReader.getPipeline()).thenReturn(pipeline);
		when(pipelineReader.getDebugPipeline()).thenReturn(debugPipeline);

		when(statusReader.getStatus()).thenReturn(pipelineStatus);
		
		when(connector.getPipelineReader()).thenReturn(pipelineReader);
		when(connector.getPipelineWriter()).thenReturn(pipelineWriter);
		when(connector.getStatusReader()).thenReturn(statusReader);
		when(connector.getDocumentReader()).thenReturn(documentReader);
		when(connector.getDocumentWriter()).thenReturn(documentWriter);
		service = new ConfigurationService<AdminServiceType>(connector);
	}

	@Test
	public void testGetStats() throws DatabaseException {
		Map<String, Object> stats = service.getStats();

		verify(pipelineReader).getPipeline();
		verify(pipelineReader).getDebugPipeline();

		verify(documentReader).getActiveDatabaseSize();
		verify(documentReader).getInactiveDatabaseSize();

		assertTrue(stats.containsKey("documents"));
		assertTrue(stats.containsKey("groups"));
		assertEquals(2, stats.size());
	}
}
