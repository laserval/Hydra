package com.findwise.hydra;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.findwise.hydra.mongodb.MongoType;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.findwise.hydra.mongodb.MongoConfiguration;
import com.findwise.hydra.mongodb.MongoConnector;
import com.findwise.hydra.mongodb.MongoDocument;
import com.findwise.hydra.mongodb.MongoDocumentIO;
import com.findwise.hydra.mongodb.MongoQuery;
import com.findwise.hydra.mongodb.MongoTailableIterator;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class FullScaleIT {
	private final boolean useOneStageGroupPerStage;
	private final boolean cacheIsEnabled;
	private final String name;
	Logger logger = LoggerFactory.getLogger(FullScaleIT.class);

	@Parameters(name = "useOneStageGroupPerStage={0},cacheIsEnabled={1}")
	public static Iterable<Object[]> testParameters() {
		// Not the most intuitive API here.
		final boolean cacheIsEnabled = true, useOneStageGroupPerStage = true;
		final boolean cacheIsDisabled = false, useOneStageGroupForAllStages = false;
		return Arrays.asList(
				new Object[][]{
						{useOneStageGroupPerStage, cacheIsEnabled},
						{useOneStageGroupPerStage, cacheIsDisabled},
						{useOneStageGroupForAllStages, cacheIsEnabled},
						{useOneStageGroupForAllStages, cacheIsDisabled}
				}
		);
	}

	public FullScaleIT(boolean useOneStageGroupPerStage, boolean cacheIsEnabled) {
		this.useOneStageGroupPerStage = useOneStageGroupPerStage;
		this.cacheIsEnabled = cacheIsEnabled;
		this.name = String.format("hydra-test-FullScaleIT-%s-%s", useOneStageGroupPerStage, cacheIsEnabled);
	}

	MongoConfiguration mongoConfiguration;
	MongoConnector mongoConnector;
	private Main core;

	@Before
	public void setUp() throws Exception {
		mongoConfiguration = new MongoConfiguration();
		mongoConfiguration.setNamespace(name);
		mongoConnector = new MongoConnector(mongoConfiguration);
		mongoConnector.connect();

		mongoConnector.getDB().dropDatabase();

		// Because I don't trust MongoConnector after the database has been destroyed.
		mongoConnector = new MongoConnector(mongoConfiguration);
		mongoConnector.connect();

		// Initialize core, but don't start until test wants to.
		CoreMapConfiguration coreConfiguration = new CoreMapConfiguration(mongoConfiguration, new MapConfiguration());
		coreConfiguration.setCaching(cacheIsEnabled);
		core = new Main(coreConfiguration);
		core.setKiller(new NoopHydraKiller());
	}

	@After
	public void tearDown() throws Exception {
		core.shutdown();
		mongoConnector.getDB().dropDatabase();
	}

	// A reasonable setting for this timeout is unfortunately very dependent on the
	// performance of the machine running the test. Setting it very high to avoid
	// random failures on TravisCI
	@SuppressWarnings("unchecked")
	@Test(timeout = 60000)
	public void testAPrimitivePipelineWorks() throws Exception {
		// Add libraries, using the filename as the library id. These jars should
		// be on the classpath, having been copied there by maven during the "package"
		// phase.
		uploadJar("hydra-basic-stages-jar-with-dependencies.jar");
		uploadJar("integration-test-stages-jar-with-dependencies.jar");

		createPrimitivePipeline();

		// We start the core after we've inserted the stages and libraries so
		// we don't have to wait for it to poll for updates.
		core.startup();

		// Next, we add documents with a field "externalDocId" to let us identify them
		Set<String> externalDocumentIds = createDocuments(3);

		// Now we just have to wait for all documents to end up in the "oldDocuments" repository
		MongoTailableIterator inactiveIterator = mongoConnector.getDocumentReader().getInactiveIterator(new MongoQuery());

		Set<String> finishedDocumentIds = new HashSet<String>();
		while(!finishedDocumentIds.equals(externalDocumentIds)) {
			if(inactiveIterator.hasNext()) {
				MongoDocument finishedDocument = inactiveIterator.next();
				logger.info("Found finished document " + finishedDocument);
				// Assert that the document was successfully processed
				assertThat(finishedDocument.getStatus(), equalTo(Document.Status.PROCESSED));
				// Here we assert that we indeed have passed through the fieldSetter stage
				assertThat((String) finishedDocument.getContentField("setField"), equalTo("Set by fieldSetter"));
				// Assert that the fieldModifier changed the correct fields
				Object overwrittenField = finishedDocument.getContentField("overwrittenField");
				assertThat(overwrittenField, is(instanceOf(String.class)));
				assertTrue("overwrittenField should have a single value", overwrittenField instanceof String);
				assertThat((String) finishedDocument.getContentField("overwrittenField"), equalTo("Overwritten by fieldOverwriter"));
				Object modifiedField = finishedDocument.getContentField("modifiedField");
				assertThat(modifiedField, is(instanceOf(List.class)));
				assertTrue("modifiedField should have multiple values", modifiedField instanceof List);
				assertThat((Iterable<String>) finishedDocument.getContentField("modifiedField"), hasItems("Set by fieldSetter", "Modified by fieldModifier"));
				Object attachmentContentsField = finishedDocument.getContentField("attachmentContents");
				assertThat(attachmentContentsField, is(instanceOf(List.class)));
				assertTrue("modifiedField should have multiple values", attachmentContentsField instanceof List);
				assertThat((Iterable<String>) attachmentContentsField, hasItems("attachment 1 contents", "attachment 2 contents"));
				finishedDocumentIds.add((String) finishedDocument.getContentField("externalDocId"));
			} else {
				// Wait for a little while before polling again.
				Thread.sleep(100);
			}
		}
	}

	private Set<String> createDocuments(int numDocs) throws IOException {
		MongoDocumentIO mongoDocumentIO = buildMongoDocumentIO(mongoConfiguration);
		Set<String> externalDocumentIds = new HashSet<String>();
		for(int i = 0; i < numDocs; i++) {
			String externalDocId = UUID.randomUUID().toString();
			addDocument(externalDocId, mongoDocumentIO);
			externalDocumentIds.add(externalDocId);
		}
		return externalDocumentIds;
	}

	private void addDocument(String externalDocId, MongoDocumentIO mongoDocumentIO) throws IOException {
		MongoDocument mongoDocument = new MongoDocument();
		mongoDocument.putContentField("externalDocId", externalDocId);
		InputStream attachment1InputStream = new ByteArrayInputStream(
				"attachment 1 contents".getBytes(Charset.forName("UTF-8")));
		InputStream attachment2InputStream = new ByteArrayInputStream(
				"attachment 2 contents".getBytes(Charset.forName("UTF-8")));
		List<DocumentFile<MongoType>> attachments = new ArrayList<DocumentFile<MongoType>>();
		attachments.add(new DocumentFile<MongoType>(null, "attachment1-test-file.txt", attachment1InputStream));
		attachments.add(new DocumentFile<MongoType>(null, "attachment2-test-file.txt", attachment2InputStream));
		mongoDocumentIO.insert(mongoDocument, attachments);
	}

	/**
	 * 	Creates a small linear pipeline
	 */
	private void createPrimitivePipeline() throws Exception {
		final String setField = "setField";
		final String overwrittenField = "overwrittenField";
		final String modifiedField = "modifiedField";
		// fieldSetter
		Map<String, Object> fieldSetterFieldValueMap = new HashMap<String, Object>();
		fieldSetterFieldValueMap.put(setField, "Set by fieldSetter");
		fieldSetterFieldValueMap.put(overwrittenField, "Set by fieldSetter");
		fieldSetterFieldValueMap.put(modifiedField, "Set by fieldSetter");
		Map<String, Object> fieldSetterParams = new HashMap<String, Object>();
		fieldSetterParams.put("fieldValueMap", fieldSetterFieldValueMap);
		fieldSetterParams.put("overwritePolicy", "ADD");
		// fieldModifier
		Map<String, Object> fieldModifierFieldValueMap = new HashMap<String, Object>();
		fieldModifierFieldValueMap.put(modifiedField, "Modified by fieldModifier");
		Map<String, Object> fieldModifierParams = new HashMap<String, Object>();
		fieldModifierParams.put("fieldValueMap", fieldModifierFieldValueMap);
		fieldModifierParams.put("overwritePolicy", "ADD");
		// fieldOverwriter
		Map<String, Object> fieldOverwriterFieldValueMap = new HashMap<String, Object>();
		fieldOverwriterFieldValueMap.put(overwrittenField, "Overwritten by fieldOverwriter");
		Map<String, Object> fieldOverwriterParams = new HashMap<String, Object>();
		fieldOverwriterParams.put("fieldValueMap", fieldOverwriterFieldValueMap);
		fieldOverwriterParams.put("overwritePolicy", "OVERWRITE");
		new LinearPipelineBuilder().
			addStages(
				new StageBuilder()
					.stageName("initRequired")
					.className("com.findwise.hydra.stage.InitRequiredStage")
					.libraryId("integration-test-stages-jar-with-dependencies.jar")
					.build(),
				new StageBuilder()
					.stageName("fieldSetter")
					.className("com.findwise.hydra.stage.SetStaticFieldStage")
					.libraryId("hydra-basic-stages-jar-with-dependencies.jar")
					.stageProperties(fieldSetterParams).build(),
				new StageBuilder()
					.stageName("fieldModifier")
					.className("com.findwise.hydra.stage.SetStaticFieldStage")
					.libraryId("hydra-basic-stages-jar-with-dependencies.jar")
					.stageProperties(fieldOverwriterParams).build(),
				new StageBuilder()
					.stageName("fieldOverwriter")
					.className("com.findwise.hydra.stage.SetStaticFieldStage")
					.libraryId("hydra-basic-stages-jar-with-dependencies.jar")
					.stageProperties(fieldModifierParams).build(),
				new StageBuilder()
					.stageName("attachmentReader")
					.className("com.findwise.hydra.stage.AttachmentReaderStage")
					.libraryId("integration-test-stages-jar-with-dependencies.jar")
					.build(),
				new StageBuilder()
					.stageName("nullOutput")
					.className("com.findwise.hydra.stage.NullOutputStage")
					.libraryId("integration-test-stages-jar-with-dependencies.jar").build()
			)
			.useOneStageGroupPerStage(useOneStageGroupPerStage)
			// N.B. stageGroupName is only used if useOneStageGroupPerStage is set to false
			.stageGroupName(FullScaleIT.class.getName())
			.buildAndSave(mongoConnector);
	}

	private MongoDocumentIO buildMongoDocumentIO(MongoConfiguration mongoConfiguration) throws UnknownHostException {
		MongoClient mongo = new MongoClient(new MongoClientURI(mongoConfiguration.getDatabaseUrl()));
		DB db = mongo.getDB(mongoConfiguration.getNamespace());
		WriteConcern concern = mongo.getWriteConcern();
		long documentsToKeep = mongoConfiguration.getOldMaxCount();
		int oldDocsMaxSizeMB = mongoConfiguration.getOldMaxSize();
		StatusUpdater updater = new StatusUpdater(new MongoConnector(mongoConfiguration));
		GridFS documentFs = new GridFS(db, MongoDocumentIO.DOCUMENT_FS);

		MongoDocumentIO io = new MongoDocumentIO(db, concern, documentsToKeep,
			oldDocsMaxSizeMB, updater, documentFs);
		io.prepare();
		return io;
	}

	private void uploadJar(String jarFileName) {
		InputStream resourceAsStream = getClass().getResourceAsStream("/" + jarFileName);
		assert(resourceAsStream != null);
		mongoConnector.getPipelineWriter().save(jarFileName, jarFileName, resourceAsStream);
	}
}
