package org.molgenis.downloader.rdf;

import com.google.common.collect.ImmutableList;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.FOAF;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.mockito.*;
import org.mockito.quality.Strictness;
import org.molgenis.downloader.api.EntityConsumer;
import org.molgenis.downloader.api.MolgenisClient;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;
import org.molgenis.rdf.ConnectionCallback;
import org.molgenis.rdf.RdfTemplate;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URISyntaxException;
import java.util.function.BiFunction;

import static org.mockito.Mockito.*;

public class RdfExporterTest
{
	private RdfExporter rdfExporter;
	@Mock
	private MolgenisClient molgenisClient;
	@Mock
	private RdfTemplate rdfTemplate;
	@Mock
	private RepositoryConnection repositoryConnection;
	@Mock
	BiFunction<Entity, RepositoryConnection, EntityConsumer> consumerFactory;
	@Mock
	private EntityConsumer entityConsumer;
	@Mock
	private Entity samples;
	@Mock
	private Entity biobanks;
	@Mock
	private MolgenisVersion molgenisVersion;
	@Captor
	private ArgumentCaptor<ConnectionCallback> connectionCallbackCaptor;
	private Integer pageSize = 101;
	private MockitoSession mockitoSession;
	private SimpleValueFactory valueFactory = SimpleValueFactory.getInstance();

	@BeforeMethod
	public void beforeMethod() throws URISyntaxException
	{
		mockitoSession = Mockito.mockitoSession().initMocks(this).strictness(Strictness.STRICT_STUBS).startMocking();
		rdfExporter = new RdfExporter(molgenisClient, rdfTemplate, consumerFactory);
	}

	@AfterMethod
	public void afterMethod()
	{
		mockitoSession.finishMocking();
	}

	@Test
	public void testAddNamespaces()
	{
		doNothing().when(rdfTemplate).execute(connectionCallbackCaptor.capture());

		rdfExporter.addNamespaces();
		connectionCallbackCaptor.getValue().doInConnection(repositoryConnection);

		verify(repositoryConnection).setNamespace(RDF.PREFIX, RDF.NAMESPACE);
		verify(repositoryConnection).setNamespace(FOAF.PREFIX, FOAF.NAMESPACE);
	}

	@Test
	public void testExportData() throws Exception
	{
		doNothing().when(rdfTemplate).execute(connectionCallbackCaptor.capture());
		doReturn("bbmri_nl_biobanks").when(biobanks).getFullName();
		doReturn("bbmri_nl_sample_collections").when(samples).getFullName();
		doReturn(biobanks).when(molgenisClient).getEntity("bbmri_nl_biobanks");
		doReturn(samples).when(molgenisClient).getEntity("bbmri_nl_sample_collections");
		doReturn(entityConsumer).when(consumerFactory).apply(biobanks, repositoryConnection);
		doReturn(entityConsumer).when(consumerFactory).apply(samples, repositoryConnection);

		rdfExporter.exportData(ImmutableList.of("bbmri_nl_sample_collections", "bbmri_nl_biobanks"), pageSize,
				molgenisVersion);
		connectionCallbackCaptor.getAllValues().forEach(value -> value.doInConnection(repositoryConnection));

		verify(molgenisClient).streamEntityData("bbmri_nl_sample_collections", entityConsumer, pageSize);
		verify(molgenisClient).streamEntityData("bbmri_nl_biobanks", entityConsumer, pageSize);
	}

	@Test
	public void testExportEntity() throws Exception
	{
		doNothing().when(rdfTemplate).execute(connectionCallbackCaptor.capture());
		when(samples.getFullName()).thenReturn("bbmri_nl_sample_collections");
		when(consumerFactory.apply(samples, repositoryConnection)).thenReturn(entityConsumer);

		rdfExporter.exportEntity(samples, pageSize);
		connectionCallbackCaptor.getValue().doInConnection(repositoryConnection);

		verify(molgenisClient).streamEntityData("bbmri_nl_sample_collections", entityConsumer, pageSize);
	}

	@Test
	public void testClear()
	{
		rdfExporter.clear();
		verify(rdfTemplate).execute(connectionCallbackCaptor.capture());
		connectionCallbackCaptor.getValue().doInConnection(repositoryConnection);
		verify(repositoryConnection).clear();
	}
}