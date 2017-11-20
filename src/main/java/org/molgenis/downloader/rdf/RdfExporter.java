package org.molgenis.downloader.rdf;

import com.google.common.collect.Maps;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.molgenis.downloader.api.EntityConsumer;
import org.molgenis.downloader.api.MetadataRepository;
import org.molgenis.downloader.api.MolgenisClient;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;
import org.molgenis.downloader.client.IncompleteMetadataException;
import org.molgenis.rdf.RdfTemplate;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

/**
 * Streaming export of entity data from MolgenisClient to RDF format.
 */
public class RdfExporter
{
	/**
	 * The MolgenisClient to retrieve entity data
	 */
	private MolgenisClient molgenisClient;
	/**
	 * The RdfTemplate to use to obtain connections to the RDF repository.
	 */
	private RdfTemplate template;
	/**
	 * Function to use to create an EntityConsumer.
	 */
	private BiFunction<Entity, RepositoryConnection, EntityConsumer> consumerFactory;

	public RdfExporter(MolgenisClient molgenisClient, RdfTemplate template)
	{
		this(molgenisClient, template, RdfEntityConsumer::create);
	}

	RdfExporter(MolgenisClient molgenisClient, RdfTemplate template,
			BiFunction<Entity, RepositoryConnection, EntityConsumer> consumerFactory)
	{
		this.molgenisClient = requireNonNull(molgenisClient);
		this.template = requireNonNull(template);
		this.consumerFactory = requireNonNull(consumerFactory);
	}

	public void addNamespaces()
	{
		template.execute(connection -> this.addNamespaces(loadDefaultNamespaces(), connection));
	}

	public Map<String, String> loadDefaultNamespaces()
	{
		Properties properties = new Properties();
		try
		{
			properties.load(getClass().getResourceAsStream("namespaces.properties"));
		}
		catch (IOException ignore)
		{
		}
		return Maps.fromProperties(properties);
	}

	private void addNamespaces(Map<String, String> namespaces, RepositoryConnection connection)
	{
		namespaces.forEach(connection::setNamespace);
	}

	public void exportData(List<String> entities, Integer pageSize, MolgenisVersion molgenisVersion)
			throws IOException, URISyntaxException, IncompleteMetadataException
	{
		MetadataRepository metadata = molgenisClient.getMetadata(molgenisVersion);
		for (String entityId : entities)
		{
			Entity entity = metadata.getEntities()
									.stream()
									.filter(candidate -> candidate.getFullName().equals(entityId))
									.findFirst()
									.orElseThrow(() -> new IllegalArgumentException(
											"Entity with id " + entityId + " not found"));
			exportEntity(entity, pageSize);
		}
	}

	void exportEntity(Entity entity, Integer pageSize)
	{
		template.execute(connection -> molgenisClient.streamEntityData(entity.getFullName(),
				consumerFactory.apply(entity, connection), pageSize));
	}

	public void clear()
	{
		template.execute(RepositoryConnection::clear);
	}

}
