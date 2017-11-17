package org.molgenis.downloader.rdf;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.molgenis.downloader.ExporterImpl;
import org.molgenis.downloader.api.MetadataRepository;
import org.molgenis.downloader.api.MolgenisClient;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

public class RdfClient extends ExporterImpl
{
	private MetadataRepository metadataRepository;

	public RdfClient(final MolgenisClient client)
	{
		super(client);
	}

	@Override
	public boolean export(List<String> entities, Path path, boolean includeMetadata, boolean overwrite,
			MolgenisVersion version, Integer pageSize) throws Exception
	{
		Repository repo = createRepository(path);
		try(RepositoryConnection connection = repo.getConnection())
		{
			molgenisClient.streamMetadata(this::setMetadataRepository, version);
			for (Entity entity : metadataRepository.getEntities())
			{
				RdfEntityConsumer entityConsumer = new RdfEntityConsumer(entity, connection.getValueFactory(), connection::add);
				// TODO: Unsure if getId() works for low molgenis version (Hint: IT SHOULD!)
				molgenisClient.streamEntityData(entity.getId(), entityConsumer, pageSize);
			}
		}
		return true;
	}

	private Repository createRepository(Path path)
	{
		File dataDir = path.toFile();
		String indexes = "spoc,posc,cosp";
		Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
		repo.initialize();
		return repo;
	}

	public void setMetadataRepository(MetadataRepository metadataRepository)
	{
		this.metadataRepository = metadataRepository;
	}
}
