package org.molgenis.downloader.api;

import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;
import org.molgenis.downloader.client.IncompleteMetadataException;

import javax.naming.AuthenticationException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

public interface MolgenisClient extends AutoCloseable
{

	void login(final String username, final String password, final Integer timeout) throws AuthenticationException;

	boolean logout();

	default void streamEntityData(final String name, final EntityConsumer consumer)
	{
		streamEntityData(name, consumer, null);
	}

	void streamEntityData(final String name, final EntityConsumer consumer, Integer pageSize);

	Entity getEntity(final String name) throws IOException, URISyntaxException;

	MetadataRepository getMetadata(MolgenisVersion version) throws IncompleteMetadataException;

	MolgenisVersion getVersion() throws IOException, URISyntaxException;

	MetadataRepository getFilteredMetadata(MolgenisVersion version, List<String> entities)
			throws IncompleteMetadataException;
}
