package org.molgenis.downloader.rdf;

import org.molgenis.downloader.api.*;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RdfFileWriter implements EMXWriter
{

	private final MolgenisVersion version;
	private final List<Exception> errors;
	private final EMXBackend backend;

	public RdfFileWriter(final EMXBackend store, final MolgenisVersion molgenisVersion)
	{
		version = molgenisVersion;
		errors = new ArrayList<>();
		backend = store;
	}

	@Override
	public EntityConsumer createConsumerForEntity(final Entity entity) throws IOException
	{
		return null;
	}

	@Override
	public MetadataConsumer createMetadataConsumer()
	{
		return null;
	}

	@Override
	public EMXDataStore createDataStore(String name) throws IOException
	{
		return backend.createDataStore(name);
	}

	@Override
	public boolean hasExceptions()
	{
		return !errors.isEmpty();
	}

	@Override
	public List<Exception> getExceptions()
	{
		return errors;
	}

	@Override
	public void addException(final Exception ex)
	{
		errors.add(ex);
	}
}
