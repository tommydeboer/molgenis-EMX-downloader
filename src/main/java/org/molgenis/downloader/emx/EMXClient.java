package org.molgenis.downloader.emx;

import org.molgenis.downloader.ExporterImpl;
import org.molgenis.downloader.api.*;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;
import org.molgenis.downloader.client.EntityFilter;
import org.molgenis.downloader.emx.excel.ExcelBackend;
import org.molgenis.downloader.emx.tsv.ZipFileBackend;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class EMXClient extends ExporterImpl
{

	private static final String XLSX = ".xlsx";
	private static final String XLS = ".xls";

	public EMXClient(final MolgenisClient client)
	{
		super(client);
	}

	public boolean export(final List<String> entities, final Path path, final boolean includeMetadata,
			boolean overwrite, MolgenisVersion version, Integer pageSize) throws Exception
	{
		try (final EMXBackend backend = createBackend(path, overwrite))
		{
			final EMXFileWriter writer = new EMXFileWriter(backend, version);
			List<String> target = entities;
			if (includeMetadata)
			{
				MetadataRepository filteredMetadata = molgenisClient.getFilteredMetadata(version, entities);
				writeMetadata(writer, filteredMetadata);
				target = getDatasheets(entities, version, filteredMetadata);
			}
			for (final String name : target)
			{
				writeData(pageSize, writer, name);
			}
			exceptions.addAll(writer.getExceptions());
			return writer.hasExceptions();
		}
	}

	private void writeMetadata(EMXFileWriter writer, MetadataRepository filteredMetadata) throws Exception
	{
		try (final MetadataConsumer consumer = writer.createMetadataConsumer())
		{
			consumer.accept(filteredMetadata);
		}
	}

	private void writeData(Integer pageSize, EMXFileWriter writer, String name) throws Exception
	{
		final EntityConsumer consumer = writer.createConsumerForEntity(molgenisClient.getEntity(name));
		try
		{
			molgenisClient.streamEntityData(name, consumer, pageSize);
		}
		catch (final org.json.JSONException ex)
		{
			writer.addException(new IllegalArgumentException("entity: " + name + " does not exist", ex));
		}
	}

	private List<String> getDatasheets(List<String> entities, MolgenisVersion version,
			MetadataRepository filteredMetadata)
	{
		EntityFilter filter = new EntityFilter(entities, version);
		return filteredMetadata.getEntities()
							   .stream()
							   .filter(filter::isIncluded)
							   .filter(ent -> !ent.isAbstractClass())
							   .map(Entity::getFullName)
							   .distinct()
							   .collect(toList());
	}

	private EMXBackend createBackend(final Path path, boolean overwrite) throws IOException, URISyntaxException
	{
		final EMXBackend backend;
		if (path.toString().endsWith(XLSX) || path.toString().endsWith(XLS))
		{
			backend = new ExcelBackend(path, overwrite);
		}
		else
		{
			boolean fileExists = path.toFile().exists();
			if (!fileExists || overwrite)
			{
				if (fileExists)
				{
					Files.delete(path);
				}
				backend = new ZipFileBackend(path);
			}
			else
			{
				throw new FileAlreadyExistsException(
						String.format("File %s already exists, please use the '-o' option to overwrite.", path));
			}
		}
		return backend;
	}
}
