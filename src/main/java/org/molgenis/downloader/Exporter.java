package org.molgenis.downloader;

import org.molgenis.downloader.api.metadata.MolgenisVersion;

import java.nio.file.Path;
import java.util.List;

public interface Exporter
{
	boolean export(final List<String> entities, final Path path, final boolean includeMetadata, boolean overwrite,
			MolgenisVersion version, Integer pageSize) throws Exception;

	List<Exception> getExceptions();
}
