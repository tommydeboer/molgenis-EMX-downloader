package org.molgenis.downloader;

import org.molgenis.downloader.api.MolgenisClient;

import java.util.ArrayList;
import java.util.List;

public abstract class ExporterImpl implements Exporter
{
	protected final List<Exception> exceptions;
	protected final MolgenisClient molgenisClient;

	public ExporterImpl(MolgenisClient client)
	{
		this.molgenisClient = client;
		this.exceptions = new ArrayList<>();
	}

	public List<Exception> getExceptions()
	{
		return exceptions;
	}
}
