package org.molgenis.downloader.client;

import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.MolgenisVersion;

import java.util.List;

import static org.molgenis.downloader.api.metadata.MolgenisVersion.VERSION_3;

public class EntityFilter
{
	private final List<String> entities;
	private MolgenisVersion version;

	public EntityFilter(final List<String> entities, MolgenisVersion version)
	{
		this.entities = entities;
		this.version = version;
	}

	public boolean isIncluded(Entity entity)
	{
		return !entities.contains(getEntityId(entity));
	}

	private String getEntityId(Entity ent)
	{
		if (version.smallerThan(VERSION_3))
		{
			return ent.getFullName();
		}
		else
		{
			return ent.getId();
		}
	}
}
