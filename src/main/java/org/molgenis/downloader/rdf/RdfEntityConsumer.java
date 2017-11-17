package org.molgenis.downloader.rdf;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;
import org.molgenis.downloader.api.EntityConsumer;
import org.molgenis.downloader.api.metadata.Attribute;
import org.molgenis.downloader.api.metadata.DataType;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.Tag;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

class RdfEntityConsumer implements EntityConsumer
{

	private final List<Attribute> attributes;
	private final Map<String, Attribute> attributesMap;
	private final Repository repository;

	public RdfEntityConsumer(final Entity entity, String pathToRdfFolder)
	{
		attributes = setAttributes(entity);
		attributesMap = attributes.stream().collect(toMap(Attribute::getName, a -> a));

		File dataDir = new File(pathToRdfFolder);
		repository = new SailRepository(new NativeStore(dataDir));
		repository.initialize();
	}

	@Override
	public void accept(Map<String, String> data)
	{
		final List<String> values = new ArrayList<>();
		for (int index = 0; index < getAttributeNames().size(); index++)
		{
			final String value = data.get(getAttributeNames().get(index));
			if (value != null && !value.trim().isEmpty())
			{
				values.add(value.trim());
			}
			else
			{
				values.add(null);
			}

		}

		data.forEach((name, value) ->
		{
			Attribute attribute = attributesMap.get(name);
			Set<Tag> tags = attribute.getTags();

			tags.forEach(tag ->
			{

			});

			if (value != null && !value.trim().isEmpty())
			{
				values.add(value.trim());
			}
			else
			{
				values.add(null);
			}
		});

		//		try
		//		{
		//			file.writeRow(values);
		//		}
		//		catch (final IOException ex)
		//		{
		//			writer.addException(ex);
		//		}
	}

	private Stream<String> getAttributeNames()
	{
		return attributes.stream().map(Attribute::getName);
	}

	private List<Attribute> getParts(final Attribute compound)
	{
		List<Attribute> atts = new ArrayList<>();
		compound.getParts().forEach((Attribute att) ->
		{
			if (att.getDataType().equals(DataType.COMPOUND))
			{
				atts.addAll(getParts(att));
			}
			else
			{
				atts.add(att);
			}
		});
		return atts;
	}

	private List<Attribute> setAttributes(final Entity entity)
	{
		List<Attribute> atts = new ArrayList<>();
		entity.getAttributes().forEach((Attribute att) ->
		{
			if (att.getDataType().equals(DataType.COMPOUND))
			{
				atts.addAll(getParts(att));
			}
			else
			{
				atts.add(att);
			}
		});
		return atts;
	}
}
