package org.molgenis.downloader.rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.ValueFactory;
import org.molgenis.downloader.api.EntityConsumer;
import org.molgenis.downloader.api.metadata.Attribute;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.Tag;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toMap;

class RdfEntityConsumer implements EntityConsumer
{

	private static final String MAIL_TO = "mailto:";
	private static final String IS_ASSOCIATED_WITH_URI = "http://molgenis.org#isAssociatedWith";

	private static final DatatypeFactory DATATYPE_FACTORY;

	static
	{
		try
		{
			DATATYPE_FACTORY = DatatypeFactory.newInstance();
		}
		catch (DatatypeConfigurationException e)
		{
			throw new Error("Could not instantiate javax.xml.datatype.DatatypeFactory", e);
		}
	}

	private final Map<String, Attribute> attributesMap;
	private final ValueFactory valueFactory;
	private final Consumer<Statement> consumer;
	private final String entityId;

	public RdfEntityConsumer(final Entity entity, ValueFactory valueFactory, Consumer<Statement> consumer)
	{
		entityId = entity.getId();
		attributesMap = getAttributes(entity).stream().collect(toMap(Attribute::getName, a -> a));

		this.valueFactory = valueFactory;
		this.consumer = consumer;
	}

	@Override
	public void accept(Map<String, String> data)
	{
		data.entrySet()
			.stream()
			.flatMap(entry -> createStatements(valueFactory, attributesMap.get(entry.getKey()), entry.getValue()))
			.forEach(consumer);
	}

	private Stream<Statement> createStatements(ValueFactory valueFactory, Attribute attribute, String value)
	{
		Set<Tag> tags = attribute.getTags();

		return tags.stream().filter(this::isAssociatedWithUri).flatMap(tag -> createStatement(attribute, tag, value));
	}

	private Stream<Statement> createStatement(Attribute attribute, Tag tag, String attributeValue)
	{
		IRI subject = valueFactory.createIRI(entityId);
		IRI predicate = valueFactory.createIRI(String.valueOf(tag.getObjectIRI()));

		List<Statement> statements = new ArrayList<>();

		switch (attribute.getDataType())
		{
			case MREF:
			case CATEGORICAL_MREF:
				if (attributeValue != null && !attributeValue.isEmpty())
				{
					asList(attributeValue.split(",")).forEach(id ->
					{
						Literal object = valueFactory.createLiteral(attributeValue);
						statements.add(valueFactory.createStatement(subject, predicate, object));
					});
				}
				break;
			case BOOL:
				Literal object = valueFactory.createLiteral(Boolean.valueOf(attributeValue));
				statements.add(valueFactory.createStatement(subject, predicate, object));
				break;
			case DATE:
				XMLGregorianCalendar calendar = DATATYPE_FACTORY.newXMLGregorianCalendar(attributeValue);
				Literal object = valueFactory.createLiteral(calendar);
				statements.add(valueFactory.createStatement(subject, predicate, object));
				break;
			case DATE_TIME:
				calendar = DATATYPE_FACTORY.newXMLGregorianCalendar(attributeValue);
				objectValue = valueFactory.createLiteral(calendar);
				break;
			case DECIMAL:
				objectValue = valueFactory.createLiteral(Double.valueOf(attributeValue));
				break;
			case LONG:
				objectValue = valueFactory.createLiteral(Long.valueOf(attributeValue));
				break;
			case INT:
				objectValue = valueFactory.createLiteral(Integer.valueOf(attributeValue));
				break;
			case ENUM:
			case HTML:
			case TEXT:
			case SCRIPT:
			case STRING:
				objectValue = valueFactory.createLiteral(attributeValue);
				break;
			case EMAIL:
				objectValue = valueFactory.createIRI(MAIL_TO + attributeValue);
				break;
			case HYPERLINK:
				objectValue = valueFactory.createIRI(attributeValue);
				break;
			case XREF:
			case CATEGORICAL:
			case FILE:
				// TODO how?
				break;
			default:
				throw new IllegalArgumentException("DataType " + attribute.getDataType() + "is not supported");
		}

		return objectValue;
	}

	private boolean isAssociatedWithUri(Tag tag)
	{
		boolean equalsAssociatedWith = false;
		try
		{
			equalsAssociatedWith = tag.getRelationIRI().equals(new URI(IS_ASSOCIATED_WITH_URI));
		}
		catch (URISyntaxException e)
		{
			e.printStackTrace();
		}
		return equalsAssociatedWith;
	}
}
