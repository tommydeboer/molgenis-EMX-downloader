package org.molgenis.downloader.rdf;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.molgenis.downloader.api.EntityConsumer;
import org.molgenis.downloader.api.metadata.Attribute;
import org.molgenis.downloader.api.metadata.DataType;
import org.molgenis.downloader.api.metadata.Entity;
import org.molgenis.downloader.api.metadata.Tag;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * {@link EntityConsumer} that creates {@link Statement}s for an entity
 */
class RdfEntityConsumer implements EntityConsumer
{
	static final String DEFAULT_NAME_SPACE = "http://molgenis.org/"; //TODO: make configurable
	static final String IS_ASSOCIATED_WITH_URI = "http://molgenis.org#isAssociatedWith";
	static final DatatypeFactory DATATYPE_FACTORY;

	static
	{
		try
		{
			DATATYPE_FACTORY = DatatypeFactory.newInstance();
		}
		catch (DatatypeConfigurationException e)
		{
			throw new InstantiationError("Could not instantiate javax.xml.datatype.DatatypeFactory");
		}
	}

	private static final String MAIL_TO = "mailto:";

	private final ValueFactory valueFactory;
	private final Consumer<Statement> statements;
	private final Entity entity;
	private final Map<String, Attribute> attributesMap;

	/**
	 * Creates an {@link EntityConsumer} for an {@link Entity} and a {@link RepositoryConnection}
	 *
	 * @param entity     {@link Entity} describing the metadata for the data entities we should consume
	 * @param connection {@link RepositoryConnection} to get {@link ValueFactory} from and add {@link Statement}s to.
	 * @return newly created {@link EntityConsumer}
	 */
	public static EntityConsumer create(Entity entity, RepositoryConnection connection)
	{
		return new RdfEntityConsumer(entity, connection.getValueFactory(), connection::add);
	}

	RdfEntityConsumer(final Entity entity, ValueFactory valueFactory, Consumer<Statement> statements)
	{
		this.valueFactory = valueFactory;
		this.statements = statements;
		this.entity = entity;
		this.attributesMap = getAttributes(entity).stream().distinct().collect(toMap(Attribute::getName, a -> a));
	}

	@Override
	public void accept(Map<String, String> data)
	{
		IRI subject = createIRI(DEFAULT_NAME_SPACE, entity.getFullName(), data.get(getIdAttribute(entity).getName()));
		data.entrySet()
			.stream()
			.filter(entry -> entry.getValue() != null)
			.forEach(entry -> createStatements(subject, entry.getKey(), entry.getValue()));
	}

	private IRI createIRI(String namespace, String entityTypeId, String entityId)
	{
		return valueFactory.createIRI(namespace, String.format("%s/%s", entityTypeId, entityId));
	}

	private Attribute getIdAttribute(Entity entity)
	{
		return getAttributes(entity).stream()
									.filter(Attribute::isIdAttribute)
									.findFirst()
									.orElseThrow(
											() -> new IllegalStateException("Entity " + entity + "has no idAttribute"));
	}

	private void createStatements(IRI subject, String attributeName, String attributeValue)
	{
		Attribute attribute = attributesMap.get(attributeName);
		Set<Tag> attributeTags = attribute.getTags();
		attributeTags.stream()
					 .filter(this::isAssociatedWithUri)
					 .forEach(tag -> createAttributeValueStatement(subject, attribute, tag.getObjectIRI(),
							 attributeValue));
	}

	private void createAttributeValueStatement(IRI subject, Attribute attribute, URI tagObjectIRI,
			String attributeValue)
	{
		IRI predicate = valueFactory.createIRI(String.valueOf(tagObjectIRI));
		DataType dataType = attribute.getDataType();
		switch (dataType)
		{
			case BOOL:
			case DATE:
			case DATE_TIME:
			case DECIMAL:
			case LONG:
			case INT:
			case ENUM:
			case HTML:
			case TEXT:
			case SCRIPT:
			case STRING:
			case EMAIL:
			case HYPERLINK:
			case XREF:
			case CATEGORICAL:
				statements.accept(
						valueFactory.createStatement(subject, predicate, createObjectValue(attribute, attributeValue)));
				break;
			case MREF:
			case CATEGORICAL_MREF:
				streamMref(attributeValue).map(id -> createObjectValue(attribute, id))
										  .map(object -> valueFactory.createStatement(subject, predicate, object))
										  .forEach(statements);
				break;
			default:
				throw new IllegalArgumentException("DataType " + dataType + "is not supported");
		}
	}

	private Value createObjectValue(Attribute attribute, String attributeValue)
	{
		switch (attribute.getDataType())
		{
			case BOOL:
				return valueFactory.createLiteral(Boolean.valueOf(attributeValue));
			case DATE:
				return valueFactory.createLiteral(
						DATATYPE_FACTORY.newXMLGregorianCalendar(LocalDate.parse(attributeValue).toString()));
			case DATE_TIME:
				return valueFactory.createLiteral(
						DATATYPE_FACTORY.newXMLGregorianCalendar(Instant.parse(attributeValue).toString()));
			case DECIMAL:
				return valueFactory.createLiteral(Double.valueOf(attributeValue));
			case LONG:
				return valueFactory.createLiteral(Long.valueOf(attributeValue));
			case INT:
				return valueFactory.createLiteral(Integer.valueOf(attributeValue));
			case ENUM:
			case HTML:
			case TEXT:
			case SCRIPT:
			case STRING:
				return valueFactory.createLiteral(attributeValue);
			case EMAIL:
				return valueFactory.createIRI(MAIL_TO + attributeValue);
			case HYPERLINK:
				return valueFactory.createIRI(attributeValue);
			case XREF:
			case CATEGORICAL:
			case MREF:
			case CATEGORICAL_MREF:
				return createIRI(DEFAULT_NAME_SPACE, attribute.getRefEntity().getId(), attributeValue);
			default:
				throw new IllegalArgumentException("Unknown attribute type" + attribute.getDataType());
		}
	}

	private Stream<String> streamMref(String attributeValue)
	{
		return Optional.ofNullable(attributeValue)
					   .map(Stream::of)
					   .orElseGet(Stream::empty)
					   .map(value -> value.split(","))
					   .flatMap(Arrays::stream);
	}

	private boolean isAssociatedWithUri(Tag tag)
	{
		return tag.getRelationIRI().toString().equals(IS_ASSOCIATED_WITH_URI);
	}
}
