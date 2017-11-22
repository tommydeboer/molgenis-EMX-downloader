package org.molgenis.downloader.rdf;

import com.google.common.io.Files;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.rio.turtle.TurtleWriter;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;

public class RdfBackend implements AutoCloseable
{
	private static final String INDEXES = "spoc,posc,cosp";
	private final Repository repository;
	private final File outputPath;
	private final File storeDir;

	public RdfBackend(Path path, boolean overwrite) throws FileAlreadyExistsException
	{
		if (path.toFile().exists() && !overwrite)
		{
			throw new FileAlreadyExistsException(path.toString(), null, "File already exists.");
		}

		this.outputPath = path.toFile();

		storeDir = Files.createTempDir();
		storeDir.deleteOnExit();

		repository = createRepository();
	}

	private Repository createRepository()
	{
		Repository sailRepository = new SailRepository(new NativeStore(storeDir, INDEXES));
		sailRepository.initialize();
		return sailRepository;
	}

	public Repository getRepository()
	{
		return repository;
	}

	public void export() throws IOException
	{
		try (FileOutputStream out = new FileOutputStream(outputPath))
		{
			repository.getConnection().export(new TurtleWriter(out));
		}
	}

	@Override
	public void close() throws Exception
	{
		repository.shutDown();
	}
}