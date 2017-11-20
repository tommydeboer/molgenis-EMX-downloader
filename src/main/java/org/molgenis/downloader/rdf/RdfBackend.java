package org.molgenis.downloader.rdf;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.nativerdf.NativeStore;

import java.io.File;
import java.nio.file.Path;

public class RdfBackend
{
	public Repository createRepository(Path path)
	{
		File dataDir = path.toFile();
		String indexes = "spoc,posc,cosp";
		Repository repo = new SailRepository(new NativeStore(dataDir, indexes));
		repo.initialize();
		return repo;
	}
}
