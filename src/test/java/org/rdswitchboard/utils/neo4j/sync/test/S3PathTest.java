package org.rdswitchboard.utils.neo4j.sync.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;
import org.rdswitchboard.utils.neo4j.sync.s3.S3Path;

public class S3PathTest {
	
	public static final String TEST_PATH = "S3://neo4j.rdswitchboard/2015-11-23/neo4j.zip";
	public static final String TEST_BROKEN_PATH = "s://neo4j.rdswitchboard";
	public static final String TEST_BUCKET = "neo4j.rdswitchboard";
	public static final String TEST_KEY = "2015-11-23/neo4j.zip";
	
	public static final String TEST_SOURCE = "jenkins/sync_9076357384926881382/neo4j-target/README.txt";
	public static final String TEST_ROOT = "jenkins/sync_9076357384926881382/neo4j-target";
	public static final String TEST_RELATIVE = "README.txt";
	public static final String TEST_NEW_ROOT = "neo4j";
	public static final String TEST_NEW_PATH = "neo4j/README.txt";
			
	
	
	@Test
	public void testS3Path() {
		
		S3Path path = S3Path.parse(TEST_PATH);
		
		assertNotNull("Must be able to parse correct S3 Path", 
				path);
		
		assertEquals("Must have detected correct S3 Bucket", 
				TEST_BUCKET,
				path.getBucket());

		assertEquals("Must have detected correct S3 Key", 
				TEST_KEY,
				path.getKey());

		assertNull("Must be able to ignore incorrect S3 Paths", 
				S3Path.parse(TEST_BROKEN_PATH));		
	}
	
	@Test
	public void TestRelativePaths() {
		Path source = Paths.get(TEST_SOURCE);
		Path root = Paths.get(TEST_ROOT);
		
		assertNotNull("Must be able to parse the relative path", 
				source);
		assertNotNull("Must be able to parse the root path", 
				root);
		
		Path relative = root.relativize(source);
		
		assertNotNull("Must be able to relativize path", 
				root);
		assertEquals("The relative puth must be correct", 
				TEST_RELATIVE,
				relative.toString());

		Path local = Paths.get(TEST_NEW_ROOT, relative.toString());
		
		assertNotNull("Must be able to conjure path", 
				root);
		assertEquals("New path must be correct", 
				TEST_NEW_PATH,
				local.toString());
	}
}
