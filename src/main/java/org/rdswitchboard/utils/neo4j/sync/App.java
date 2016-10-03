package org.rdswitchboard.utils.neo4j.sync;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.rdswitchboard.utils.neo4j.sync.enums.Relationships;
import org.rdswitchboard.utils.neo4j.sync.enums.Types;
import org.rdswitchboard.utils.neo4j.sync.exceptions.Neo4jException;
import org.rdswitchboard.utils.neo4j.sync.s3.S3Path;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class App {
	private static final String DEF_PATH_TMP = "tmp";
	private static final String DEF_PATH_ZIP = ".zip";
	private static final String DEF_SYNC_HOME = "sync";
	private static final String DEF_SYNC_PREFIX = "sync_";
	private static final String DEF_KEYS_LIST = "keys.list";
	private static final String DEF_SYNC_LEVEL = "3";	
	private static final String DEF_SOURCE_DB = "neo4j-source";
	private static final String DEF_TARGET_DB = "neo4j-target";
//	private static final String DEF_NEO4J_DB = "neo4j";
//	private static final String DEF_NEO4J_ZIP = "neo4j.zip";
	
	private static final String PROPERTY_KEY = "key";
	private static final String PROPERTY_SOURCE = "node_source";
	private static final String PROPERTY_TYPE = "node_type";
		
	private static GraphDatabaseService srcGraphDb;
	private static GraphDatabaseService dstGraphDb;
	
	private static Path work;	
	private static Set<String> keys; 
	private static AmazonS3 s3client;
	private static Map<Long, Long> mapImported;
				
	private static int syncLevel;
	private static long processedCounter = 0;
	private static long nodeCounter = 0;
	private static long relCounter = 0;
	private static long chunksCounter = 0;
	private static long chunkSize = 0;
	
	public static final String NEO4J_CONF = "/conf/neo4j.conf";
	public static final String NEO4J_DB = "/data/databases/graph.db";
	
	public static File GetDbPath(final String folder) throws Neo4jException, IOException
	{
		File db = new File(folder, NEO4J_DB);
		if (!db.exists())
			db.mkdirs();
				
		if (!db.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return db;
	}
	
	public static File GetConfPath(final String folder) throws Neo4jException
	{
		File conf = new File(folder, NEO4J_CONF);
		if (!conf.exists() || conf.isDirectory())
			throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");
		
		return conf;
	}	
	
	public static GraphDatabaseService getReadOnlyGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.setConfig( GraphDatabaseSettings.read_only, "true" )
				.newGraphDatabase();
			
			registerShutdownHook( graphDb );
			
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	public static GraphDatabaseService getGraphDb( final String graphDbPath ) throws Neo4jException {
		if (StringUtils.isEmpty(graphDbPath))
			throw new Neo4jException("Please provide path to an existing Neo4j instance");
		
		try {
			GraphDatabaseService graphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(graphDbPath) )
				.loadPropertiesFromFile( GetConfPath(graphDbPath).toString() )
				.newGraphDatabase();
		
			registerShutdownHook( graphDb );
		
			return graphDb;
		} catch (Exception e) {
			throw new Neo4jException("Unable to open Neo4j instance located at: " + graphDbPath + ". Error: " + e.getMessage());
		}
	}
	
	public static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    });
	}


	public static void main(String[] args) {
		try {
			Properties properties = Configuration.fromArgs(args);
	        
	        System.out.println("Sync Neo4j database");

	        String syncHome = properties.getProperty(Configuration.PROPERTY_SYNC_HOME, DEF_SYNC_HOME);
	        if (StringUtils.isEmpty(syncHome))
	            throw new IllegalArgumentException("The Sync Home can not be empty");
	        System.out.println("Home: " + syncHome);

	        String source = properties.getProperty(Configuration.PROPERTY_SYNC_SOURCE);
	        if (StringUtils.isEmpty(source))
	            throw new IllegalArgumentException("Source Neo4j can not be empty");
	        System.out.println("Nexus Neo4j: " + source);

	        String target = properties.getProperty(Configuration.PROPERTY_SYNC_TARGET);
	        if (StringUtils.isEmpty(target))
	            throw new IllegalArgumentException("Target Neo4j can not be empty");
	        System.out.println("Input Neo4j: " + target);

	        String bucket = properties.getProperty(Configuration.PROPERTY_SYNC_BUCKET);
	        
	        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	        String drop = "neo4j-enriched-" + dateFormat.format(new Date());

	        if (!StringUtils.isEmpty(bucket))
	        	System.out.println("Output Neo4j: s3://" + bucket + "/" + drop + ".zip");

	        syncLevel = Integer.parseInt(properties.getProperty(Configuration.PROPERTY_SYNC_LEVEL, DEF_SYNC_LEVEL));
	        
	        String keysList = properties.getProperty(Configuration.PROPERTY_SYNC_KEYS, DEF_KEYS_LIST);

	        keys = new HashSet<String>();
	        keys.add(PROPERTY_KEY);
	        
	        File keysFile = new File(keysList);
			if (keysFile.isFile()) {
				List<String> list = FileUtils.readLines(keysFile);
				for (String l : list) {
					String s = l.trim();
					if (!s.isEmpty())// && !s.equals(GraphUtils.PROPERTY_KEY))
						keys.add(s); 
				}
			}

			
			mapImported = new HashMap<Long, Long>();
						
	        s3client = new AmazonS3Client(new InstanceProfileCredentialsProvider());

	        Path home = Paths.get(syncHome);
	        Files.createDirectories(home);
	        work = Files.createTempDirectory(home, DEF_SYNC_PREFIX);
	        
	        Path sourceDb = getPath(DEF_SOURCE_DB);
	        Path targetDb = getPath(DEF_TARGET_DB);
	        
	        System.out.println("Install Nexus database");
	        downloadDatabase(source, sourceDb);
	        
	        System.out.println("Install Input database");
	        downloadDatabase(target, targetDb);
	        
	        System.out.println("Connecting to Nexus database");
	        srcGraphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(sourceDb.toString()) )
				.loadPropertiesFromFile( GetConfPath(sourceDb.toString()).toString() )
				.setConfig( GraphDatabaseSettings.read_only, "false" )
				.newGraphDatabase();
	
	        registerShutdownHook( srcGraphDb );
	        
	        System.out.println("Connecting to Input database");
	        
	        dstGraphDb = new GraphDatabaseFactory()
				.newEmbeddedDatabaseBuilder( GetDbPath(targetDb.toString()) )
				.loadPropertiesFromFile( GetConfPath(targetDb.toString()).toString() )
				.setConfig( GraphDatabaseSettings.read_only, "false" )
				.newGraphDatabase();

	        registerShutdownHook( dstGraphDb );

	        System.out.println("Create global operation's driver");
			
			Set<Label> types = new HashSet<Label>();
			types.add(Types.dataset);
			types.add(Types.grant);
			types.add(Types.researcher);
			types.add(Types.publication);
			
			System.out.println("Create indexes in Nexus database");
	        try ( Transaction tx = srcGraphDb.beginTx() ) {
	        	Schema schema = srcGraphDb.schema();

        		for (Label type : types) {
        			for (String key : keys) 
        				createIndex(schema, type, key);
        		}
	        		
	        	tx.success();
	        }
			
	        System.out.println("Create constraints in Input database");
	        try ( Transaction tx = dstGraphDb.beginTx() ) {
	        	Schema schema = dstGraphDb.schema();
	        	
	        	for (Label type : types) {
	        		createConstraint(schema, type, PROPERTY_KEY);
	        	}
	        	
	        	tx.success();
	        }
	        
	        try ( Transaction ignored = srcGraphDb.beginTx() ) 
			{
	        	Transaction tx = dstGraphDb.beginTx();
	        	try {
	        		
	        		System.out.println("Sync nodes");
		        	for (Node dstNode : dstGraphDb.getAllNodes()) {
		        		
		        		syncNode(dstNode);
		        
		        		if (chunkSize > 1000) {
        					
        					chunkSize = 0;
        					++chunksCounter;

        					System.out.println("Writing " + chunksCounter + " chunk to database");
        				
        					tx.success();
        					tx.close();
        					tx = dstGraphDb.beginTx();			        					
        				}
		        	}
		        	
		        	System.out.println("Found " + mapImported.size() + " unique nodes");
		        	
		        	System.out.println("Sync synblings");
		        	
		        	Map<Long,Long> map = new HashMap<Long,Long>(mapImported);
		        	for (Map.Entry<Long,Long> entry : map.entrySet()) {
		        		Node srcNode = srcGraphDb.getNodeById(entry.getKey());
		        		Node dstNode = dstGraphDb.getNodeById(entry.getValue());
		        				
        				copySyblings(srcNode, dstNode, syncLevel);
        				
        				if (chunkSize > 1000) {
        					
        					chunkSize = 0;
        					++chunksCounter;

        					System.out.println("Writing " + chunksCounter + " chunk to database");
        				
        					tx.success();
        					tx.close();
        					tx = dstGraphDb.beginTx();			        					
        				}
		        	}
		        	
		        	System.out.println("Writing final chunk to database");
		        	
		        	nodeCounter += chunkSize;
		        	tx.success();
	        	} finally {
	        		tx.close();
	        	}
			}
	        
	        System.out.println("Processed " + processedCounter + " nodes. Imported " + nodeCounter + " nodes and " + relCounter + " relationships");
	      		        
	        System.out.println("Shutdown database");
	        
	        srcGraphDb.shutdown();
	        srcGraphDb = null;
	        dstGraphDb.shutdown();
	        dstGraphDb = null;
	        
	        System.out.println("Archive database");
	        
	        Path zipFile = getPath(drop + ".zip");
	        zipFile(zipFile, targetDb, drop);
	        
	        System.out.println("Publish database");
	        
	        if (!StringUtils.isEmpty(bucket))
	        	uploadDatabase(zipFile, bucket);
	              
	        
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
		}
	}	
	
	private static boolean isConstraintExists(Schema schema, Label label, String key) {
		for (ConstraintDefinition constraint : schema.getConstraints(label)) 
			for (String property : constraint.getPropertyKeys())
				if (property.equals(key)) 
					return true;
		
		return false;
	}
	
	private static boolean isIndexExists(Schema schema, Label label, String key) {
		for (IndexDefinition index : schema.getIndexes(label)) 
			for (String property : index.getPropertyKeys()) 
				if (property.equals(key)) 
					return true;
		
		return false;
	}
	
	private static void createConstraint(Schema schema, Label label, String key) {
		//Label label = DynamicLabel.label(type);
		if (!isConstraintExists(schema, label, key)) {
			System.out.println("Creating Constraint on: " + label.toString() + "(" + key + ")");
			
			schema
				.constraintFor(label)
				.assertPropertyIsUnique(key)
				.create();
		}
	}
	
	private static void createIndex(Schema schema, Label label, String key) {	
		if (!isConstraintExists(schema, label, key) && !isIndexExists(schema, label, key)) {
			System.out.println("Creating Index on: " + label.toString() + "(" + key + ")");
    		
			schema
				.indexFor(label)
				.on(key)
				.create();
		}
	}

	
	private static void copySyblings(Node src, Node dst, int synblingLevel) {

	//	System.out.println("Copy syblings with level: " + synblingLevel);
		
		// Iterate throigh all node relationships
		Iterable<Relationship> rels = src.getRelationships();
		for (Relationship rel : rels) {
			// find node sitting on other end of relationship
			Node other = rel.getOtherNode(src);
			Node copy = copyNode(other);
			
			createRelationship(dst, copy, rel.getType()); 
						
			if (synblingLevel > 0)
				copySyblings(other, copy, synblingLevel-1);			
		}
	}
	
	private static boolean isRelated(Node from, Node to) {
		if (from.getId() == to.getId())
			return true;
		
		Iterable<Relationship> rels = from.getRelationships();
		for (Relationship rel : rels) 
			if (rel.getOtherNode(from).getId() == to.getId()) 
				return true;
		
		return false;
	}
	
	private static Node copyNode(Node srcNode) {
		// first check did we already have imported that node
		Long id = mapImported.get(srcNode.getId());
		if (id != null)
			return dstGraphDb.getNodeById(id);
		
		// Acquire source node key and type
		// We are in the RDS ecosystem now, therefore all keys must be strings, 
		// all types must be valid and no additional checks should be required
		String srcKey = (String) srcNode.getProperty(PROPERTY_KEY);
		String srcType = (String) srcNode.getProperty(PROPERTY_TYPE);
		
		// Convert type to a proper node label
		Label type = Label.label(srcType);
		
		// let try find same node in the dst database
		Node dstNode = dstGraphDb.findNode(type, PROPERTY_KEY, srcKey);
		if (dstNode == null) {
		
		//		System.out.println("Creting new node");
				
				// if the node does not exists, create it
				dstNode = dstGraphDb.createNode();
				
				// copy all node properties
				for (String p : srcNode.getPropertyKeys()) 
					dstNode.setProperty(p, srcNode.getProperty(p));
				
				// copy all node labels
				for (Label l : srcNode.getLabels())
					dstNode.addLabel(l);
				
				// increase nodes count
				++nodeCounter;
				
				// increase chunk syze
				++chunkSize;
			//}
		}
		
		// store node id in the map, so we do not need to search it again
		mapImported.put(srcNode.getId(), dstNode.getId());
		
		return dstNode;
	}
	
	private static void createRelationship(Node from, Node to, RelationshipType type) {
		// create relationship to the node if needed
		if (!isRelated(from, to)) {
	//		System.out.println("Creting new relationship");
			
			from.createRelationshipTo(to, type);
			
			// increase relationships count
			++relCounter;
			
			// increase chunk size
			++chunkSize; 
		}
	}
	
	private static void matchNode(Node dstNode, Label labelType, String property, Object value) {
//		System.out.println("Searching for " + property + " = " + value);
		
		// At this point the sync will only match nodes of the same type. 
		// This will require source nodes to have correct type or sync program will not work
		ResourceIterator<Node> nodes = srcGraphDb.findNodes(labelType, property, value);
		if (null != nodes)
			while (nodes.hasNext()) {
				Node srcNode = nodes.next();
				
				mapImported.put(srcNode.getId(), dstNode.getId());

				//System.out.println("Match found with id : " + srcNode.getId());

				// DK Disabled the creation of knownAs relationsip
				// to enable, comment map adding above and uncomment the rest
				
				// get or copy the node to the dst database
				Node cpyNode = copyNode(srcNode);
				
				// create relationships
				createRelationship(dstNode, cpyNode, Relationships.linkedBySynthesis);
								
				//System.out.println("Done");
			}
	}
	
	private static void syncNode(Node dstNode) throws Exception {
		// Node healty check 
		
		// a simple check to see if node has a key, source and type
		if (!dstNode.hasProperty(PROPERTY_KEY) || 
			!dstNode.hasProperty(PROPERTY_SOURCE) ||
			!dstNode.hasProperty(PROPERTY_TYPE))
			return;
		
		// extract node type. The node must have one string type
		Object type = dstNode.getProperty(PROPERTY_TYPE);
		if (type == null || !(type instanceof String))
			return;
		
		// the type must be either datatase, grant, researcher or publication
		Label labelType;
		if (type.equals(Types.dataset.name()))
			labelType = Types.dataset;
		else if (type.equals(Types.grant.name()))
			labelType = Types.grant;
		else if (type.equals(Types.researcher.name()))
			labelType = Types.researcher;
		else if (type.equals(Types.publication.name()))
			labelType = Types.publication;
		else
			return;
		
		++processedCounter;
		
		//System.out.println("Node id: " + dstNode.getId());
		
		// check if node has one of property required for syncing 
		for (String property : keys) {
			if (dstNode.hasProperty(property)) {
				Object values = dstNode.getProperty(property);
				
				// we obly interesting in String or String[] properties at this point
				if (values instanceof String) 
					matchNode(dstNode, labelType, property, (String) values);
				else if (values instanceof String[])
	        		for (String value : (String[]) values)
	        			matchNode(dstNode, labelType, property, value);
			}
		}
	}
	
	private static boolean isZip(String path) {
		return path.trim().toLowerCase().endsWith(DEF_PATH_ZIP);
	}
	
	private static String getFolderName(String file) {
		int idx = file.toLowerCase().lastIndexOf(DEF_PATH_ZIP);
		if (idx < 0)
			throw new IllegalArgumentException("Expected ZIP archive name but got " + file);
		
		return file.substring(0, idx);
	}
	
	private static Path getPath(String path) {
		return Paths.get(work.toString(), path);
	}

	private static Path getTmpPath() {
		return getPath(DEF_PATH_TMP);
	}
	
	private static void downloadFileS3(S3Path path, Path output) throws FileNotFoundException, IOException {
		S3Object object = s3client.getObject(new GetObjectRequest(path.getBucket(), path.getKey()));
		byte[] buffer = new byte[1024];
		int n;
		
		try (InputStream is = object.getObjectContent()) {
			try (OutputStream os = new FileOutputStream(output.toFile())) {
				while((n = is.read(buffer)) > 0) {
				    os.write(buffer, 0, n); 
				}
			}
		}	
	}
	
	private static void zipFile(Path zipFile, Path input, String rootName) throws IOException {
		try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFile.toFile()))) {
			zipEntry(zos, input, input, rootName);
		}
	}
	
	private static void zipEntry(ZipOutputStream zos, Path root, Path source, String rootName) throws IOException {
		Path relative = root.relativize(source);
		Path local = Paths.get(rootName, relative.toString());
		
		//System.out.println("zip: " + local);
		
		if (Files.isDirectory(source)) {
			zos.putNextEntry(new ZipEntry(local.toString() + "/"));
			
			File files[] = source.toFile().listFiles();
    		for (File file : files) 
    			zipEntry(zos, root, file.toPath(), rootName);
	    } else {
			ZipEntry ze = new ZipEntry(local.toString());
			zos.putNextEntry(ze);

			try (InputStream in = new FileInputStream(source.toString())) {
	    		byte[] buffer = new byte[1024];
    			int n;
    	        
				//copy the file content in bytes 
    			while ((n = in.read(buffer)) > 0) {
        			zos.write(buffer, 0, n);
        		}
    		}
	    	
	    	zos.closeEntry();
    	}
	}
	
	private static void unzipFile(Path zipFile, Path output) throws IOException {
		try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile.toFile()))) {
		
			Path base = Paths.get(getFolderName(zipFile.toFile().getName()));
			byte[] buffer = new byte[1024];
			Path file;
			int n;

			ZipEntry ze = zis.getNextEntry();
			while (ze != null) {
	        	file = Paths.get(ze.getName());
				
	        	if (file.toString().startsWith(base.toString()))
					file = base.relativize(file);

		      //  System.out.println("unzip : "+ file.toString());
	        	
				file = Paths.get(output.toString(), file.toString());

		        if (ze.isDirectory()) 
		        	Files.createDirectories(file);
	        	else {
		        	// create all non exists folders
		        	// else you will hit FileNotFoundException for compressed folder
				      
		            try (OutputStream os = new FileOutputStream(file.toFile())) {             
			        	while ((n = zis.read(buffer)) > 0) {
			        		os.write(buffer, 0, n);
			            }
		            }
		        }
		        		
	        	ze = zis.getNextEntry();
	    	}
		    	
		    zis.closeEntry();
		}
	}
	
	public static void copyFolder(Path src, Path dest) throws IOException{
	    if (Files.isDirectory(src)) {
	    	// if directory not exists, create it
	    	if (!Files.exists(dest)) {
	    		System.out.println("Create Directory " + dest);
	    		
	    		Files.createDirectories(dest);	    		
    		}
	    		
    		// list all the directory contents
    		String files[] = src.toFile().list();
    		
    		for (String file : files) {
    			// construct the src and dest file structure
    			Path srcFile = Paths.get(src.toString(), file);
    			Path destFile = Paths.get(dest.toString(), file);
    		  
    			// recursive copy
    			copyFolder(srcFile,destFile);
    		}
	    } else {
	    	System.out.println("Copy File " + dest);
	    	
    		// if file, then copy it
    		// Use bytes stream to support all file types
    		try (InputStream in = new FileInputStream(src.toFile())) {
    			try (OutputStream out = new FileOutputStream(dest.toFile())) { 
    	                     
    				byte[] buffer = new byte[1024];
    				int n;
    	        
    				//copy the file content in bytes 
    				while ((n = in.read(buffer)) > 0){
    					out.write(buffer, 0, n);
    				}
    			}
    		}
    	}
	}

	private static void downloadDatabase(String from, Path to) throws FileNotFoundException, IOException {
	//	System.out.println("Downloading database from " + from + " to " + to);
		S3Path path = S3Path.parse(from);
		if (null != path && path.isValud()) {
	//		System.out.println("The file is hosted on S3 bucket: " + path.getBucket() + ", key: " + path.getKey() + ", file: " + path.getFile());
			if (isZip(path.getFile())) {
				// the from path is a path to S3 file 
				Path tmp = Paths.get(getTmpPath().toString(), path.getFile());
				Files.createDirectories(tmp.getParent());
				
			//	System.out.println("Tmp path: " + tmp);
			
				downloadFileS3(path, tmp);
				unzipFile(tmp, to);
			} else 
				throw new IllegalArgumentException("Only Zip archives are supported for S3");
		} else {
			Path local = Paths.get(from);
			if (null == local || !Files.exists(local))
				throw new IllegalArgumentException("The local path are invalid: " + local.toString());
			if (Files.isDirectory(local))
				copyFolder(local, to);
			else if (isZip(local.toString())) 
				unzipFile(local, to);
			else 
				throw new IllegalArgumentException("The local path are invalid: " + local.toString());
				
		}			
	}
	
	private static void uploadDatabase(Path zipFile, String bucket) throws FileNotFoundException, IOException {
		try (InputStream is = new FileInputStream(zipFile.toFile())) {
        	
        	byte[] bytes = IOUtils.toByteArray(is);
        	
        	ObjectMetadata metadata = new ObjectMetadata();
        	metadata.setContentLength(bytes.length);
        	
        	InputStream inputStream = new ByteArrayInputStream(bytes);
        	
        	PutObjectRequest request = new PutObjectRequest(bucket, zipFile.getFileName().toString(), inputStream, metadata);
        	
	        s3client.putObject(request);
        }  
	}
	
	
}
