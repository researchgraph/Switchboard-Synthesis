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


	private static Path work;	
	private static Set<String> keys; 
	private static AmazonS3 s3client;

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
	        String drop = "neo4j-augmented-" + dateFormat.format(new Date());

            int syncLevel = Integer.parseInt(properties.getProperty(Configuration.PROPERTY_SYNC_LEVEL, DEF_SYNC_LEVEL));

            String keysList = properties.getProperty(Configuration.PROPERTY_SYNC_KEYS, DEF_KEYS_LIST);
            System.out.println("KeyList:" + keysList );

            if (keysList==null || keysList.isEmpty())
                throw new IllegalArgumentException("sync.keys can not be empty");

	        keys = new HashSet<String>();

	        File keysFile = new File(keysList);
			if (keysFile.isFile()) {
				List<String> list = FileUtils.readLines(keysFile);
				for (String l : list) {
					String s = l.trim();
					if (!s.isEmpty())// && !s.equals(GraphUtils.PROPERTY_KEY))
						keys.add(s); 
				}
			}

            System.out.println("List of keys");
			if (keys.isEmpty()) throw new IllegalArgumentException("There is no keys in " + keysList );
			for (String k:keys)
            {
                System.out.println("Key: " + k );
            }

            Path home = Paths.get(syncHome);
            Files.createDirectories(home);
            work = Files.createTempDirectory(home, DEF_SYNC_PREFIX);

            Path sourceDb;
            Path targetDb;

            if (!StringUtils.isEmpty(bucket)) {
                System.out.println("Output Neo4j: s3://" + bucket + "/" + drop + ".zip");

                s3client = new AmazonS3Client(new InstanceProfileCredentialsProvider());

                sourceDb = getPath(DEF_SOURCE_DB);
                targetDb = getPath(DEF_TARGET_DB);

                System.out.println("Install Nexus database");
                downloadDatabase(source, sourceDb);

                System.out.println("Install Input database");
                downloadDatabase(target, targetDb);

            }else{

                sourceDb = Paths.get(source);
                targetDb = Paths.get(target);

            }

            Process.synthesis(sourceDb, targetDb, keys, syncLevel);

            if (!StringUtils.isEmpty(bucket)) {
                System.out.println("Archive database");

                Path zipFile = getPath(drop + ".zip");
                zipFile(zipFile, targetDb, drop);

                System.out.println("Publish database");

                if (!StringUtils.isEmpty(bucket))
                    uploadDatabase(zipFile, bucket);
            }
	        
		} catch (Exception e) {
			e.printStackTrace();
			
			System.exit(1);
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
		System.out.println("Downloading database from " + from + " to " + to);
		S3Path path = S3Path.parse(from);
		if (null != path && path.isValud()) {
			System.out.println("The file is hosted on S3 bucket: " + path.getBucket() + ", key: " + path.getKey() + ", file: " + path.getFile());
			if (isZip(path.getFile())) {
				// the from path is a path to S3 file 
				Path tmp = Paths.get(getTmpPath().toString(), path.getFile());
				Files.createDirectories(tmp.getParent());
				
				System.out.println("Tmp path: " + tmp);
			
				downloadFileS3(path, tmp);
				unzipFile(tmp, to);
			} else 
				throw new IllegalArgumentException("Only Zip archives are supported for S3");
		} else {
			Path local = Paths.get(from);
            System.out.println("Local: " + local.toString());
			if (null == local || !Files.exists(local))
				throw new IllegalArgumentException("The local path is null or no file exists: " + local.toString());
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
