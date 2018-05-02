package org.rdswitchboard.utils.neo4j.sync;

import org.joda.time.DateTime;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;
import org.rdswitchboard.utils.neo4j.sync.enums.Types;
import org.rdswitchboard.utils.neo4j.sync.exceptions.Neo4jException;

import org.rdswitchboard.utils.neo4j.sync.enums.Relationships;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class Process {

    private static final String PROPERTY_KEY = "key";
    //private static final String PROPERTY_NODE_SOURCE = "node_source";
    private static final String PROPERTY_NODE_TYPE = "type";
    private static final String PROPERTY_SOURCE = "source";
    private static final String PROPERTY_TYPE = "type";

    //private static int syncLevel;
    private static long processedCounter = 0;
    private static long nodeCounter = 0;
    private static long relCounter = 0;
    private static long chunksCounter = 0;
    private static long chunkSize = 0;


    //private static final String DEF_NEO4J_DB = "neo4j";
    //private static final String DEF_NEO4J_ZIP = "neo4j.zip";
    public static final String NEO4J_CONF = "/conf/neo4j.conf";
    public static final String NEO4J_DB = "/data/databases/graph.db";

    private static Map<Long, Long> mapImported;

    private static GraphDatabaseService srcGraphDb;
    private static GraphDatabaseService dstGraphDb;

    private static void printStatistics(GraphDatabaseService graphDB) throws Exception{
        Result result;

        System.out.println("Number of nodes/type:");
        result =graphDB.execute("Match (n) return n.type as TYPE, count(n) as COUNT");
        System.out.println(result.resultAsString());

        System.out.println("Number of nodes with DOI/type:");
        result =graphDB.execute("Match (n) where exists(n.doi) return n.type as TYPE, count(n) as COUNT");
        System.out.println(result.resultAsString());

        System.out.println("Number of unique DOI/type:");
        result =graphDB.execute("Match (n) return n.type as TYPE, count(distinct(n.doi)) as COUNT");
        System.out.println(result.resultAsString());

        System.out.println("Sample of DOI:");
        result =graphDB.execute("Match (n) where exists(n.doi) return n.doi limit 10");
        System.out.println(result.resultAsString());

        System.out.println("Number of nodes with ORCID/type:");
        result =graphDB.execute("Match (n) where exists(n.orcid) return n.type as TYPE, count(n) as ORCID");
        System.out.println(result.resultAsString());

        System.out.println("Sample of unique ORCID/type:");
        result =graphDB.execute("Match (n) return n.type as TYPE, count(distinct(n.orcid)) as COUNT");
        System.out.println(result.resultAsString());

        System.out.println("Sample of ORCID:");
        result =graphDB.execute("Match (n) where exists(n.orcid) return n.orcid limit 10");
        System.out.println(result.resultAsString());


    }
    public static void synthesis(Path sourceDb, Path targetDb, Set<String> keys, int syncLevel) throws Exception {

        mapImported = new HashMap<Long, Long>();

        System.out.println("Connecting to Nexus database");
        srcGraphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( GetDbPath(sourceDb.toString()) )
                .loadPropertiesFromFile( GetConfPath(sourceDb.toString()).toString() )
                .setConfig( GraphDatabaseSettings.read_only, "false" )
                .newGraphDatabase();

        registerShutdownHook( srcGraphDb );

        printStatistics(srcGraphDb);

        System.out.println("Connecting to Input database");

        dstGraphDb = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( GetDbPath(targetDb.toString()) )
                .loadPropertiesFromFile( GetConfPath(targetDb.toString()).toString() )
                .setConfig( GraphDatabaseSettings.read_only, "false" )
                .newGraphDatabase();

        registerShutdownHook( dstGraphDb );

        printStatistics(dstGraphDb);

        System.out.println("Create global operation's driver");

        Set<Label> types = new HashSet<Label>();
        types.add(Types.dataset);
        types.add(Types.grant);
        types.add(Types.researcher);
        types.add(Types.publication);

        System.out.println("Create indexes in source (Nexus) database");
        try ( Transaction tx = srcGraphDb.beginTx() ) {
            Schema schema = srcGraphDb.schema();

            for (Label type : types) {
                for (String key : keys)
                    createIndex(schema, type, key);
            }

            tx.success();
        }

        System.out.println("Create constraints in target (Client) database");
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

                    syncNode(dstNode, keys);

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
    }

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
        System.out.println("Conf: " + conf.toString());
        if (!conf.exists() || conf.isDirectory())
            throw new Neo4jException("The " + folder + " folder is not valid Neo4j instance. Please provide path to an existing Neo4j instance");

        return conf;
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
        String srcType = (String) srcNode.getProperty(PROPERTY_NODE_TYPE);

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

            // add a new label for augmented date and time
                dstNode.setProperty("augmented_at", DateTime.now().toString());

            // copy all node labels
            for (Label l : srcNode.getLabels())
                dstNode.addLabel(l);

            //add researchgraph label to show the node is added to the neo4j by Research Graph Augment Services
                dstNode.addLabel(new Label() {
                    @Override
                    public String name() {
                        return "researchgraph.org";
                    }
                });


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
            //		System.out.println("Creating new relationship");

            from.createRelationshipTo(to, type);

            // increase relationships count
            ++relCounter;

            // increase chunk size
            ++chunkSize;
        }
    }

    private static void syncNode(Node dstNode, Set<String> keys) throws Exception {
        // Node healty check

        // a simple check to see if node has a key, source and type
        if (!dstNode.hasProperty(PROPERTY_KEY) ||
                !dstNode.hasProperty(PROPERTY_SOURCE) ||
                !dstNode.hasProperty(PROPERTY_TYPE)){
            System.out.println("Warning: node ID(" + dstNode.getId() + ") is missing key, source or type!" );
            return;}


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

        // check if node has one of property required for syncing
        for (String property : keys) {
            if (dstNode.hasProperty(property)) {
                Object values = dstNode.getProperty(property);

                // we only interesting in String or String[] properties at this point
                if (values instanceof String)
                    matchNode(dstNode, labelType, property, (String) values);
                else if (values instanceof String[])
                    for (String value : (String[]) values)
                        matchNode(dstNode, labelType, property, value);
            }
        }
    }

    private static void matchNode(Node dstNode, Label labelType, String property, Object value) {
        System.out.println("Searching for label:" + labelType + " | " + property + " = " + value);

        // At this point the sync will only match nodes of the same type.
        // This will require source nodes to have correct type or sync program will not work
        ResourceIterator<Node> nodes = srcGraphDb.findNodes(labelType, property, value);

        if (null != nodes)
            while (nodes.hasNext()) {
                Node srcNode = nodes.next();

                System.out.println("Match found with id : " + srcNode.getId());

                // DK Disabled the creation of knownAs relationsip
                // to enable, comment map adding above and uncomment the rest

                // get or copy the node to the dst database
                Node cpyNode = copyNode(srcNode);

                // create relationships
                createRelationship(dstNode, cpyNode, Relationships.augment);

                //System.out.println("Done");
            }
    }

}
