package org.rdswitchboard.utils.neo4j.sync.test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;


public class SynthesisTest {

    @Test
    public void testSynthesis() {

        Path source = Paths.get("/Users/Admin/git/researchgraph/Synthesis/src/test/A/R.Neo4j");
        Path target = Paths.get("/Users/Admin/git/researchgraph/Synthesis/src/test/B/R.Neo4j");
        Set<String> keys = new HashSet<>();
        keys.add("doi");
       // keys.add("orcid");
        int syncLevel =1;

        try {
            org.rdswitchboard.utils.neo4j.sync.Process.synthesis(source, target, keys, syncLevel);

        } catch (Exception e){
             e.printStackTrace();
        }

    }

}
