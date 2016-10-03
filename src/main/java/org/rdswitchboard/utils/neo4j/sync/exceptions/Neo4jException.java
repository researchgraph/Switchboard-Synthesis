package org.rdswitchboard.utils.neo4j.sync.exceptions;

public class Neo4jException extends Exception {
	private static final long serialVersionUID = -2178981642827003503L;

	public Neo4jException(String message) {
        super(message);
    }
}
