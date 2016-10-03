package org.rdswitchboard.utils.neo4j.sync.enums;

import org.neo4j.graphdb.Label;

public enum Types implements Label {
	dataset, grant, researcher, institution, service, publication, pattern, version
}
