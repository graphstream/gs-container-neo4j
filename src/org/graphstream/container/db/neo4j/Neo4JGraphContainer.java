package org.graphstream.container.db.neo4j;

import java.util.HashMap;
import java.util.logging.Logger;

import org.graphstream.container.db.DBConnectionException;
import org.graphstream.container.db.DBGraphContainer;
import org.graphstream.graph.ElementNotFoundException;
import org.graphstream.graph.GraphSelection;
import org.graphstream.stream.SourceBase;
import org.graphstream.util.VerboseSink;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;

//
// Error "No schema index provider org.neo4j.kernel.api.index.SchemaIndexProvider found."
// --> neo4j-lucene-index.jar is missing
//
public class Neo4JGraphContainer extends SourceBase implements DBGraphContainer {
	private static final Logger LOGGER = Logger.getLogger(Neo4JGraphContainer.class.getName());

	public static final String NODE_TYPE = "Default";
	public static final String EDGE_TYPE = "Default";

	protected static final Label NODE_LABEL=new Label(){public String name(){return NODE_TYPE;}};

	protected static enum EdgeType implements RelationshipType {

	}

	protected GraphDatabaseService db;
	protected boolean autoCreate;
	protected boolean strictChecking;

	public Neo4JGraphContainer() {
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (Neo4JGraphContainer.this.db != null) {
					Neo4JGraphContainer.this.db.shutdown();
					LOGGER.info("neo4j db shutdown");
				}
			}
		});
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.graphstream.container.db.DBGraphContainer#connect(java.lang.String)
	 */
	@Override
	public void connect(String path) throws DBConnectionException {
		if (db != null)
			db.shutdown();

		db = null;

		try {
			db = new GraphDatabaseFactory().newEmbeddedDatabase(path);
		} catch (Exception e) {
			Throwable t = e;

			while (t.getCause() != null) {
				t = t.getCause();

				if (t instanceof StoreLockException)
					throw new DBConnectionException("neo4j db is locked by another connection", t);
			}

			throw new DBConnectionException(e);
		}

		initDb();

		LOGGER.info("neo4j db connected");
	}

	protected void initDb() {
		try (Transaction tx = db.beginTx()) {
			if (!db.schema().getConstraints(NODE_LABEL).iterator().hasNext())
				db.schema().constraintFor(NODE_LABEL).assertPropertyIsUnique("nodeId").create();

			tx.success();
		}
	}

	protected Node getDbNode(String nodeId) {
		ResourceIterator<Node> result = null;
		Node n = null;

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId: {id}}) RETURN n";

			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("id", nodeId);

			result = db.execute(queryString, params).columnAs("n");
			n = result.next();

			result.close();
			tx.success();
		}

		return n;
	}

	protected void createNode(String nodeId) {
		try (Transaction tx = db.beginTx()) {
			String queryString = "MERGE (n:" + NODE_TYPE + " {nodeId: {id}})";

			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("id", nodeId);

			db.execute(queryString, params);

			tx.success();
		}
	}

	protected boolean hasNode(String nodeId) {
		ResourceIterator<Long> result = null;
		boolean h = false;

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId: {id}}) RETURN COUNT(DISTINCT n) AS count";

			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("id", nodeId);

			result = db.execute(queryString, params).columnAs("count");
			h = result.next() > 0;

			result.close();
			tx.success();
		}

		return h;
	}

	protected boolean hasEdge(String edgeId) {
		ResourceIterator<Long> result = null;
		boolean h = false;

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH ()-[r:" + EDGE_TYPE + " {edgeId: {id}}]-() RETURN COUNT(DISTINCT r) AS count";

			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("id", edgeId);

			result = db.execute(queryString, params).columnAs("count");
			h = result.next() > 0;

			result.close();
			tx.success();
		}

		return h;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.graph.GraphContainer#select(java.lang.String)
	 */
	@Override
	public GraphSelection select(String query) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void graphAttributeAdded(String sourceId, long timeId, String attribute, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void graphAttributeChanged(String sourceId, long timeId, String attribute, Object oldValue,
			Object newValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void graphAttributeRemoved(String sourceId, long timeId, String attribute) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nodeAttributeAdded(String sourceId, long timeId, String nodeId, String attribute, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nodeAttributeChanged(String sourceId, long timeId, String nodeId, String attribute, Object oldValue,
			Object newValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void nodeAttributeRemoved(String sourceId, long timeId, String nodeId, String attribute) {
		// TODO Auto-generated method stub

	}

	@Override
	public void edgeAttributeAdded(String sourceId, long timeId, String edgeId, String attribute, Object value) {
		// TODO Auto-generated method stub

	}

	@Override
	public void edgeAttributeChanged(String sourceId, long timeId, String edgeId, String attribute, Object oldValue,
			Object newValue) {
		// TODO Auto-generated method stub

	}

	@Override
	public void edgeAttributeRemoved(String sourceId, long timeId, String edgeId, String attribute) {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#nodeAdded(java.lang.String, long,
	 * java.lang.String)
	 */
	@Override
	public void nodeAdded(String sourceId, long timeId, String nodeId) {
		createNode(nodeId);

		sendNodeAdded(sourceId, timeId, nodeId);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#nodeRemoved(java.lang.String,
	 * long, java.lang.String)
	 */
	@Override
	public void nodeRemoved(String sourceId, long timeId, String nodeId) {
		sendNodeRemoved(sourceId, timeId, nodeId);

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId: {id}}) DELETE n";

			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("id", nodeId);

			db.execute(queryString, params);

			tx.success();
		}
	}

	@Override
	public void edgeAdded(String sourceId, long timeId, String edgeId, String fromNodeId, String toNodeId,
			boolean directed) {
		boolean src = hasNode(fromNodeId), trg = hasNode(toNodeId);

		if (!src || !trg) {
			if (strictChecking)
				throw new ElementNotFoundException(
						String.format("Cannot create edge %s[%s-%s%s]. Node '%s' does not exist.", edgeId, fromNodeId,
								directed ? ">" : "-", toNodeId, !src ? fromNodeId : toNodeId));

			if (!autoCreate) {
				LOGGER.warning("node(s) \"" + (src ? toNodeId : (trg ? fromNodeId : fromNodeId + "\", \"" + toNodeId))
						+ "\" missing, failed to create edge from it.");

				return;
			}

			if (!src)
				createNode(fromNodeId);

			if (!trg)
				createNode(toNodeId);
		}

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH (a:" + NODE_TYPE + " {nodeId:{fromId}}), (b:" + NODE_TYPE + " {nodeId:{toId}}) "
					+ "MERGE (a)-[r:" + EDGE_TYPE + " {edgeId:{id}}]-(b)";

			HashMap<String, Object> params = new HashMap<String, Object>();
			params.put("id", edgeId);
			params.put("fromId", fromNodeId);
			params.put("toId", toNodeId);

			db.execute(queryString, params);

			tx.success();
		}

		sendEdgeAdded(sourceId, timeId, edgeId, fromNodeId, toNodeId, directed);
	}

	@Override
	public void edgeRemoved(String sourceId, long timeId, String edgeId) {
		// TODO Auto-generated method stub

	}

	@Override
	public void graphCleared(String sourceId, long timeId) {
		sendGraphCleared(sourceId, timeId);

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH (n:" + NODE_TYPE + ") DELETE n";

			db.execute(queryString);

			tx.success();
		}

		// TODO delete edges
	}

	@Override
	public void stepBegins(String sourceId, long timeId, double step) {
		// TODO Auto-generated method stub

	}

	public static void main(String... args) throws Exception {
		Neo4JGraphContainer container = new Neo4JGraphContainer();
		VerboseSink v = new VerboseSink();
		container.addSink(v);

		container.connect("test.db");

		container.nodeAdded("me", 0, "A");
		container.nodeAdded("me", 1, "B");

		container.edgeAdded("me", 2, "AB", "A", "B", false);

		System.out.printf("has node A  ? %s%n", container.hasNode("A"));
		System.out.printf("has node B  ? %s%n", container.hasNode("B"));
		System.out.printf("has edge AB ? %s%n", container.hasEdge("AB"));
		System.out.printf("has edge BA ? %s%n", container.hasEdge("BA"));
	}
}
