package org.graphstream.container.db.neo4j;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Logger;

import org.graphstream.algorithm.generator.BarabasiAlbertGenerator;
import org.graphstream.container.GraphSelection;
import org.graphstream.container.db.DBConnectionException;
import org.graphstream.container.db.DBGraphContainer;
import org.graphstream.graph.ElementNotFoundException;
import org.graphstream.stream.SourceBase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;

//
// Error "No schema index provider org.neo4j.kernel.api.index.SchemaIndexProvider found."
// --> neo4j-lucene-index.jar is missing
//
public class Neo4JGraphContainer extends SourceBase implements DBGraphContainer {
	private static final Logger LOGGER = Logger.getLogger(Neo4JGraphContainer.class.getName());

	public static final int MAX_QUEUE_SIZE = 1000;

	public static final String NODE_TYPE = "Default";
	public static final String EDGE_TYPE = "Default";

	protected static final Label NODE_LABEL = new Label() {
		public String name() {
			return NODE_TYPE;
		}
	};

	protected static enum EdgeType implements RelationshipType {

	}

	protected GraphDatabaseService db;
	protected boolean autoCreate;
	protected boolean strictChecking;
	protected Node graphProperties;

	protected LinkedList<GraphEvent> events;

	public Neo4JGraphContainer() {
		events = new LinkedList<GraphEvent>();

		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (Neo4JGraphContainer.this.db != null) {
					Neo4JGraphContainer.this.processEvents();
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
		ResourceIterator<Node> result = null;

		try (Transaction tx = db.beginTx()) {
			if (!db.schema().getConstraints(NODE_LABEL).iterator().hasNext())
				db.schema().constraintFor(NODE_LABEL).assertPropertyIsUnique("nodeId").create();

			String queryString = "MERGE (g:Graph) ON CREATE SET g.step = 0 RETURN g";

			result = db.execute(queryString).columnAs("g");
			graphProperties = result.next();

			result.close();
			tx.success();
		}
	}

	protected Node getDBNode(String nodeId) {
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

	protected Relationship getDBEdge(String edgeId) {
		return null;
	}

	protected void createNode(String nodeId) {
		String queryString = "MERGE (n:" + NODE_TYPE + " {nodeId: {id}})";

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", nodeId);

		db.execute(queryString, params);
	}

	protected void removeNode(String nodeId) {
		String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId: {id}}) OPTIONAL MATCH (n)-[r]-() DELETE n,r";

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", nodeId);

		db.execute(queryString, params);
	}

	protected void createEdge(String edgeId, String fromNodeId, String toNodeId, boolean directed) {
		boolean src = hasNodeNoTx(fromNodeId), trg = hasNodeNoTx(toNodeId);

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

		String queryString = "MATCH (a:" + NODE_TYPE + " {nodeId:{fromId}}), (b:" + NODE_TYPE + " {nodeId:{toId}}) "
				+ "MERGE (a)-[r:" + EDGE_TYPE + " {edgeId:{id}}]-(b)";

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", edgeId);
		params.put("fromId", fromNodeId);
		params.put("toId", toNodeId);

		db.execute(queryString, params);
	}

	protected void removeEdge(String edgeId) {
		String queryString = "MATCH ()-[r:" + EDGE_TYPE + " {edgeId: {id}}]-() DELETE r";

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", edgeId);

		db.execute(queryString, params);
	}

	protected boolean hasNode(String nodeId) {
		boolean h = false;

		try (Transaction tx = db.beginTx()) {
			h = hasNodeNoTx(nodeId);
			tx.success();
		}

		return h;
	}

	protected boolean hasNodeNoTx(String nodeId) {
		ResourceIterator<Long> result = null;
		boolean h = false;

		String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId: {id}}) RETURN COUNT(DISTINCT n) AS count";

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", nodeId);

		result = db.execute(queryString, params).columnAs("count");
		h = result.next() > 0;

		result.close();

		return h;
	}

	protected boolean hasEdge(String edgeId) {
		boolean h = false;

		try (Transaction tx = db.beginTx()) {
			h = hasEdgeNoTx(edgeId);
			tx.success();
		}

		return h;
	}

	protected boolean hasEdgeNoTx(String edgeId) {
		ResourceIterator<Long> result = null;
		boolean h = false;

		String queryString = "MATCH ()-[r:" + EDGE_TYPE + " {edgeId: {id}}]-() RETURN COUNT(DISTINCT r) AS count";

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", edgeId);

		result = db.execute(queryString, params).columnAs("count");
		h = result.next() > 0;

		result.close();

		return h;
	}

	public long getNodeCount() {
		ResourceIterator<Long> result = null;
		long c = 0;

		processEvents();

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH (n:" + NODE_TYPE + ") RETURN COUNT(DISTINCT n) AS count";

			result = db.execute(queryString).columnAs("count");
			c = result.next();

			result.close();
			tx.success();
		}

		return c;
	}

	public long getEdgeCount() {
		ResourceIterator<Long> result = null;
		long c = 0;

		processEvents();

		try (Transaction tx = db.beginTx()) {
			String queryString = "MATCH ()-[r:" + EDGE_TYPE + "]-() RETURN COUNT(DISTINCT r) AS count";

			result = db.execute(queryString).columnAs("count");
			c = result.next();

			result.close();
			tx.success();
		}

		return c;
	}

	protected void clearGraph() {
		String queryString = "MATCH ()-[r:" + EDGE_TYPE + "]-(), (n:" + NODE_TYPE + ") DELETE r, n";
		db.execute(queryString);
	}

	protected void setNodeAttribute(String nodeId, String attributeId, Object value) {
		String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId:{id}}) SET n += {update}";

		HashMap<String, Object> update = new HashMap<String, Object>();
		update.put(attributeId, value);

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", nodeId);
		params.put("update", update);

		db.execute(queryString, params);
	}

	protected void removeNodeAttribute(String nodeId, String attributeId) {
		String queryString = "MATCH (n:" + NODE_TYPE + " {nodeId:{id}}) REMOVE n." + attributeId;

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", nodeId);

		db.execute(queryString, params);
	}

	protected void setEdgeAttribute(String edgeId, String attributeId, Object value) {
		String queryString = "MATCH ()-[r:" + EDGE_TYPE + " {edgeId:{id}}]-() SET r += {update}";

		HashMap<String, Object> update = new HashMap<String, Object>();
		update.put(attributeId, value);

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", edgeId);
		params.put("update", update);

		db.execute(queryString, params);
	}

	protected void removeEdgeAttribute(String edgeId, String attributeId) {
		String queryString = "MATCH ()-[r:" + EDGE_TYPE + " {edgeId:{id}}]-() REMOVE r." + attributeId;

		HashMap<String, Object> params = new HashMap<String, Object>();
		params.put("id", edgeId);

		db.execute(queryString, params);
	}

	protected void enqueueEvent(GraphEvent e) {
		events.add(e);

		if (events.size() > MAX_QUEUE_SIZE)
			processEvents();
	}

	protected void processEvents() {
		try (Transaction tx = db.beginTx()) {
			while (!events.isEmpty()) {
				GraphEvent e = events.poll();

				switch (e.type) {
				case AN:
					createNode(e.elementId());
					break;
				case DN:
					removeNode(e.elementId());
					break;
				case CN:
					if (e.attributeValue() == null)
						removeNodeAttribute(e.elementId(), e.attributeId());
					else
						setNodeAttribute(e.elementId(), e.attributeId(), e.attributeValue());

					break;
				case AE:
					createEdge(e.elementId(), e.fromNodeId(), e.toNodeId(), e.directed());
					break;
				case DE:
					removeEdge(e.elementId());
					break;
				case CE:
					if (e.attributeValue() == null)
						removeEdgeAttribute(e.elementId(), e.attributeId());
					else
						setEdgeAttribute(e.elementId(), e.attributeId(), e.attributeValue());

					break;
				case CG:
					if (e.attributeValue() == null)
						graphProperties.removeProperty(e.attributeId());
					else
						graphProperties.setProperty(e.attributeId(), e.attributeValue());

					break;
				case CR:
					clearGraph();
					break;
				case ST:
					graphProperties.setProperty("step", e.step());
					break;
				}
			}

			tx.success();
		}
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
		enqueueEvent(new GraphEvent(GraphEventType.CG, attribute, value));
		sendGraphAttributeAdded(sourceId, timeId, attribute, value);
	}

	@Override
	public void graphAttributeChanged(String sourceId, long timeId, String attribute, Object oldValue,
			Object newValue) {
		enqueueEvent(new GraphEvent(GraphEventType.CG, attribute, newValue));
		sendGraphAttributeChanged(sourceId, timeId, attribute, oldValue, newValue);
	}

	@Override
	public void graphAttributeRemoved(String sourceId, long timeId, String attribute) {
		enqueueEvent(new GraphEvent(GraphEventType.CG, attribute, null));
		sendGraphAttributeRemoved(sourceId, timeId, attribute);
	}

	@Override
	public void nodeAttributeAdded(String sourceId, long timeId, String nodeId, String attribute, Object value) {
		enqueueEvent(new GraphEvent(GraphEventType.CN, nodeId, attribute, value));
		sendNodeAttributeAdded(sourceId, timeId, nodeId, attribute, value);
	}

	@Override
	public void nodeAttributeChanged(String sourceId, long timeId, String nodeId, String attribute, Object oldValue,
			Object newValue) {
		enqueueEvent(new GraphEvent(GraphEventType.CN, nodeId, attribute, newValue));
		sendNodeAttributeChanged(sourceId, timeId, nodeId, attribute, oldValue, newValue);
	}

	@Override
	public void nodeAttributeRemoved(String sourceId, long timeId, String nodeId, String attribute) {
		sendNodeAttributeRemoved(sourceId, timeId, nodeId, attribute);
		enqueueEvent(new GraphEvent(GraphEventType.CN, nodeId, attribute, null));
	}

	@Override
	public void edgeAttributeAdded(String sourceId, long timeId, String edgeId, String attribute, Object value) {
		enqueueEvent(new GraphEvent(GraphEventType.CE, edgeId, attribute, value));
		sendEdgeAttributeAdded(sourceId, timeId, edgeId, attribute, value);
	}

	@Override
	public void edgeAttributeChanged(String sourceId, long timeId, String edgeId, String attribute, Object oldValue,
			Object newValue) {
		enqueueEvent(new GraphEvent(GraphEventType.CE, edgeId, attribute, newValue));
		sendEdgeAttributeChanged(sourceId, timeId, edgeId, attribute, oldValue, newValue);
	}

	@Override
	public void edgeAttributeRemoved(String sourceId, long timeId, String edgeId, String attribute) {
		sendEdgeAttributeRemoved(sourceId, timeId, edgeId, attribute);
		enqueueEvent(new GraphEvent(GraphEventType.CE, edgeId, attribute, null));
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.graphstream.stream.ElementSink#nodeAdded(java.lang.String, long,
	 * java.lang.String)
	 */
	@Override
	public void nodeAdded(String sourceId, long timeId, String nodeId) {
		enqueueEvent(new GraphEvent(GraphEventType.AN, nodeId));
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
		enqueueEvent(new GraphEvent(GraphEventType.DN, nodeId));
	}

	@Override
	public void edgeAdded(String sourceId, long timeId, String edgeId, String fromNodeId, String toNodeId,
			boolean directed) {
		enqueueEvent(new GraphEvent(GraphEventType.AE, edgeId, fromNodeId, toNodeId, directed));
		sendEdgeAdded(sourceId, timeId, edgeId, fromNodeId, toNodeId, directed);
	}

	@Override
	public void edgeRemoved(String sourceId, long timeId, String edgeId) {
		sendEdgeRemoved(sourceId, timeId, edgeId);
		enqueueEvent(new GraphEvent(GraphEventType.DE, edgeId));
	}

	@Override
	public void graphCleared(String sourceId, long timeId) {
		sendGraphCleared(sourceId, timeId);
		enqueueEvent(new GraphEvent(GraphEventType.CR));
	}

	@Override
	public void stepBegins(String sourceId, long timeId, double step) {
		enqueueEvent(new GraphEvent(GraphEventType.ST, step));
		sendStepBegins(sourceId, timeId, step);
	}

	static enum GraphEventType {
		AN, DN, CN, AE, DE, CE, CG, CR, ST
	}

	static class GraphEvent {
		final GraphEventType type;
		final Object[] args;

		GraphEvent(GraphEventType type, String elementId) {
			this(type, elementId, null, null);
		}

		GraphEvent(GraphEventType type, String attributeId, Object value) {
			this(type, null, attributeId, value);
		}

		GraphEvent(GraphEventType type, double step) {
			this(type, null, null, step);
		}

		GraphEvent(GraphEventType type, Object... args) {
			this.type = type;
			this.args = args;
		}

		String elementId() {
			return (String) args[0];
		}

		String attributeId() {
			return (String) args[type == GraphEventType.CG ? 0 : 1];
		}

		Object attributeValue() {
			return args[type == GraphEventType.CG ? 1 : 2];
		}

		String fromNodeId() {
			return (String) args[1];
		}

		String toNodeId() {
			return (String) args[2];
		}

		boolean directed() {
			return (Boolean) args[3];
		}

		double step() {
			return (Double) args[0];
		}
	}

	public static void main(String... args) throws Exception {
		Neo4JGraphContainer container = new Neo4JGraphContainer();
		long timeId = 0;

		container.connect("test.db");
		container.graphCleared("me", timeId++);

		BarabasiAlbertGenerator gen = new BarabasiAlbertGenerator();
		gen.addSink(container);

		gen.begin();
		for (int i = 0; i < 1000; i++) {
			if (i % 100 == 0)
				System.out.printf("%5d / 1000\n", i);
			gen.nextEvents();
		}

		gen.end();
	}
}
