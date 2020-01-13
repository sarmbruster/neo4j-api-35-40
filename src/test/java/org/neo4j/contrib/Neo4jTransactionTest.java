package org.neo4j.contrib;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Neo4jTransactionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void cypherParameterTest() {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        db.execute("CREATE (p:Person{name:{name}})", MapUtil.map("name", "John"));

        String name = (String) Iterators.single(db.execute("MATCH (p:Person) return p.name as name")).get("name");
    }

    @Test
    public void nestedTransaction() {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        try (Transaction tx = db.beginTx()) {

            System.out.println(tx.getClass().getName());
            Node node = db.createNode();
            node.setProperty("name", "node1");

            try (Transaction innerTx = db.beginTx()) {
                System.out.println(innerTx.getClass().getName());
                Node innerNode = db.createNode();
                innerNode.setProperty("name", "node2");

                innerTx.success();  // change to .failure()
            }

            tx.success();
        }

        long count = (long) Iterators.single(db.execute("MATCH (n) RETURN count(n) AS count")).get("count");
        assertEquals(2L, count);
    }

    @Test
    public void shareInstancesBetweenTransactions() {

        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        db.execute("CREATE (:Person{name:1})-[:KNOWS]->(:Person{name:2})");

        Node person1;
        try (Transaction tx = db.beginTx()) {
            person1 = db.findNode(Label.label("Person"), "name", 1);

            tx.success();
        }
        assertNotNull(person1);

        try (Transaction tx = db.beginTx()) {
            Relationship rel = person1.getSingleRelationship(RelationshipType.withName("KNOWS"), Direction.OUTGOING);
            Node person2 = rel.getEndNode();

            assertEquals(2L, person2.getProperty("name"));

            tx.success();
        }
    }

    @Test
    public void algoTest() {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        db.execute("CREATE (a:City{name:'a'})-[:ROAD{d:5}]->(:City{name:'b'})-[:ROAD{d:2}]->(c:City{name:'c'}), " +
                "(a)-[:ROAD{d:10}]->(c)");

        try (Transaction tx = db.beginTx()) {

            Node start = db.findNode( Label.label("City"), "name", "a");
            Node end = db.findNode( Label.label("City"), "name", "c");

            Iterable<WeightedPath> paths = GraphAlgoFactory.dijkstra(
                    PathExpanders.allTypesAndDirections(),
                    "d"
            ).findAllPaths(start, end);

            for (WeightedPath path: paths) {
                System.out.println(path);
                assertEquals(7.0, path.weight(), 0.0001);
            }

            tx.success();
        }

    }

    @Test
    public void graphProperties() {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        GraphDatabaseAPI api = (GraphDatabaseAPI) db;
        GraphPropertiesProxy graphProperties = api.getDependencyResolver()
                .resolveDependency(EmbeddedProxySPI.class, DependencyResolver.SelectionStrategy.FIRST)
                .newGraphPropertiesProxy();

        try (Transaction tx = db.beginTx()) {
            graphProperties.setProperty("hello", "world");
            tx.success();
        }

        try (Transaction tx = db.beginTx()) {
            assertEquals("world",  graphProperties.getProperty("hello"));
            tx.success();
        }

    }

}
