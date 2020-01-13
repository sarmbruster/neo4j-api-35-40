package org.neo4j.contrib;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.rule.Neo4jRule;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.internal.helpers.collection.MapUtil;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Neo4jTransactionTest {

    @Rule
    public Neo4jRule neo4j = new Neo4jRule();

    @Test
    public void cypherParameterTest() {
        GraphDatabaseService db = neo4j.defaultDatabaseService();
        db.executeTransactionally("CREATE (p:Person{name:$name})", MapUtil.map("name", "John"));

        String name = singleResultFirstColumn(db, "MATCH (p:Person) return p.name as name");
        assertEquals("John", name);
    }

    private <T> T singleResultFirstColumn(GraphDatabaseService db, String cypher) {
        return db.executeTransactionally(cypher, Collections.emptyMap(), result -> {
            String firstColumnName = Iterables.first(result.columns());
            return Iterators.single(result.columnAs(firstColumnName));
        });
    }

    @Test
    public void nestedTransaction() {
        GraphDatabaseService db = neo4j.defaultDatabaseService();
        try (Transaction tx = db.beginTx()) {

            System.out.println(tx.getClass().getName());
            Node node = tx.createNode();
            node.setProperty("name", "node1");

            try (Transaction innerTx = db.beginTx()) {
                System.out.println(innerTx.getClass().getName());
                Node innerNode = tx.createNode();
                innerNode.setProperty("name", "node2");

                innerTx.commit();  // change to .failure()
            }

            tx.commit();
        }

        long count = singleResultFirstColumn(db, "MATCH (n) RETURN count(n) AS count");
        assertEquals(2L, count);
    }

    @Test
    public void shareInstancesBetweenTransactions() {

        GraphDatabaseService db = neo4j.defaultDatabaseService();
        db.executeTransactionally("CREATE (:Person{name:1})-[:KNOWS]->(:Person{name:2})");

        Node person1;
        try (Transaction tx = db.beginTx()) {
            person1 = tx.findNode(Label.label("Person"), "name", 1);

            tx.commit();
        }
        assertNotNull(person1);

        try (Transaction tx = db.beginTx()) {
            person1 = tx.getNodeById(person1.getId());
            Relationship rel = person1.getSingleRelationship(RelationshipType.withName("KNOWS"), Direction.OUTGOING);
            Node person2 = rel.getEndNode();

            assertEquals(2L, person2.getProperty("name"));

            tx.commit();
        }
    }

    @Test
    public void algoTest() {
        GraphDatabaseService db = neo4j.defaultDatabaseService();
        db.executeTransactionally("CREATE (a:City{name:'a'})-[:ROAD{d:5}]->(:City{name:'b'})-[:ROAD{d:2}]->(c:City{name:'c'}), " +
                "(a)-[:ROAD{d:10}]->(c)");

        try (Transaction tx = db.beginTx()) {

            Node start = tx.findNode( Label.label("City"), "name", "a");
            Node end = tx.findNode( Label.label("City"), "name", "c");

            Iterable<WeightedPath> paths = GraphAlgoFactory.dijkstra(
                    new BasicEvaluationContext(tx, db),
                    PathExpanders.allTypesAndDirections(),
                    "d"
            ).findAllPaths(start, end);

            for (WeightedPath path: paths) {
                System.out.println(path);
                assertEquals(7.0, path.weight(), 0.0001);
            }

            tx.commit();
        }

    }
}
