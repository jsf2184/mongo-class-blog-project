package com.mongodb;

import com.mongodb.client.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Assert;
import org.junit.Test;
import util.Helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.*;
import static java.util.Arrays.asList;

public class MongoDbBasicTests {
    @Test
    public void testClientDbCollectionDiscovery() {
        MongoClient mongoClient = new MongoClient();
        ListDatabasesIterable<Document> databases = mongoClient.listDatabases();
        Map<String, MongoCollection<Document>> collectionMap = new HashMap<>();
        for (Document d : databases) {
            String dbName = d.getString("name");
            System.out.println(dbName);
            MongoDatabase db = mongoClient.getDatabase(dbName);
            MongoIterable<String> collectionNames = db.listCollectionNames();
            for (String collectionName : collectionNames) {
                String fullCollectionName = dbName + "." + collectionName;
                System.out.printf("    %s\n", fullCollectionName);
                // Note that there are other classes (e.g. BsonDocument) that a collection can be templatized on.
                // You pass that (e.g. BsonDocument.class) as an arg to getCollection() to get that specialization.
                //
                MongoCollection<Document> collection = db.getCollection(collectionName);
                collectionMap.put(fullCollectionName, collection);
            }
        }

    }

    @Test
    public void testDocumentBasics() {
        Document document = new Document();
        document.append("str", "hello").append("num", 123);
        System.out.println(document.toJson());
        String str = document.getString("str");
        Assert.assertEquals("hello", str);
        Object abc = document.get("abc");
        Assert.assertNull(abc);
        document.put("num", 123);
        Assert.assertEquals(123, document.get("num"));


        // Note that document will not do any auto type conversion as shown below.
        boolean caught = false;
        try {
            String numStr = document.getString("num");
        } catch (Exception ignore) {
            caught = true;
        }
        Assert.assertTrue(caught);
    }

    @Test
    public void testInsert() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("course");
        MongoCollection<Document> collection = db.getCollection("insertTest");
        collection.drop();
        Document smith = new Document("name", "Smith")
                .append("age", 30)
                .append("profession", "programmer");
        Helpers.printJson(smith);
        collection.insertOne(smith);
        // note that the insertOne created an _id field.
        Helpers.printJson(smith);

        Document jones = new Document("name", "Jones")
                .append("age", 33)
                .append("profession", "dancer");
        Document mcbride = new Document("name", "McBride")
                .append("age", 40)
                .append("profession", "analyst");

        collection.insertMany(asList(jones, mcbride));
    }

    public MongoCollection<Document> createEmptyCollection() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("course");
        MongoCollection<Document> collection = db.getCollection("insertTest");
        collection.drop();
        return collection;
    }

    public MongoCollection<Document> createIJpairCollection() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<10; i++) {
            for (int j=0; j<10; j++) {
                Document document = new Document("i", i).append("j", j);
                collection.insertOne(document);
            }
        }
        return collection;
    }

    public MongoCollection<Document> popSimpleCollection(int n) {
        MongoCollection<Document> collection = createEmptyCollection();
        collection.drop();
        Random random = new Random();

        IntStream.range(0, n)
                .boxed()
                .map(x -> new Document("x", random.nextInt(2)).append("y", random.nextInt(100)))
                .forEach(collection::insertOne);

        return collection;
    }

    @Test
    public void testFindIterate() {
        MongoCollection<Document> collection = popSimpleCollection(10);
        Assert.assertEquals(10, collection.count());

        FindIterable<Document> iterable = collection.find();
        iterable.forEach((Consumer<Document>) Helpers::printJson);


        for (Document document : iterable) {
            System.out.printf("for loop: %s\n", document.toJson());
        }

        ArrayList<Document> list = iterable.into(new ArrayList<>());
        list.forEach(Helpers::printJson);

        // use try with resources to get auto-closing
        try (MongoCursor<Document> iterator = collection.find().iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void testFindWithDocuments() {
        MongoCollection<Document> collection = popSimpleCollection(10);
        Document document = new Document("x", 1)
                .append("y", new Document("$gt", 10).append("$lt", 90)
                );
        System.out.println("All Documents");
        try (MongoCursor<Document> iterator = collection.find().iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }

        System.out.println("\nFiltered Documents");
        try (MongoCursor<Document> iterator = collection.find(document).iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void testFindWithFilters() {
        MongoCollection<Document> collection = popSimpleCollection(10);

        Bson filter = and(eq("x", 0), gt("y", 10), lt("y", 90));
        System.out.println("All Documents");
        try (MongoCursor<Document> iterator = collection.find().iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }

        System.out.println("\nFiltered Documents");
        try (MongoCursor<Document> iterator = collection.find(and(eq("x", 0), gt("y", 10), lt("y", 90))).iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void testFindWithFiltersAndDocumentProjection() {
        MongoCollection<Document> collection = popSimpleCollection(10);

        Bson filter = and(eq("x", 0), gt("y", 10), lt("y", 90));

        System.out.println("All Documents");
        try (MongoCursor<Document> iterator = collection.find().iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }

        System.out.println("\nFiltered Documents");
        Bson projection = new Document("_id", 0).append("y", 1).append("x", 1);
        try (MongoCursor<Document> iterator = collection
                .find(filter)
                .projection(projection)
                .iterator())
        {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void testFindWithFiltersAndNonDocumentProjection() {
        MongoCollection<Document> collection = popSimpleCollection(10);

        Bson filter = and(eq("x", 0), gt("y", 10), lt("y", 90));

        System.out.println("All Documents");
        try (MongoCursor<Document> iterator = collection.find().iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }

        System.out.println("\nFiltered Documents");
        Bson projection =  Projections.fields(
                Projections.include("y", "x"),
                Projections.excludeId()
        );
        try (MongoCursor<Document> iterator = collection
                .find(filter)
                .projection(projection)
                .iterator())
        {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void testSortWithDocument() {
        MongoCollection<Document> collection = createIJpairCollection();

        System.out.println("All Documents");

        Bson projection =  Projections.fields(
                Projections.include("i", "j"),
                Projections.excludeId()
        );

        // 1 is ascending , -1 is descending
        Bson sort1 = new Document("j", 1).append("i", -1);

        Bson sort2 = Sorts.orderBy(Sorts.ascending("j"), Sorts.descending("i"));

        try (MongoCursor<Document> iterator = collection
                .find()
                .projection(projection)
                .sort(sort2)
                .skip(20)   // skip first 20 results
                .limit(50)   // first 50 results
                .iterator())
        {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void replaceTest() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<8; i++) {
            collection.insertOne(new Document("_id", i)
                                         .append("x", i)
                                         .append("y", true)
            );
        }

        collection.replaceOne(eq("x", 5), new Document("x", 20).append("updated", true));

        try (MongoCursor<Document> iterator = collection
                .find()
                .iterator())
        {

            // when you do this, the _id field stays the same.
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }


    }

    @Test
    public void updateTestWithDocument() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<8; i++) {
            collection.insertOne(new Document("_id", i)
                                         .append("x", i)
                                         .append("y", false)
            );
        }

        Bson u = new Document("$set", new Document("x", 20).append("updated", true));
        collection.updateOne(eq("x", 5), u);

        try (MongoCursor<Document> iterator = collection
                .find()
                .iterator())
        {

            // when you do this, the _id field stays the same.
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }


    }

    @Test
    public void updateTestWithBuilder() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<8; i++) {
            collection.insertOne(new Document("_id", i)
                                         .append("x", i)
                                         .append("y", false)
            );
        }

        Bson u = combine(set("x", 20),
                         set("updated", true));

        //new Document("$set", new Document("x", 20).append("updated", true));
        collection.updateOne(eq("x", 5), u);

        try (MongoCursor<Document> iterator = collection
                .find()
                .iterator())
        {

            // when you do this, the _id field stays the same.
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void upsertTestWithBuilder() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<8; i++) {
            collection.insertOne(new Document("_id", i)
                                         .append("x", i)
                                         .append("y", false)
            );
        }

        Bson u = combine(set("x", 9),
                         set("updated", true));

        //new Document("$set", new Document("x", 20).append("updated", true));
//        collection.updateOne(eq("_id", 9), u, new UpdateOptions().upsert(true));
        collection.updateOne(eq("_id", 7), u, new UpdateOptions().upsert(true));

        try (MongoCursor<Document> iterator = collection
                .find()
                .iterator())
        {

            // when you do this, the _id field stays the same.
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }


    @Test
    public void updateManyTestWithBuilder() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<8; i++) {
            collection.insertOne(new Document("_id", i)
                                         .append("x", i)
                                         .append("y", false)
            );
        }

        Bson u = combine(inc("x", 1),
                         set("updated", true));

        //new Document("$set", new Document("x", 20).append("updated", true));
        collection.updateMany(gte("x", 5), u, new UpdateOptions().upsert(true));

        try (MongoCursor<Document> iterator = collection
                .find()
                .iterator())
        {

            // when you do this, the _id field stays the same.
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

    @Test
    public void deleteOneTestWithBuilder() {
        MongoCollection<Document> collection = createEmptyCollection();
        for (int i=0; i<8; i++) {
            collection.insertOne(new Document("_id", i)
                                         .append("x", i)
                                         .append("y", false)
            );
        }

        Bson u = combine(inc("x", 1),
                         set("updated", true));

        //new Document("$set", new Document("x", 20).append("updated", true));
        collection.deleteMany(gte("x", 5));

        try (MongoCursor<Document> iterator = collection
                .find()
                .iterator())
        {

            // when you do this, the _id field stays the same.
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }

}


