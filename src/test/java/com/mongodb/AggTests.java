package com.mongodb;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class AggTests {

    @Test
    public void testPrint10Zips() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("zips");
        System.out.println("All Documents");
        try (MongoCursor<Document> iterator = collection.find().limit(10).iterator()) {
            iterator.forEachRemaining(d -> System.out.printf("forEachRemaining: %s\n", d.toJson()));
        }
    }


    @Test
    public void testSimpleAggUsingDocuments() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("zips");

        Document stage1 = new Document("$group",
                                       new Document("_id", "$state")
                                               .append("totalPop",
                                                       new Document("$sum", "$pop")));
        Document stage2 = new Document("$match",
                                       new Document("totalPop", new Document("$gte", 10000000)));

        List<Document> pipeline = Arrays.asList(stage1, stage2);
        System.out.println("All Documents");
        AggregateIterable<Document> aggregate = collection.aggregate(pipeline);
        aggregate.forEach((Block<? super Document>) d -> System.out.println(d.toJson()));
    }

    @Test
    public void testSimpleAggUsingParsedDocuments() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("zips");

        Document stage1 = Document.parse("{$group:  { _id: \"$state\",  totalPop: {$sum:\"$pop\"} } }");
        Document stage2 = Document.parse("{$match: {totalPop:  {$gte: 10000000}}}");

        List<Document> pipeline = Arrays.asList(stage1, stage2);
        System.out.println("All Documents");
        AggregateIterable<Document> aggregate = collection.aggregate(pipeline);
        aggregate.forEach((Block<? super Document>) d -> System.out.println(d.toJson()));
    }


    @Test
    public void testSimpleAggUsingBuilders() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = db.getCollection("zips");

        List<Bson> pipeLine = Arrays.asList(
                Aggregates.group("$state", Accumulators.sum("totalPop", "$pop")),
                Aggregates.match(Filters.gte("totalPop", 10000000))
        );

       System.out.println("All Documents");
        AggregateIterable<Document> aggregate = collection.aggregate(pipeLine);
        aggregate.forEach((Block<? super Document>) d -> System.out.println(d.toJson()));
    }


}
