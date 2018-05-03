package com.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static com.mongodb.client.model.Filters.eq;


public class Final_Problem_7_Cleanup_Tests {
    static MongoDatabase _db;
    @BeforeClass
    public static void init()
    {
        MongoClient mongoClient = new MongoClient();
        _db = mongoClient.getDatabase("final7");

    }

    @Test
    public void deleteUnusedImagesTest() {
        Set<Integer> imageIds = new HashSet<>();
        MongoCollection<Document> imageCollection = _db.getCollection("images");
        MongoCollection<Document> albumCollection = _db.getCollection("albums");

        FindIterable<Document> images = imageCollection.find();
        images
                .projection(new Document("_id", 1))
                .forEach((Consumer<Document>) d -> {
                    Integer id = d.getInteger("_id");
                    imageIds.add(id);
                });

        int originalSize = imageIds.size();
        System.out.printf("read %d images\n", originalSize);

        FindIterable<Document> albums = albumCollection.find();
        albums.forEach((Consumer<Document>)
                               d-> {
                                   ArrayList<Integer> albumImages = (ArrayList<Integer>)  d.get("images");
                                   albumImages.forEach(imageIds::remove);
                               }
        );
        int numOrphans = imageIds.size();
        System.out.printf("found %d oprhan images and %d non-orphans\n", numOrphans, originalSize-numOrphans);
        imageIds.forEach(i -> imageCollection.deleteOne(eq("_id", i)));
        long goodImages = imageCollection.count();
        System.out.printf("After deletion, there are %d good images\n", goodImages);

    }

    @Test
    public void testCountSunrises() {
        long sunrises = countSunrises();
        System.out.printf("found %d sunrise images\n", sunrises);
    }

    public long countSunrises() {
        MongoCollection<Document> imageCollection = _db.getCollection("images");
        long res = imageCollection.count(eq("tags", "sunrises"));
        return res;
    }
}
