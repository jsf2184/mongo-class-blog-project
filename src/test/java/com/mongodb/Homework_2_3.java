package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.Test;

import static com.mongodb.client.model.Filters.eq;

public class Homework_2_3 {

    @Test
    public void doIt() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("students");
        MongoCollection<Document> collection = db.getCollection("grades");

        Bson sort2 = Sorts.orderBy(Sorts.ascending("student_id"), Sorts.ascending("score"));

        try (MongoCursor<Document> iterator = collection
                .find(eq("type", "homework"))
                .sort(sort2)
                .iterator())
        {
            int count = 0;
            Integer prior = null;
            while (iterator.hasNext()) {
                count++;
                Document document = iterator.next();
                Integer studentId = document.getInteger("student_id");
                boolean deleteIt = false;
                if (!studentId.equals(prior)) {
                    deleteIt = true;
                }
                System.out.printf("%03d: deleteIt: %5s, %s\n", count, deleteIt, document.toJson());
                if (deleteIt) {
                    ObjectId id = document.getObjectId("_id");
                    collection.deleteOne(eq("_id", id));
                }
                prior = studentId;

            }
        }



    }
}
