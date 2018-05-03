package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.set;

public class Homework_3_1 {

    @SuppressWarnings("unchecked")
    @Test
    public void doIt() {
        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase("school");
        MongoCollection<Document> collection = db.getCollection("students");

        try (MongoCursor<Document> iterator = collection
                .find(eq("scores.type", "homework"))
                .iterator()) {
            while (iterator.hasNext()) {
                Document document = iterator.next();
                ArrayList<Document> scores = (ArrayList<Document>) document.get("scores");
                System.out.println(document.toJson());
                Integer lowestIdx = null;
                Double lowestScore = null;
                int numScores = scores.size();
                for(int i = 0; i< numScores; i++) {
                    Document scoreDocument = scores.get(i);
                    String type = scoreDocument.getString("type");
                    if (type.equals("homework")) {
                        Double score = scoreDocument.getDouble("score");
                        if (lowestScore == null || score < lowestScore) {
                            lowestScore = score;
                            lowestIdx = i;
                        }
                    }
                }

                if (lowestIdx != null) {
                    scores.remove(lowestIdx.intValue());
                    Integer id = document.getInteger("_id");
                    System.out.printf("For student: %s, lowest homework scored was %f at idx: %d\n",
                                      id, lowestScore, lowestIdx);

                    ArrayList<Document> updatedScores = (ArrayList<Document>) document.get("scores");
                    Assert.assertEquals(numScores-1, updatedScores.size());

                    collection.updateOne(eq("_id", id), set("scores", scores));
                }
            }

        }
    }

}
