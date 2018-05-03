/*
 * Copyright 2013-2015 MongoDB Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package finished.project;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.geoWithinCenter;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Sorts.descending;

public class BlogPostDAO {
    private final MongoCollection<Document> postsCollection;

    public BlogPostDAO(final MongoDatabase blogDatabase) {
        postsCollection = blogDatabase.getCollection("posts");
    }

    public Document findByPermalink(String permalink) {
        Document post = postsCollection.find(eq("permalink", permalink)).first();

        // fix up if a post has no likes
        if (post != null) {
            List<Document> comments = (List<Document>) post.get("comments");
            for (Document comment : comments) {
                if (!comment.containsKey("num_likes")) {
                    comment.put("num_likes", 0);
                }
            }
        }
        return post;
    }

    public List<Document> findByDateDescending(int limit) {
        return postsCollection.find().sort(descending("date"))
                              .limit(limit)
                              .into(new ArrayList<Document>());
    }

    public List<Document> findByTagDateDescending(final String tag) {
        return postsCollection.find(eq("tags", tag))
                              .sort(descending("date"))
                              .limit(10)
                              .into(new ArrayList<Document>());
    }

    public String addPost(String title, String body, List tags, String username) {
        String permalink = title.replaceAll("\\s", "_"); // whitespace becomes _
        permalink = permalink.replaceAll("\\W", ""); // get rid of non alphanumeric
        permalink = permalink.toLowerCase();

        Document post = new Document("title", title)
                        .append("author", username)
                        .append("body", body)
                        .append("permalink", permalink)
                        .append("tags", tags)
                        .append("comments", new ArrayList())
                        .append("date", new Date());

        postsCollection.insertOne(post);

        return permalink;
    }

    public void addPostComment(final String name, final String email, final String body, final String permalink) {
        Document comment = new Document("author", name)
                           .append("body", body);

        if (email != null && !email.isEmpty()) {
            comment.append("email", email);
        }

        postsCollection.updateOne(eq("permalink", permalink),
                                  new Document("$push", new Document("comments", comment)));
    }

    public void likePost(final String permalink, final int ordinal) {

        Document document = findByPermalink(permalink);
        ArrayList<Document> comments = (ArrayList<Document>)  document.get("comments");
        Document comment = comments.get(ordinal);
        int numLikes = comment.getInteger("num_likes", 0) + 1;
        System.out.printf("likePost(): permalink=%s, ordinal = %d, numLikes=%d, numComments=%d\n", permalink, ordinal, numLikes, comments.size());
//        comment.put("num_likes", numLikes);
//        Document comment2 = comments.get(ordinal);
//
        String fieldName = String.format("comments.%d.num_likes", ordinal);

        boolean useIncrSpec = true;

        //noinspection ConstantConditions
        if (useIncrSpec) {
            // Here is the incr approach
            Document incrFieldSpec = new Document(fieldName, 1);
            Document incrSpec = new Document("$inc", incrFieldSpec);
            postsCollection.updateOne(eq("permalink", permalink), incrSpec);
        } else {
            // Here is the set and replace approach
            Document setFieldSpec = new Document(fieldName, numLikes);
            Document updateSpec = new Document("$set", setFieldSpec);
            postsCollection.updateOne(eq("permalink", permalink), updateSpec);
        }


        // The following code is just debugging code to verify the change.
        document = findByPermalink(permalink);
        comments = (ArrayList<Document>)  document.get("comments");
        comment = comments.get(ordinal);
        int updatedNumLikes = comment.getInteger("num_likes", 0);
        System.out.printf("likePost(): updated numLikes=%d\n", updatedNumLikes);










        //
        //
        // XXX Final Question 4 - work here
        // You must increment the number of likes on the comment in position `ordinal`
        // on the post identified by `permalink`.
        //
        //
    }
}
