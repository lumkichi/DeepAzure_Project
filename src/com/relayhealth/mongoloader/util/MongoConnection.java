package com.relayhealth.mongoloader.util;

import org.bson.Document;

import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * A utility class simplified for making direct connections to MongoDB in 
 * Microsoft Azure's Cosmos DB
 * @author lawrence.spiwak@relayhealth.com
 *
 */
public class MongoConnection {

  private MongoClient mongoClient = null;
  private MongoDatabase mongoDB = null;

  public MongoConnection() {

    try {
      // Copy out the MongoClient string from the Azure Portal
      mongoClient = new MongoClient(new MongoClientURI("mongodb://lummymongo:Hb7oqBPsctlYiWq7qljdHg9sAIdDCOVladpTsRab32GdjluEJaY45ZtKta5Y3VsxntUhU0afpE41kIxezeoVmA==@lummymongo.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"));
      mongoDB = mongoClient.getDatabase("mrdd_prod");
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

  public MongoCollection<Document> getMRDDCollection() {
    return mongoDB.getCollection("MRDD");
  }

  public MongoCollection<Document> getPBICollection() {
    return mongoDB.getCollection("PBI");
  }

  public MongoDatabase getMongoDB() {
    return mongoDB;
  }

  public void close() {
    if (mongoClient != null) {
      mongoClient.close();
    }
    mongoClient = null;
  }

}
