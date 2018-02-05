package com.relayhealth.hadoop.util;


import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.util.JSON;
import com.relayhealth.mongoloader.util.MongoConnection;

/**
 * The Mongo DB data load worker extending the Mapper object, called by the Map/Reduce
 * infrastructure.  Parses one line provided by Map/Reduce and connects (if necessary)
 * to MongoDB and inserts/updates/deletes a document (record).
 * @author lawrence.spiwak@relayhealth.com
 *
 */
/**
 * @author ekgriol
 *
 */
@SuppressWarnings("deprecation")
public class MongoMap extends Mapper<Object, Text, Text, Text>  {

  boolean dirtyInsert = false;
  String content = "";
  String processDate = "";
  char sepChar = 02;

  // Mongo Connection
  MongoConnection mongoConn = null;
  MongoCollection<Document> collection = null;


  // Various field ID's to load into Mongo
  private static String PHARMACY_NK = "PHARMACY_NK";
  private static String RXNORM_CODE = "RXNORM_CODE";
  private static String DS_TXN_ID = "DS_TXN_ID";
  private static String SWITCH_XMISSION_ID = "SWITCH_XMISSION_ID";
  private static String SWITCH_TXN_NUM = "SWITCH_TXN_NUM";
  private static String SWITCH_THREAD_ID = "SWITCH_THREAD_ID";
  private static String LOC = "loc";
  private static String REQUEST_B1B3 = "REQUEST_B1B3";
  private static String FINANCIAL_SPONSOR_ID = "FINANCIAL_SPONSOR_ID";
  private static String ZIP_SEC = "ZIP_SEC";
  private static String NCPDP_LOC_STATE = "STATE";
  private static String TXN_DATE_TIME = "TXN_DATE_TIME";
  private static String POST_DATE = "POST_DATE";
  private static String PROCESS_DATE = "PROCESS_DATE";

  public MongoMap() {
    // On creation of object, make a connection to MongoDB
    if (mongoConn == null) {
      mongoConn = new MongoConnection();
      collection = mongoConn.getMRDDCollection();
    }

    // Get the process date as today's date YYYYMMDD
    java.util.Date date= new java.util.Date();
    Timestamp ts = new Timestamp(date.getTime());
    processDate = ts.toString().substring(0,10).replace("-","");
  }

  public void finalize() throws Throwable {
    // On deletion of object, clean up connection to MongoDB
    if (mongoConn != null) {
      mongoConn.close();
    }

    super.finalize();
  }

  /**
   * Map/Reduce will call this map() method passing in the object Key, Value and Context<br/>
   * We are concerned mainly with the 'value' parameter.  This class has been simplified from
   * the original MRDDLoader which supported bulk inserts - Map/Reduce feeds this class one
   * line at a time.
   */
  public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    // Split the incoming line (delimited by 0x02)
    String[] tokens = value.toString().split("" + sepChar);

    // Check line format for correct number of columns
    if (tokens.length != 12 && tokens.length != 15 && tokens.length != 3) {
      System.err.println("Line does not have the expected values of 3 (del), 12 (old-style ins/upd), 15 (new-style ins/upd) fields");
      System.err.println("Line = " + content);
      System.exit(1);
    }

    // tokens[0] contains the "A" add "U" update "D" delete action flag.  Uppercase it, if non-null
    if (tokens[0] != null) {
      tokens[0] = tokens[0].toUpperCase();
    }


    // Validate the Action token
    if ("A".equals(tokens[0]) || "U".equals(tokens[0]) || "D".equals(tokens[0])) {

      // In this block we are handling Add records
      if ("A".equals(tokens[0])) {
        if (tokens.length != 15 && tokens.length != 12) {
          System.err.println("Received an ADD record without 12 or 15 columns (" + tokens.length + " columns)");
          System.exit(1);
        }

        // A list of doc for bulk insert (not needed here)
        List<Document> insertDocList = new ArrayList<Document>();

        // Build a new bson/json document
        Document doc = new Document();
        doc.append(PHARMACY_NK,tokens[1]);
        doc.append(RXNORM_CODE,tokens[2]);
        doc.append(DS_TXN_ID,tokens[3]);
        doc.append(SWITCH_XMISSION_ID,tokens[4]);
        doc.append(SWITCH_TXN_NUM,tokens[5]);
        doc.append(SWITCH_THREAD_ID,tokens[6]);

        // If lat or long is empty, do not insert a coordinate object
        if (tokens[7] != null && tokens[8] != null && !"".equals(tokens[7].trim()) && !"".equals(tokens[8].trim())) {
          // GeoJSON Point Objects are [longitude, latitude] and not [latitude, longitude]
          String coordinatesJSON = "{ type: \"Point\", coordinates: [ " +  tokens[8] + ", " + tokens[7] + " ] }";
          try {
            DBObject coordinatesObj = (DBObject) JSON.parse(coordinatesJSON);
            doc.append(LOC, coordinatesObj);
          } catch (Exception e) {}
        } else {
          // System.out.println("Received a line with no Lat/Long - not creating a coordinate object: DS_TXN_ID = " + tokens[3]);
        }
        doc.append(TXN_DATE_TIME, tokens[9]);
        doc.append(POST_DATE, tokens[10]);
        doc.append(PROCESS_DATE, processDate);
        if (tokens.length == 12) {
          doc.append(REQUEST_B1B3,tokens[11].getBytes());
        } else {
          doc.append(FINANCIAL_SPONSOR_ID, tokens[11]);
          doc.append(ZIP_SEC, tokens[12]);
          doc.append(NCPDP_LOC_STATE, tokens[13]);
          doc.append(REQUEST_B1B3,tokens[14].getBytes());
        }

        // Add this record to the bulk list (vestigial)
        insertDocList.add(doc);
        dirtyInsert = true;

        // Push data to mongo every bulkSize records (one record, in this case)
        try {
          collection.insertMany(insertDocList);
        } catch (Exception e) {
          // Caught a duplicate key exception, most likely - reattempt to iterate through the bulk and insert
          // individual rows - if duplicate key, reattempt using updates
          System.out.println("Bulk insert failed - attempting single insert/update");
          for (Document obj: insertDocList) {
            try {
              // Attempt an single document insert.
              collection.insertOne(obj);

            } catch (Exception e1) {
              // If insert fails, attempt update
              String pharmacy_nk = obj.getString(PHARMACY_NK);
              String rxnorm_code = obj.getString(RXNORM_CODE);

              BasicDBObject searchQuery = new BasicDBObject();
              searchQuery.append(PHARMACY_NK,pharmacy_nk);
              searchQuery.append(RXNORM_CODE,rxnorm_code);
              BasicDBObject newDoc = new BasicDBObject().append("$set", obj);

              try {
                collection.updateOne(searchQuery, newDoc);
              } catch (Exception e2) {
                System.err.println("Caught an Exception trying to update to DB");
                System.err.println("Key was PHARMACY_NK=" + pharmacy_nk + "  RXNORM_CODE=" + rxnorm_code);
                e.printStackTrace();
                System.exit(1);
              }

            }
          }
        }

        dirtyInsert = false;
        insertDocList = new ArrayList<Document>();

        // In this block we handle Update records.  Attempt to update, if it fails attempt an insert.
      } else if ("U".equals(tokens[0])) {
        if (tokens.length != 15 && tokens.length != 12) {
          System.err.println("Received an UPDATE record without 12 or 15 columns (" + tokens.length + " columns)");
          System.exit(1);
        }

        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.append(PHARMACY_NK,tokens[1]);
        searchQuery.append(RXNORM_CODE,tokens[2]);
        Document updateDoc = new Document();
        updateDoc.append(PHARMACY_NK,tokens[1]);
        updateDoc.append(RXNORM_CODE,tokens[2]);
        updateDoc.append(DS_TXN_ID,tokens[3]);
        updateDoc.append(SWITCH_XMISSION_ID,tokens[4]);
        updateDoc.append(SWITCH_TXN_NUM,tokens[5]);
        updateDoc.append(SWITCH_THREAD_ID,tokens[6]);

        // If lat or long is empty, do not insert a coordinate object
        if (tokens[7] != null && tokens[8] != null && !"".equals(tokens[7].trim()) && !"".equals(tokens[8].trim())) {
          // GeoJSON Point Objects are [longitude, latitude] and not [latitude, longitude]
          String coordinatesJSON = "{ type: \"Point\", coordinates: [ " +  tokens[8] + ", " + tokens[7] + " ] }";
          try {
            DBObject coordinatesObj = (DBObject) JSON.parse(coordinatesJSON);
            updateDoc.append(LOC, coordinatesObj);
          } catch (Exception e) {
          }
        }else {
          System.out.println("Received a line with no Lat/Long - not creating a coordinate object: DS_TXN_ID = " + tokens[3]);
        }

        updateDoc.append(TXN_DATE_TIME, tokens[9]);
        updateDoc.append(POST_DATE, tokens[10]);
        updateDoc.append(PROCESS_DATE, processDate);
        if (tokens.length == 12) {
          updateDoc.append(REQUEST_B1B3,tokens[11].getBytes());
        } else {
          updateDoc.append(FINANCIAL_SPONSOR_ID, tokens[11]);
          updateDoc.append(ZIP_SEC, tokens[12]);
          updateDoc.append(NCPDP_LOC_STATE, tokens[13]);
          updateDoc.append(REQUEST_B1B3,tokens[14].getBytes());
        }
        BasicDBObject newDoc = new BasicDBObject().append("$set", updateDoc);

        try {
          UpdateResult ur = collection.updateOne(searchQuery, newDoc);

          // An update failed to update an existing record - so try an insert instead
          if (ur.getModifiedCount() == 0) {
            try {
              collection.insertOne(updateDoc);
            } catch (Exception e) {
              System.out.println("Update : PHARMACY_NK = " + tokens[1] + "  RNXORM_CODE = " + tokens[2] + " - failed to update and insert.  Exception was " + e.getMessage());
            }
          }
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }

        // Here we handled Delete records.  Simply delete the referenced document by its key
      } else {
        // Do the deletion
        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.append(PHARMACY_NK, tokens[1]);
        searchQuery.append(RXNORM_CODE,tokens[2]);
        try {
          collection.deleteMany(searchQuery);
        } catch (Exception e) {
          e.printStackTrace();
          System.exit(1);
        }
      }
    } else {
      System.err.println("Line does not have the expected Action of 'A', 'U' or 'D' - found '" + tokens[0] + "' instead.");
      System.err.println("Line = " + content);
      System.exit(1);
    }
  }

}
