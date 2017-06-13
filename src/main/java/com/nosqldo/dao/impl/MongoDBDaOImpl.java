package com.nosqldo.dao.impl;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.nosqldo.dao.interfaces.BaseDAO;
import com.nosqldo.template.DataTemplate;
import com.nosqldo.template.DataTemplate.Field;
import com.nosqldo.util.NoSQLConstants;

public class MongoDBDaOImpl implements BaseDAO {

	public void persistDataTemplate(DataTemplate dataTemplate) {

		try {
			MongoClient mongoClient = new MongoClient(
					NoSQLConstants.MONGO_CLIENT_HOST,
					NoSQLConstants.MONGO_CLIENT_DEFAULT_PORT);
			DB db = mongoClient.getDB(NoSQLConstants.MONGO_SCHEMA);
			// table name
			DBCollection items = db.getCollection(dataTemplate.getTableName());

			Map<String, Object> documentMap = new HashMap<String, Object>();
			List<Field> fields = dataTemplate.getField();
			for (Field field : fields) {
				documentMap.put(field.getName(), field.getValue());
			}
			// documentMap.put(dataTemplate.getTableName(), documentMapDetail);
			items.insert(new BasicDBObject(documentMap));

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public DataTemplate readDataTemplate(DataTemplate dataTemplate) {
		try {

			MongoClient mongoClient = new MongoClient(
					NoSQLConstants.MONGO_CLIENT_HOST,
					NoSQLConstants.MONGO_CLIENT_DEFAULT_PORT);
			DB db = mongoClient.getDB(NoSQLConstants.MONGO_SCHEMA);
			DBCollection collection = db.getCollection(dataTemplate
					.getTableName());

			BasicDBObject whereQuery = new BasicDBObject();
			List<Field> list = dataTemplate.getField();

			for (Field field : list) {
				if (field.getValue() != null) {
					System.err.println(field.getName() + " - "
							+ field.getValue());
					whereQuery.put(field.getName(), field.getValue());
				}
			}

			DBCursor cursor = collection.find(whereQuery);
			while (cursor.hasNext()) {
				DBObject dbObject = cursor.next();
				for (Field field : list) {
					field.setValue(dbObject.get(field.getName()).toString());
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataTemplate;
	}

	public void updateDataTemplate(DataTemplate dataTemplate) {

		try {
			MongoClient mongoClient = new MongoClient(
					NoSQLConstants.MONGO_CLIENT_HOST,
					NoSQLConstants.MONGO_CLIENT_DEFAULT_PORT);
			DB db = mongoClient.getDB(NoSQLConstants.MONGO_SCHEMA);
			DBCollection collection = db.getCollection(dataTemplate
					.getTableName());

			BasicDBObject searchUpdate = new BasicDBObject();

			BasicDBObject basicDBObjectUpdate = new BasicDBObject();

			List<Field> list = dataTemplate.getField();

			for (Field field : list) {
				if (field.getQuery() != null && field.getUpdate() != null) {
					if (field.getQuery().equals("true")) {
						System.out.println(field.getName() + "query"
								+ field.getValue());

						searchUpdate.append(field.getName(), field.getValue());
					}
					if (field.getUpdate().equals("true")) {
						System.out.println(field.getName() + "update"
								+ field.getValue());
						basicDBObjectUpdate.append("$set", new BasicDBObject(
								field.getName(), field.getValue()));
					}
				}
			}

			collection.update(searchUpdate, basicDBObjectUpdate);

			System.out.println("Document updated successfully");

		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void deleteDataTemplate(DataTemplate dataTemplate) {
		try {

			MongoClient mongoClient = new MongoClient(
					NoSQLConstants.MONGO_CLIENT_HOST,
					NoSQLConstants.MONGO_CLIENT_DEFAULT_PORT);
			DB db = mongoClient.getDB(NoSQLConstants.MONGO_SCHEMA);
			DBCollection collection = db.getCollection(dataTemplate
					.getTableName());

			BasicDBObject searchDelete = new BasicDBObject();

			List<Field> list = dataTemplate.getField();

			for (Field field : list) {

				searchDelete.append(field.getName(), field.getValue());

			}

			collection.remove(searchDelete);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
