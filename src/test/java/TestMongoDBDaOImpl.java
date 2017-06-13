import java.util.ArrayList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.nosqldo.dao.impl.MongoDBDaOImpl;
import com.nosqldo.dao.interfaces.BaseDAO;
import com.nosqldo.template.DataTemplate;
import com.nosqldo.template.DataTemplate.Field;

public class TestMongoDBDaOImpl implements BaseDAO {

	public void persistDataTemplate(DataTemplate dataTemplate) {

		try {

			DataTemplate template = new DataTemplate();

			template.setOperationType("insert");
			template.setRowKey("rowkey");
			template.setTableName("mycollection");

			List<Field> fields = new ArrayList<Field>();

			Field f = new Field();
			f.setName("key");
			f.setValue("someValue");

			Field f1 = new Field();
			f1.setName("state");
			f1.setValue("ALASKA");

			Field f2 = new Field();
			f2.setName("email");
			f2.setValue("test.xyz@yahoo.com");

			Field f3 = new Field();
			f3.setName("full_name");
			f3.setValue("xyz JI");
			fields.add(f);
			fields.add(f1);
			fields.add(f2);
			fields.add(f3);

			template.getField().addAll(fields);

			MongoClient mongoClient = new MongoClient("IP ADDRESS", 27017);
			MongoDBDaOImpl m = new MongoDBDaOImpl();

			m.persistDataTemplate(template);

			DB db = mongoClient.getDB("test");

			DBCollection coll = db.getCollection("mycollection");

			List<DBObject> list = coll.getIndexInfo();

			DBCursor cursor = coll.find();
			try {
				while (cursor.hasNext()) {
					System.err.println(cursor.next());
				}
			} finally {
				cursor.close();
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public DataTemplate readDataTemplate(DataTemplate dataTemplate) {

		try {
			DataTemplate template = new DataTemplate();

			template.setOperationType("insert");
			template.setRowKey("someValue");
			template.setTableName("mycollection");

			List<Field> fields = new ArrayList<Field>();

			Field f = new Field();
			f.setName("key");
			f.setValue("someValue");

			Field f1 = new Field();
			f1.setName("state");
			f1.setValue("ALASKA");

			Field f2 = new Field();
			f2.setName("email");
			f2.setValue("test.xyz@yahoo.com");

			Field f3 = new Field();
			f3.setName("full_name");
			f3.setValue("xyz JI");

			fields.add(f);
			fields.add(f1);
			fields.add(f2);
			fields.add(f3);

			template.getField().addAll(fields);

			MongoDBDaOImpl m = new MongoDBDaOImpl();

			DataTemplate d = m.readDataTemplate(template);

			System.out.println(d.toString());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataTemplate;

	}

	public void updateDataTemplate(DataTemplate dataTemplate) {
		DataTemplate template = new DataTemplate();

		template.setOperationType("insert");
		template.setRowKey("someValue");
		template.setTableName("mycollection");

		List<Field> fields = new ArrayList<Field>();

		Field f = new Field();
		f.setName("key");
		f.setValue("rowkey");
		f.setUpdate("false");
		f.setQuery("true");

		Field f1 = new Field();
		f1.setName("state");

		f1.setValue("DOMINICKAND");
		f1.setUpdate("false");
		f1.setQuery("true");

		Field f2 = new Field();
		f2.setName("email");

		Field f3 = new Field();
		f3.setName("full_name");
		f3.setValue("NAME");
		f3.setQuery("false");
		f3.setUpdate("true");

		fields.add(f);
		fields.add(f1);
		fields.add(f2);
		fields.add(f3);

		template.getField().addAll(fields);

		MongoDBDaOImpl m = new MongoDBDaOImpl();

		m.updateDataTemplate(template);
		// DataTemplate d = m.readDataTemplate(template);

		// System.out.println(d.toString());

	}

	public void deleteDataTemplate(DataTemplate dataTemplate) {
		DataTemplate template = new DataTemplate();

		template.setOperationType("delete");
		template.setRowKey("rowkey");
		template.setTableName("mycollection");

		List<Field> fields = new ArrayList<Field>();

		Field f = new Field();
		f.setName("key");
		f.setValue("keyValue");
		f.setUpdate("false");
		f.setQuery("true");

		Field f1 = new Field();
		f1.setName("state");

		f1.setValue("DOMINICKAND");
		f1.setUpdate("false");
		f1.setQuery("true");

		Field f2 = new Field();
		f2.setName("email");

		Field f3 = new Field();
		f3.setName("full_name");
		f3.setValue("FIRSTNAME LASTNAME");
		f3.setQuery("false");
		f3.setUpdate("true");

		// fields.add(f);
		// fields.add(f1);
		// fields.add(f2);
		fields.add(f3);

		template.getField().addAll(fields);

		MongoDBDaOImpl m = new MongoDBDaOImpl();

		m.deleteDataTemplate(template);
		// DataTemplate d = m.readDataTemplate(template);

		// System.out.println(d.toString());

	}

}
