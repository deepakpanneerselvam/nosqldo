import java.util.ArrayList;
import java.util.List;

import com.nosqldo.dao.impl.CassandraDaoImpl;
import com.nosqldo.template.DataTemplate;
import com.nosqldo.template.DataTemplate.Field;

public class TestNoSqlDO {
	
	
	public void testCassandraDaoImpl(){
		CassandraDaoImpl c = new CassandraDaoImpl();
		DataTemplate template = new DataTemplate();

		template.setOperationType("delete");
		template.setRowKey("rowkey");
		template.setTableName("users");

		List<Field> fields = new ArrayList<Field>();

		Field f = new Field();
		f.setName("key");
		f.setValue("firstname");

		Field f1 = new Field();
		f1.setName("state");
		f1.setValue("TEXAS");

		Field f2 = new Field();
		f2.setName("email");
		f2.setValue("xyz.jj@gmail.com");

		Field f3 = new Field();
		f3.setName("full_name");
		f3.setValue("firstname_lastname");
		// fields.add(f);
		fields.add(f1);
		// fields.add(f2);

		// fields.add(f3);

		template.getField().addAll(fields);

		StringBuilder sb = c.queryBuilder(template);

		System.err.println(sb.toString());

		c.updateDataTemplate(template);

		template.setOperationType("select");

		DataTemplate d = c.readDataTemplate(template);

		List<Field> s = d.getField();

		for (Field field : s) {
			System.out.println(field.getName() + " - " + field.getValue());

		}

		
	}

	public void testMongoDbDAOImpl(){
		
	}
	
}
