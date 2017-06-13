package com.nosqldo.dao.impl;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.nosqldo.dao.interfaces.BaseDAO;
import com.nosqldo.template.DataTemplate;
import com.nosqldo.template.DataTemplate.Field;
import com.nosqldo.util.NoSQLConstants;

public class CassandraDaoImpl implements BaseDAO {

	public void persistDataTemplate(DataTemplate dataTemplate) {
		try {
			// TODO Auto-generated method stub
			Class.forName(NoSQLConstants.CASSANDRA_DRIVER);

			java.sql.Connection con = DriverManager
					.getConnection(NoSQLConstants.CASSANDRA_CONNECTION_URL);

			String query = queryBuilder(dataTemplate).toString();

			Statement st = con.createStatement();

			st.executeUpdate(query);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public DataTemplate readDataTemplate(DataTemplate dataTemplate) {
		try {
			// TODO Auto-generated method stub

			// TODO Auto-generated method stub
			Class.forName(NoSQLConstants.CASSANDRA_DRIVER);

			java.sql.Connection con = DriverManager
					.getConnection(NoSQLConstants.CASSANDRA_CONNECTION_URL);

			String query = queryBuilder(dataTemplate).toString();

			Statement st = con.createStatement();

			ResultSet rs = st.executeQuery(query);

			List<Field> fields = dataTemplate.getField();

			while (rs.next()) {

				for (int j = 1; j < rs.getMetaData().getColumnCount() + 1; j++) {
					Field f = new Field();
					f.setName(rs.getMetaData().getColumnName(j));
					f.setValue(rs.getString(rs.getMetaData().getColumnName(j)));
					fields.add(f);

				}
			}

			return dataTemplate;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataTemplate;
	}

	public void updateDataTemplate(DataTemplate dataTemplate) {

		try {
			// TODO Auto-generated method stub
			// TODO Auto-generated method stub
			Class.forName(NoSQLConstants.CASSANDRA_DRIVER);

			java.sql.Connection con = DriverManager
					.getConnection(NoSQLConstants.CASSANDRA_CONNECTION_URL);

			String query = queryBuilder(dataTemplate).toString();

			System.out.println(query);

			Statement st = con.createStatement();

			st.executeUpdate(query);

		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void deleteDataTemplate(DataTemplate dataTemplate) {
		try {
			// TODO Auto-generated method stub

			// TODO Auto-generated method stub
			Class.forName(NoSQLConstants.CASSANDRA_DRIVER);

			java.sql.Connection con = DriverManager
					.getConnection(NoSQLConstants.CASSANDRA_CONNECTION_URL);

			String data = queryBuilder(dataTemplate).toString();

			Statement st = con.createStatement();

			st.executeUpdate(data);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public StringBuilder queryBuilder(DataTemplate dataTemplate) {

		StringBuilder queryBuilder = null;

		if (dataTemplate.getOperationType().compareTo("insert") == 0) {
			StringBuilder queryKeyBuilder = new StringBuilder();
			StringBuilder queryValueBuilder = new StringBuilder();
			List<Field> fields = dataTemplate.getField();

			for (Field field : fields) {

				queryKeyBuilder.append(field.getName());
				queryKeyBuilder.append(",");
				queryValueBuilder.append("'" + field.getValue() + "'");
				queryValueBuilder.append(",");
			}
			String statementKey = queryKeyBuilder.toString();
			String statementValue = queryValueBuilder.toString();

			System.out.println(statementValue);
			queryBuilder = new StringBuilder("insert into ");
			queryBuilder.append(dataTemplate.getTableName() + " ");
			queryBuilder.append(("("
					+ statementKey.substring(0, statementKey.length() - 1)
					+ ") values (" + statementValue.substring(0,
					statementValue.length() - 1)));
			queryBuilder.append(")");

		} else if (dataTemplate.getOperationType().compareTo("select") == 0) {
			List<Field> fields = dataTemplate.getField();
			String key = null;
			String value = null;
			for (Field field : fields) {
				key = field.getName();
				value = field.getValue();

			}
			queryBuilder = new StringBuilder("select * from ");
			queryBuilder.append(dataTemplate.getTableName() + " ");
			queryBuilder.append("where key=");
			queryBuilder.append("'" + dataTemplate.getRowKey() + "'");

		} else if (dataTemplate.getOperationType().compareTo("update") == 0) {
			List<Field> fields = dataTemplate.getField();
			String fieldName = null;
			String fieldValue = null;

			for (Field field : fields) {
				fieldName = field.getName();
				fieldValue = field.getValue();

			}
			// update users set gender='Male' where key='Deepak'
			queryBuilder = new StringBuilder("update ");
			queryBuilder.append(dataTemplate.getTableName() + " ");
			queryBuilder.append(" set ");
			queryBuilder.append(fieldName + "=");
			queryBuilder.append("'" + fieldValue + "'");
			queryBuilder
					.append(" where key='" + dataTemplate.getRowKey() + "'");

		} else if (dataTemplate.getOperationType().compareTo("delete") == 0) {

			// delete from users where key='yomama'
			queryBuilder = new StringBuilder("delete from ");
			queryBuilder.append(dataTemplate.getTableName() + " ");
			queryBuilder
					.append(" where key='" + dataTemplate.getRowKey() + "'");

		}
		return queryBuilder;

	}
 
	
	public void test() throws Exception{
	if(true){
		throw new Exception();
	}
	}

}
