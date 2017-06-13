package com.nosqldo.dao.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;

import com.nosqldo.dao.interfaces.BaseDAO;
import com.nosqldo.template.DataTemplate;
import com.nosqldo.template.DataTemplate.Field;
import com.nosqldo.util.ResourceManagerLocator;

public class HbaseDaoImpl implements BaseDAO {

	public void persistDataTemplate(DataTemplate dataTemplate) {
		try {
			Configuration conf = ResourceManagerLocator.getHbaseConfig();
			
			HTable table = new HTable(conf, dataTemplate.getTableName());
			Put put = new Put(Bytes.toBytes(dataTemplate.getRowKey()));

			List<Field> fields = dataTemplate.getField();

			for (Field field : fields) {

				put.add(Bytes.toBytes(field.getCoulumFamily()),
						Bytes.toBytes(field.getName()),
						Bytes.toBytes(field.getValue()));

			}

			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public DataTemplate readDataTemplate(DataTemplate dataTemplate) {
		try {
			Configuration conf = ResourceManagerLocator.getHbaseConfig();
			
			HTable table = new HTable(conf, dataTemplate.getTableName());
			Get get = new Get(dataTemplate.getRowKey().getBytes());
			Result rs = table.get(get);

			List<DataTemplate.Field> fields = new ArrayList<DataTemplate.Field>();
			Field f = new Field();
			for (KeyValue kv : rs.raw()) {
				f.setCoulumFamily(new String(kv.getFamily()));

				System.out.print(new String(kv.getRow()) + " ");
				System.out.print(new String(kv.getFamily()) + ":");
				System.out.print(new String(kv.getQualifier()) + " ");
				System.out.print(kv.getTimestamp() + " ");
				System.out.println(new String(kv.getValue()));
				fields.add(f);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return dataTemplate;
	}

	public void updateDataTemplate(DataTemplate dataTemplate) {
		try {
			Configuration conf = ResourceManagerLocator.getHbaseConfig();
			
			HTable table = new HTable(conf, dataTemplate.getTableName());
			Put put = new Put(Bytes.toBytes(dataTemplate.getRowKey()));

			List<Field> fields = dataTemplate.getField();

			for (Field field : fields) {

				put.add(Bytes.toBytes(field.getCoulumFamily()),
						Bytes.toBytes(field.getName()),
						Bytes.toBytes(field.getValue()));

			}

			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void deleteDataTemplate(DataTemplate dataTemplate) {
		try {
			Configuration conf = ResourceManagerLocator.getHbaseConfig();
			
			HTable table = new HTable(conf, dataTemplate.getTableName());
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(dataTemplate.getRowKey().getBytes());
			list.add(del);
			table.delete(list);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
