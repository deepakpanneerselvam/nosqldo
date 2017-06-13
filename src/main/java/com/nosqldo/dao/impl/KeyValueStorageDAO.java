package com.nosqldo.dao.impl;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.Jedis;

import com.nosqldo.dao.interfaces.BaseDAO;
import com.nosqldo.template.DataTemplate;
import com.nosqldo.template.DataTemplate.Field;
import com.nosqldo.util.NoSQLConstants;

public class KeyValueStorageDAO implements BaseDAO {

	public void persistDataTemplate(DataTemplate dataTemplate) {
		Jedis jedisInstance = new Jedis(NoSQLConstants.REDIS_HOST);
		List<Field> list = dataTemplate.getField();

		for (Field field : list) {
			System.err.println(field.getName() + " - " + field.getValue());
			jedisInstance.set(field.getName(), field.getValue());
		}

	}

	public DataTemplate readDataTemplate(DataTemplate dataTemplate) {

		Jedis jedis = new Jedis(NoSQLConstants.REDIS_HOST);

		// TODO Auto-generated method stub
		List<Field> list = dataTemplate.getField();

		for (Field field : list) {
			field.setValue(jedis.get(field.getName()));
		}

		return dataTemplate;
	}

	public void updateDataTemplate(DataTemplate dataTemplate) {
		Jedis jedisInstance = new Jedis(NoSQLConstants.REDIS_HOST);
		List<Field> list = dataTemplate.getField();

		for (Field field : list) {
			System.out.println(field.getName() + " - " + field.getValue());
			jedisInstance.set(field.getName(), field.getValue());
		}

	}

	public void deleteDataTemplate(DataTemplate dataTemplate) {

		Jedis jedisInstance = new Jedis(NoSQLConstants.REDIS_HOST);

		// TODO Auto-generated method stub
		List<Field> list = dataTemplate.getField();

		for (Field field : list) {
			Set<String> keys = jedisInstance.keys(field.getName());
			for (String key : keys) {

				jedisInstance.del(key);
			}
		}

	}

}
