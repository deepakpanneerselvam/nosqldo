package com.nosqldo.dao.interfaces;

import com.nosqldo.template.DataTemplate;

public interface BaseDAO {

	void persistDataTemplate(DataTemplate dataTemplate);

	DataTemplate readDataTemplate(DataTemplate dataTemplate);

	void updateDataTemplate(DataTemplate dataTemplate);

	void deleteDataTemplate(DataTemplate dataTemplate);
}
