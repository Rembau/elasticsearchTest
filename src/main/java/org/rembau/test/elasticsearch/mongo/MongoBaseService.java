package org.rembau.test.elasticsearch.mongo;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * mongodb业务层基类
 * @author: wk
 * @created: 2014年11月7日 上午9:49:18
 */
public abstract class MongoBaseService {
	public final Logger log = LoggerFactory.getLogger(getClass());

	@Autowired
    @Qualifier("mongoBaseDao")
	public MongoBaseDao mongoBaseDao;
	
	/**
	 * 添加记录
	 */
	public <T> void add(T t){
		mongoBaseDao.add(t);
	}
	public <T> void upsert(Map<String, Object> queryMap, T t, Class<T> entityClass){
		mongoBaseDao.upsert(queryMap,
				mongoBaseDao.methodForGet(t), entityClass);

	}
	public <T> void upsert(Map<String, Object> condMap, Update update, Class<T> entityClass){
		mongoBaseDao.upsert(condMap, update, entityClass);
	}
	
	/**
	 * 删除记录
	 */
	public <T> void remove(T t) {
		mongoBaseDao.remove(t);
	}
	
	
	/**
	 * 删除指定Id的记录
	 */
	public <T> void removeById(ObjectId id, T t) {
		mongoBaseDao.removeById(id, t);
	}
	
	
	/**
	 * 删除指定条件的记录
	 */
	public <T> void remove(Map<String, Object> condMap, Class<T> entityClazz) {
		mongoBaseDao.remove(condMap, entityClazz);
	}
	
	
	/**
	 * 删除指定条件的记录
	 */
	public <T> void remove(Query q, Class<T> entityClazz) {
		mongoBaseDao.remove(q, entityClazz);
	}


	/**
	 * 根据Id获取记录
	 */
	public <T> T getById(ObjectId id, Class<T> entityClazz) {
		return mongoBaseDao.getById(id, entityClazz);
	}

	
	/**
	 * 根据多个条件返回1条记录
	 */
	public <T> T getOne(Map<String, Object> condMap, Class<T> entityClazz) {
		return mongoBaseDao.getOne(condMap, entityClazz);
	}

	
	/**
	 * 根据1个条件返回1条记录
	 */
	public <T> T getOne(Query query, Class<T> entityClazz){
		return mongoBaseDao.getOne(query, entityClazz);
	}

	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz, String collectionName, int offset) {
		return mongoBaseDao.list(condMap, entityClazz, collectionName, offset);
	}
	
	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz, int offset) {
		return mongoBaseDao.list(condMap, entityClazz, offset);
	}
	
	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz, String collectionName, int offset, int pageSize) {
		return mongoBaseDao.list(condMap, entityClazz, collectionName, offset, pageSize);
	}
	
	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Query query, Class<T> entityClazz, String collectionName) {
		return mongoBaseDao.list(query, entityClazz, collectionName);
	}
	public <T> List<T> list(Query query, Class<T> entityClazz, String collectionName, int offset, int pageSize) {
		return mongoBaseDao.list(query, entityClazz, collectionName,offset,pageSize);
	}

	/**
	 * 根据多个条件返回多条记录
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz) {
		return mongoBaseDao.list(condMap, entityClazz);
	}
	
	
	/**
	 * 根据单个条件返回多条记录
	 */
	public <T> List<T> list(Query query, Class<T> entityClazz) {
		return mongoBaseDao.list(query, entityClazz);
	}
	
	
	/**
	 * 根据字段来获取分组列表
	 */
	public <T> List<T> listGroupBy(Criteria criteria, String collectionName, GroupBy groupByFieldName, Class<T> entityClazz) {
		List<T> resultList = mongoBaseDao.listGroupBy(criteria, collectionName, groupByFieldName, entityClazz);
		return resultList;
	}

	
	/**
	 * 根据实体对象对应的类查询所有记录
	 */
	public <T> List<T> listAll(Class<T> entityClazz){
		return mongoBaseDao.listAll( entityClazz );
	}

    /**
     * 把实体对象装换成要修改器对象
     */
    public <T> Update methodForGet(T obj){
        return mongoBaseDao.methodForGet(obj);
    }
    
	/**
	 * 判断是否存在满足条件的记录
	 */
	public boolean isExists(Map<String, Object> condMap, Class<?> entityClazz) {
		return mongoBaseDao.isExists(condMap, entityClazz);
	}


	/**
	 * 根据多个条件统计数量
	 */
	public long count(Map<String, Object> condMap, Class<?> entityClazz) {
		return mongoBaseDao.count( condMap, entityClazz );
	}
	
	
	/**
	 * 根据单个条件统计数量
	 */
	public long count(Query q, Class<?> entityClazz) {
		return mongoBaseDao.count( q, entityClazz );
	}


	/**
	 * 获取满足条件的记录数
	 */
	public long count(Map<String, Object> condMap, Criteria[] cris, Class<?> entityClazz) {
		return mongoBaseDao.count(condMap, cris, entityClazz);
	}
	
	
	/**
	 * 更新对象
	 */
	public boolean update(Map<String, Object> condMap, Update update, Class<?> entityClass) {
		return mongoBaseDao.update(condMap, update, entityClass);
	}

    public boolean update(Query q, Update update, Class<?> entityClass) {
        return mongoBaseDao.update(q, update, entityClass);
    }

    public boolean updateAll(Map<String, Object> condMap, Update update, Class<?> entityClass) {
        return mongoBaseDao.updateAll(condMap, update, entityClass);
    }
	/**
	 * 
	 * @param query
	 * @param update
	 * @param entityClass
	 * @return
	 */
	public <T> boolean updateAll(Query query, Update update, Class<T> entityClass){
		return mongoBaseDao.updateAll(query, update, entityClass);
	}
	
	/**
	 * 获取表的名称
	 */
	public String getCollectionName(Class<?> entityClazz ) {
		return mongoBaseDao.getCollectionName( entityClazz );
	}
	
	
	/**
	 * 根据名称获取数据库的类
	 */
	public Class<?> getCollection(String collectionName ) {
		return mongoBaseDao.getCollection( collectionName );
	}
	
	public int getSequence(String tableName) {
		Query query = Query.query(Criteria.where("tableName").is(tableName));
		Update update = new Update();
		update.inc("cnt", 1);
		SequenceCollection ret = this.mongoBaseDao.findAndModify(query, update,
				SequenceCollection.class);
		if (ret == null) {
			SequenceCollection coll = new SequenceCollection();
			coll.setTableName(tableName);
			coll.setCnt(1);
			this.add(coll);
			return 1;
		} else {
			return ret.getCnt() + 1;
		}
	}
	
	@Document
	public static class SequenceCollection implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		private String tableName;
		private int cnt;
		public String getTableName() {
			return tableName;
		}
		public void setTableName(String tableName) {
			this.tableName = tableName;
		}
		public int getCnt() {
			return cnt;
		}
		public void setCnt(int cnt) {
			this.cnt = cnt;
		}
	}

}
