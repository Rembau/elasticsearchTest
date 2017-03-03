package org.rembau.test.elasticsearch.mongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapreduce.GroupBy;
import org.springframework.data.mongodb.core.mapreduce.GroupByResults;
import org.springframework.data.mongodb.core.mapreduce.MapReduceResults;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.util.*;

/**
 * 数据访问层
 * @author: wk
 * @created: 2014年11月7日 上午9:55:04
 */
public class MongoBaseDao {

	@Autowired
	private MongoTemplate mongoTemplate;
	@Resource(name="mongoTemplate")
	private MongoOperations mongoOperations;

	public MongoTemplate getMongoTemplate() {
		return mongoTemplate;
	}

	/**
	 * 添加记录
	 * @author: wk
	 * @created: 2014年11月7日 上午10:00:54
	 * @param <T>
	 * @param t
	 */
	public <T> void add( T t ) {
		mongoTemplate.save( t );
	}
	
	/**
	 * 保存所有
	 * @author wk
	 * @created 2015年2月11日 下午3:07:38
	 * @param t
	 */
	public <T> void addAll(Collection<T> t){
		mongoTemplate.insertAll(t);
	}


	/**
	 * 删除指定记录
	 * @author: wk
	 * @created: 2014年11月7日 上午10:03:57
	 * @param t
	 */
	public <T> boolean remove(T t) {
		boolean result = false;
		WriteResult writeResult = mongoTemplate.remove(t);
		if (null != writeResult) {
			if (writeResult.getN() > 0) {
				result = true;
			}
		}
		return result;
	}

	
	/**
	 * 删除指定Id的记录
	 */
	@SuppressWarnings("unchecked")
	public <T> void removeById(ObjectId id, T t) {
		mongoTemplate.remove(getById(id, (Class<T>) t));
	}
	
	/**
	 * 根据 字段值 列表删除对象
	 */
	public <T> void removeByFieldList(List<Object> fieldList, String field, Class<T> entityClazz) {
		
		Criteria crt = Criteria.where( field ).in( fieldList );
		Query q = new Query();
		q.addCriteria( crt );
		mongoTemplate.remove(q, entityClazz);
	}
	
	/**
	 * 根据id列表删除对象
	 */
	@SuppressWarnings("unchecked")
	public <T> void removeByIds(List<ObjectId> ids, T t) {
		
		Criteria crt = Criteria.where("id").in( ids );
		Query q = new Query();
		q.addCriteria( crt );
		mongoTemplate.remove(q, (Class<T>) t);
	}
	
	
	/**
	 * 删除指定条件的记录
	 */
	public <T> void remove(Map<String, Object> condMap, Class<T> entityClazz) {
		Query q = DbUtil.getMapQuery(condMap);
		mongoTemplate.remove(q, entityClazz);
	}
	
	
	/**
	 * 删除指定条件的记录
	 */
	public <T> void remove(Query q, Class<T> entityClazz) {
		mongoTemplate.remove(q, entityClazz);
	}


	/**
	 * 根据Id获取记录
	 */
	public <T> T getById(ObjectId id, Class<T> entityClazz) {
		return mongoTemplate.findById(id, entityClazz);
	}

	
	/**
	 * 根据多个条件返回1条记录
	 */
	public <T> T getOne(Map<String, Object> condMap, Class<T> entityClazz) {
		return mongoTemplate.findOne(DbUtil.getMapQuery(condMap), entityClazz);
	}

	
	/**
	 * 根据1个条件返回1条记录
	 */
	public <T> T getOne(Query query, Class<T> entityClazz){
		return mongoTemplate.findOne(query, entityClazz);
	}

	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz, String collectionName, int offset) {
		return mongoTemplate.find(DbUtil.getMapQuery(condMap).skip( offset ).limit( 100 ), entityClazz, collectionName);
	}
	
	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz, int offset) {
		return mongoTemplate.find(DbUtil.getMapQuery(condMap).skip( offset ).limit( 100), entityClazz);
	}
	
	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz, String collectionName, int offset, int pageSize) {
		return mongoTemplate.find(DbUtil.getMapQuery(condMap).skip( offset ).limit( pageSize ), entityClazz, collectionName);
	}
	

	/**
	 * 根据多个条件返回多条记录
	 */
	public <T> List<T> list(Map<String, Object> condMap, Class<T> entityClazz) {
		return mongoTemplate.find(DbUtil.getMapQuery(condMap), entityClazz);
	}
	
	
	/**
	 * 根据多个条件返回多条记录,分页功能
	 */
	public <T> List<T> list(Map<String, Object> condMap, int offset, Class<T> entityClazz) {
		return mongoTemplate.find(DbUtil.getMapQuery(condMap).skip( offset ).limit( 100 ), entityClazz);
	}
	
	
	/**
	 * 根据多个条件返回多条记录,分页功能
	 */
	public <T> List<T> list(Query query, int offset, Class<T> entityClazz) {
		return mongoTemplate.find( query.skip( offset ).limit( 100 ), entityClazz);
	}
	
	/**
	 * 根据多个条件返回多条记录,分页功能
	 */
	public <T> List<T> list(Query query, int offset, int limit, Class<T> entityClazz) {
		return mongoTemplate.find( query.skip( offset ).limit( limit ), entityClazz);
	}
	
	
	/**
	 * 根据单个条件返回多条记录
	 */
	public <T> List<T> list(Query query, Class<T> entityClazz) {
		return mongoTemplate.find(query, entityClazz);
	}
	

	
	/**
	 * 手动指定查询的表和 需要填充的对象
	 */
	public <T> List<T> list(Query query, Class<T> entityClazz, String collectionName) {
		return mongoTemplate.find(query, entityClazz, collectionName);
	}
	public <T> List<T> list(Query query, Class<T> entityClazz, String collectionName, int offset, int pageSize) {
		return mongoTemplate.find( query.skip( offset ).limit(pageSize), entityClazz);
	}
	
	/**
	 * 查询不重复记录
	 */
	@SuppressWarnings("rawtypes")
	public List listDistinct(String collecitonName, String distinctFieldName, Query query) {
		return mongoTemplate.getCollection( collecitonName ).distinct( distinctFieldName, query.getQueryObject());
	}
	
	
	/**
	 * 根据字段来获取分组列表
	 */
	public <T> List<T> listGroupBy(Criteria criteria, String collectionName, GroupBy groupByFieldName, Class<T> entityClazz) {
		GroupByResults<T> result = mongoTemplate.group(criteria, collectionName, groupByFieldName, entityClazz);
		// System.out.println(result.getRawResults().toString());
		Iterator<T> resultItr = result.iterator();
		List<T> resultList = new ArrayList<T>();
		
		while ( resultItr.hasNext() ) {
			resultList.add( resultItr.next() );
		}
		
		return resultList;
	}

	
	/**
	 * 根据实体对象对应的类查询所有记录
	 */
	public <T> List<T> listAll(Class<T> entityClazz){
		return mongoTemplate.findAll(entityClazz);
	}
	
	
	/**
	 * 判断是否存在满足条件的记录
	 */
	public boolean isExists(Map<String, Object> condMap, Class<?> entityClazz) {
		return mongoTemplate.count(DbUtil.getMapQuery(condMap), entityClazz) > 0;
	}


	/**
	 * 根据多个条件统计数量
	 */
	public long count(Map<String, Object> condMap, Class<?> entityClazz) {
		Query q = DbUtil.getMapQuery( condMap );
		return mongoTemplate.count(q, entityClazz);
	}
	
	
	/**
	 * 根据单个条件统计数量
	 */
	public long count(Query q, Class<?> entityClazz) {
		return mongoTemplate.count(q, entityClazz);
	}


	/**
	 * 获取满足条件的记录数
	 */
	public long count(Map<String, Object> condMap, Criteria[] cris, Class<?> entityClazz) {
		Query q = DbUtil.getMapQuery(condMap);
		if (cris != null) {
			for(int i=0;i<cris.length;i++)
			q.addCriteria(cris[i]);
		}
		return mongoTemplate.count(q, entityClazz);
	}


	/**
	 * 更新对象
	 */
	public <T> boolean update(Map<String, Object> condMap, Update update, Class<T> entityClass) {
		
		boolean result = false;
		Query query = DbUtil.getMapQuery(condMap);

		WriteResult writeResult = mongoTemplate.updateFirst(query, update, entityClass);
		if (null != writeResult) {
			if (writeResult.getN() > 0) {
				result = true;
			}
		}
		
		return result;
	}

    public <T> boolean update(Query query, Update update, Class<T> entityClass) {

        boolean result = false;

        WriteResult writeResult = mongoTemplate.updateFirst(query, update, entityClass);
        if (null != writeResult) {
            if (writeResult.getN() > 0) {
                result = true;
            }
        }

        return result;
    }
	
	/**
	 * 更新全部对象
	 */
	public <T> boolean updateAll(Map<String, Object> condMap, Update update, Class<T> entityClass) {
		
		boolean result = false;
		Query query = DbUtil.getMapQuery(condMap);

		WriteResult writeResult = mongoTemplate.updateMulti(query, update, entityClass);
		if (null != writeResult) {
			if (writeResult.getN() > 0) {
				result = true;
			}
		}
		
		return result;
	}
	
	
	/**
	 * 更新全部对象
	 */
	public <T> boolean updateAll(Query query, Update update, Class<T> entityClass) {
		
		boolean result = false;

		WriteResult writeResult = mongoTemplate.updateMulti(query, update, entityClass);
		if (null != writeResult) {
			if (writeResult.getN() > 0) {
				result = true;
			}
		}
		
		return result;
	}
	
	
	/**
	 * 更新或插入对象
	 */
	public <T> boolean upsert(Map<String, Object> condMap, Update update, Class<T> entityClass) {
		
		boolean result = false;
		Query query = DbUtil.getMapQuery(condMap);

		WriteResult writeResult = mongoTemplate.upsert(query, update, entityClass);
		if (null != writeResult) {
			if (writeResult.getN() > 0) {
				result = true;
			}
		}		
		return result;
	}
	
	
	
	/**
	 * 把实体对象装换成要修改器对象
	 */
	@SuppressWarnings("rawtypes")
	public <T> Update methodForGet(T obj){
		Class clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		Update update = new Update(); 
		for (Field field : fields) { //获得属性
			PropertyDescriptor pd;
			try {
				if( !field.getName().equals("serialVersionUID") ){
					pd = new PropertyDescriptor(field.getName(), clazz);
					Method getMethod=pd.getReadMethod(); //获得get方法
					Object o = getMethod.invoke(obj); //执行get方法返回一个Object
					if( o != null){ //如果要修改的值不为空则添加到修改对象中
						if( StringUtils.isEmpty( o ) ){ //如果o为空字符串则字段类型必须为string类型
							if( field.getType().equals( String.class ) ){
								update.set( field.getName(), o );
							}
						}else{
							if( field.getType().equals( ObjectId.class ) ){
								o = new ObjectId( o.toString() );
							}
							update.set( field.getName(), o );
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return update;
	}
	/**
	 * 把实体对象装换成要修改器对象
	 */
	@SuppressWarnings("rawtypes")
	public <T> Update methodForGetAll(T obj){
		Class clazz = obj.getClass();
		Field[] fields1 = clazz.getDeclaredFields();
		List<Field> fieldList=new ArrayList<Field>();
		for(int i=0;i<fields1.length;i++){
				fieldList.add(fields1[i]);
		}
		if(clazz.getSuperclass()!=null){
			Field[] fields2=clazz.getSuperclass().getDeclaredFields();
			for(int i=0;i<fields2.length;i++){
				if(!fieldList.contains(fields2[i])){
					fieldList.add(fields2[i]);
				}
			}
		}
		Field[] fields =new Field[fieldList.size()];
		fieldList.toArray(fields);
		Update update = new Update(); 
		for (Field field : fields) { //获得属性
			PropertyDescriptor pd;
			try {
				if( !field.getName().equals("serialVersionUID") ){
					pd = new PropertyDescriptor(field.getName(), clazz);
					Method getMethod=pd.getReadMethod(); //获得get方法
					Object o = getMethod.invoke(obj); //执行get方法返回一个Object
					if( o != null){ //如果要修改的值不为空则添加到修改对象中
						if( StringUtils.isEmpty( o ) ){ //如果o为空字符串则字段类型必须为string类型
							if( field.getType().equals( String.class ) ){
								update.set( field.getName(), o );
							}
						}else{
							if( field.getType().equals( ObjectId.class ) ){
								o = new ObjectId( o.toString() );
							}
							update.set( field.getName(), o );
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
		return update;
	}
	public <T> void dropCollection(Class<T> entityClass){
		this.mongoTemplate.dropCollection(entityClass);
	}

	public MongoOperations getMongoOperations() {
		return mongoOperations;
	}

	public void setMongoOperations(MongoOperations mongoOperations) {
		this.mongoOperations = mongoOperations;
	}
	
	public <T> String getCollectionName(Class<T> entityClass ) {
		return mongoTemplate.getCollectionName( entityClass.getClass() );
	}
	
	public Class<?> getCollection(String collectionName ) {
		DBCollection coll = mongoTemplate.getCollection( collectionName );
		Class<?> c = coll.getObjectClass();
		return c;
	}
	public <T> T findAndModify(Query query,Update update, Class<T> entityClass) {

		T t=mongoTemplate.findAndModify(query, update, entityClass);
		
		return t;
	}

    /**
     * 统计mongodb中某个字段的值
     * @author wk
     * @created 2017年2月16日 上午10:19:36
     * @param fieldName
     * @param collectionName
     * @return
     */
    public BigDecimal sum(String fieldName, String collectionName, Query query ){
        String mapFunction = "function(){emit( this._class, this."+ fieldName +")}";
        String reduceFunction = "function(key, values){var sum=0;values.forEach(function (v) {sum += parseFloat(v);});return String(sum);}";
        MapReduceResults<UnilifeMapReduceResult> results = mongoOperations.mapReduce(query, collectionName, mapFunction, reduceFunction, UnilifeMapReduceResult.class);

        Iterator<UnilifeMapReduceResult> resultItr = results.iterator();
        List<UnilifeMapReduceResult> resultList = new ArrayList<>();

        while ( resultItr.hasNext() ) {
            resultList.add( resultItr.next() );
        }

        if ( !CollectionUtils.isEmpty( resultList ) && resultList.size()==1 ) {
            BigDecimal sum = resultList.get(0).getValue();
            if ( sum != null ) {
                return sum.setScale( 2, BigDecimal.ROUND_HALF_UP );
            }
        }

        return BigDecimal.ZERO;
    }
    
    
    public BigDecimal sumField(String collection, String filedName, Criteria criteria) {
		double total = 0l;  
        String reduce = "function(doc, aggr){" +
                "            aggr.total += parseFloat((Math.round((doc." + filedName + ")*100)/100).toFixed(2));" +  
                "       }";  
        Query query = new Query();
        if(criteria!=null){
        	query.addCriteria(criteria);   	
        }
        DBObject result = mongoTemplate.getCollection(collection).group(null,   
                query.getQueryObject(),   
                new BasicDBObject("total", total),  
                reduce);  
          
        Map<String,BasicDBObject> map = result.toMap();
        if(map.size() > 0){  
            BasicDBObject bdbo = map.get("0");  
            if(bdbo != null && bdbo.get("total") != null)  
                total = bdbo.getDouble("total");  
        }  
        return BigDecimal.valueOf(total);
    } 
}
