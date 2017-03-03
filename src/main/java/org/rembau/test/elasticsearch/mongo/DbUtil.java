package org.rembau.test.elasticsearch.mongo;

import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 数据库工具类
 * @author: wk
 * @created: 2014年11月7日 上午10:08:08
 */
public class DbUtil {


	/**
	 * 将","分割的id字符串转换为ObjectId数组。</br>常用于批量操作。
	 * @author: wk
	 * @created: 2014年11月7日 上午10:08:04
	 * @param ids
	 * @return
	 */
	public final static List<ObjectId> idStrings2ObjectIdList(String ids){
		String[]idarray = ids.split(",");
		List<ObjectId> oids = new ArrayList<ObjectId>();
		for(String s: idarray){
			oids.add(new ObjectId(s));
		}
		return oids;
	}


	/**
	 * 从条件Map生成Query条件
	 * @author: wk
	 * @created: 2014年11月7日 上午10:09:11
	 * @param condMap
	 * @return
	 */
	public final static Query getMapQuery(Map<String, Object> condMap){
		Query q = new Query();
		if (condMap==null) return q;
		Iterator<Entry<String, Object>> iter = condMap.entrySet().iterator();
		while (iter.hasNext()) {
		    Entry<String, Object> entry = iter.next();
		    q.addCriteria(Criteria.where(entry.getKey()).is(entry.getValue()));
		}
		return q;
	}


	/**
	 * 将Map中的条件加入现有的Query
	 * @author: wk
	 * @created: 2014年11月7日 上午10:10:20
	 * @param condMap
	 * @param q
	 * @return
	 */
	public final static Query getMapQuery(Map<String, Object> condMap, Query q){
		Iterator<Entry<String, Object>> iter = condMap.entrySet().iterator();
		while (iter.hasNext()) {
		    Entry<String, Object> entry = iter.next();
		    q.addCriteria(Criteria.where(entry.getKey()).is(entry.getValue()));
		} 
		return q;
	}
	
	
}
