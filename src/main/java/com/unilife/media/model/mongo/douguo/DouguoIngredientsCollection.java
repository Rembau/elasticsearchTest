package com.unilife.media.model.mongo.douguo;

import java.io.Serializable;

/**
 * 豆果网使用的食材对象
 * @author wk
 * @created 2014年11月12日 下午6:56:49
 */
public class DouguoIngredientsCollection implements Serializable {
	private String name; //食材名称
	private String amount; // 食材的量
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAmount() {
		return amount;
	}
	public void setAmount(String amount) {
		this.amount = amount;
	}
}
