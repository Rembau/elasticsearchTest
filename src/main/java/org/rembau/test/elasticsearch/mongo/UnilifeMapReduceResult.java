package org.rembau.test.elasticsearch.mongo;

import java.math.BigDecimal;

public class UnilifeMapReduceResult {
	private String _id;
	private BigDecimal value;
	
	public BigDecimal getValue() {
		return value;
	}
	public void setValue(BigDecimal value) {
		this.value = value;
	}
	public String get_id() {
		return _id;
	}
	public void set_id(String _id) {
		this._id = _id;
	}
}
