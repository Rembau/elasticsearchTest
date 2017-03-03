package com.unilife.media.model.mongo.douguo;

/**
 * 豆果网的菜单目录
 * @author: wk
 * @created: 2014年11月11日 下午3:21:43
 */
public class DouguoRecipeCatalogCollection {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String catalogId;  // 菜单id
	private String name;  // 菜单名称
	private String parentId;  // 父级菜单id
	
		
	public String getCatalogId() {
		return catalogId;
	}
	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getParentId() {
		return parentId;
	}
	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
	
}
