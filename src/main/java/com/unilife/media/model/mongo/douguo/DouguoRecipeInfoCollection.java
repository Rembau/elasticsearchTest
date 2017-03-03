package com.unilife.media.model.mongo.douguo;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * 豆果菜谱详情
 * @author wk
 * @created 2014年11月12日 下午5:21:24
 */
public class DouguoRecipeInfoCollection implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6182808172619015947L;
	private String catalogId;
	private String catalogName;
	private int sortIndex;
	private String recipeId; // 菜谱id
	private String name; // 菜谱名称
	private String desc; // 菜谱描述/故事
	private String descImgUrl; // 菜谱描述图片
	private String dougouUrl; // 豆果网地址
	private String isCollAlready="n"; // 是否已经收藏
	private List<DouguoRecipeStepCollection> steps;  // 操作步骤
	private List<DouguoIngredientsCollection> zhuliao; // 主料列表
	private List<DouguoIngredientsCollection> fuliao; // 辅料列表
	
	private Date createDate=new Date();//创建日期
	
	private int total;
	private int fail;
	private boolean mainSuccess;
	
	public DouguoRecipeInfoCollection() {}
	
	
	public String getCatalogId() {
		return catalogId;
	}


	public void setCatalogId(String catalogId) {
		this.catalogId = catalogId;
	}


	public String getCatalogName() {
		return catalogName;
	}


	public void setCatalogName(String catalogName) {
		this.catalogName = catalogName;
	}

	public int getSortIndex() {
		return sortIndex;
	}


	public void setSortIndex(int sortIndex) {
		this.sortIndex = sortIndex;
	}


	public String getName() {
		return name;
	}
	public String getRecipeId() {
		return recipeId;
	}

	public void setRecipeId(String recipeId) {
		this.recipeId = recipeId;
	}

	public void setName(String name) {
		this.name = name;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public String getDescImgUrl() {
		return descImgUrl;
	}
	public void setDescImgUrl(String descImgUrl) {
		this.descImgUrl = descImgUrl;
	}
	public String getDougouUrl() {
		return dougouUrl;
	}
	public void setDougouUrl(String dougouUrl) {
		this.dougouUrl = dougouUrl;
	}
	public List<DouguoRecipeStepCollection> getSteps() {
		return steps;
	}
	public void setSteps(List<DouguoRecipeStepCollection> steps) {
		this.steps = steps;
	}
	public List<DouguoIngredientsCollection> getZhuliao() {
		return zhuliao;
	}
	public void setZhuliao(List<DouguoIngredientsCollection> zhuliao) {
		this.zhuliao = zhuliao;
	}
	public List<DouguoIngredientsCollection> getFuliao() {
		return fuliao;
	}
	public void setFuliao(List<DouguoIngredientsCollection> fuliao) {
		this.fuliao = fuliao;
	}
	public String getIsCollAlready() {
		return isCollAlready;
	}
	public void setIsCollAlready(String isCollAlready) {
		this.isCollAlready = isCollAlready;
	}


	public Date getCreateDate() {
		return createDate;
	}


	public void setCreateDate(Date createDate) {
		this.createDate = createDate;
	}


	public int getTotal() {
		return total;
	}


	public void setTotal(int total) {
		this.total = total;
	}


	public int getFail() {
		return fail;
	}


	public void setFail(int fail) {
		this.fail = fail;
	}


	public boolean isMainSuccess() {
		return mainSuccess;
	}


	public void setMainSuccess(boolean mainSuccess) {
		this.mainSuccess = mainSuccess;
	}


	


	
	
}
