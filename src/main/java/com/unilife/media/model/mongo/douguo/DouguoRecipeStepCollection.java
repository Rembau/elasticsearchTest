package com.unilife.media.model.mongo.douguo;

import java.io.Serializable;

/**
 * 豆果网使用的菜谱制作步骤
 * @author wk
 * @created 2014年11月12日 下午7:03:09
 */
public class DouguoRecipeStepCollection implements Serializable {
	private int stepNum;  // 步骤排序
	private String desc; // 步骤描述
	private String stepImg; // 步骤图片
	
	
	public int getStepNum() {
		return stepNum;
	}
	public void setStepNum(int stepNum) {
		this.stepNum = stepNum;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public String getStepImg() {
		return stepImg;
	}
	public void setStepImg(String stepImg) {
		this.stepImg = stepImg;
	}
}
