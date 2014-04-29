package com.b5m.banx.model;

import java.io.Serializable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

@Entity
@Table(name = "t_keyword_record")
public class BanxModel implements Serializable {

	private static final long serialVersionUID = 4957366240023700306L;

	@Id
	@GeneratedValue
	private Integer id;

	@Column(name = "goods_id")
	private Integer goodsId;

	@Column(name = "keyword_id")
	private Integer keywordId;

	@Column(name = "keyword")
	private String keyword;

	@Column(name = "pv")
	private Integer pv;

	@Column(name = "click")
	private Integer click;

	@Column(name = "time_hour")
	private Long timeHour;

	@Column(name = "time_date")
	private Long timeDate;

	@Column(name = "create_time")
	private String createTime;

	@Column(name = "update_time")
	private String updateTime;

	@Column(name = "t_keyword_id")
	private String tableKeywordId;

	public Integer getId() {
		return id;
	}

	public Integer getGoodsId() {
		return goodsId;
	}

	public Integer getKeywordId() {
		return keywordId;
	}

	public String getKeyword() {
		return keyword;
	}

	public Integer getPv() {
		return pv;
	}

	public Integer getClick() {
		return click;
	}

	public Long getTimeHour() {
		return timeHour;
	}

	public Long getTimeDate() {
		return timeDate;
	}

	public String getCreateTime() {
		return createTime;
	}

	public String getUpdateTime() {
		return updateTime;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public void setGoodsId(Integer goodsId) {
		this.goodsId = goodsId;
	}

	public void setKeywordId(Integer keywordId) {
		this.keywordId = keywordId;
	}

	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}

	public void setPv(Integer pv) {
		this.pv = pv;
	}

	public void setClick(Integer click) {
		this.click = click;
	}

	public void setTimeHour(Long timeHour) {
		this.timeHour = timeHour;
	}

	public void setTimeDate(Long timeDate) {
		this.timeDate = timeDate;
	}

	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}

	public void setUpdateTime(String updateTime) {
		this.updateTime = updateTime;
	}

	public String getTableKeywordId() {
		return tableKeywordId;
	}

	public void setTableKeywordId(String tableKeywordId) {
		this.tableKeywordId = tableKeywordId;
	}

	public boolean equals(BanxModel obj) {
		if (this.goodsId.equals(obj.getGoodsId())
				&& this.keywordId.equals(obj.getKeywordId())
				&& this.timeHour.equals(obj.getTimeHour())) {
			return true;
		}
		return false;
	}

	public int hashCode() {
		return this.goodsId.hashCode() + this.keywordId.hashCode()
				+ this.timeHour.hashCode();
	}

	public String toString() {
		return this.goodsId + "_" + this.keyword + "_" + this.keywordId + "_"
				+ this.timeHour + "--pv--->" + this.pv + "---click--->"
				+ this.click + "---bizId-->" + this.tableKeywordId
				+ "----createTime--->" + this.createTime
				+ "-----updTime------>" + this.updateTime;
	}
}
