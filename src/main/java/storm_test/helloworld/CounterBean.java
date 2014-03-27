package storm_test.helloworld;

import java.io.Serializable;

public class CounterBean implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8423121924986537656L;

	/**
	 * 为了能够平滑的过渡到新的CounterBean的模型下，先预先保留goodsId字段，后期改造将会被移除。
	 * @deprecated
	 */
	private String goodsId;
	
	private String id;
	
	private String source;
	
	/**
	 * 时间
	 */
	private String predict;
	
	/**
	 * 类别，例如 goods(商品), ads(广告)等等
	 */
	private CounterCategory category;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}
	
	public String getPredict() {
		return predict;
	}

	public void setPredict(String predict) {
		this.predict = predict;
	}

	/**
	 * 默认值为{@link CounterCategory#Goods}
	 * @return
	 */
	public CounterCategory getCategory() {
		if(null == category)
			category = CounterCategory.Goods;
		return category;
	}

	public void setCategory(CounterCategory category) {
		this.category = category;
	}

	/**
	 * 
	 * @deprecated
	 * @return
	 */
	public String getGoodsId() {
		return goodsId;
	}

	/**
	 * 
	 * @deprecated
	 * @param goodsId
	 */
	public void setGoodsId(String goodsId) {
		this.goodsId = goodsId;
	}

	@Override
	public String toString(){
		return new StringBuilder("id:").append(id)
					.append(",source:").append(source)
					.append(",goodsId:").append(goodsId).toString();
	}
}
