package storm_test.helloworld;

/**
 * {@link CounterBean}中的分类
 * @author jacky
 *
 */
public enum CounterCategory {

	/**
	 * 商品
	 */
	Goods("goods"), 
	
	/**
	 * 广告
	 */
	Ads("ads");
	
	private final String value;
	
	CounterCategory(String value){
		this.value = value;
	}

	public String getValue() {
		return value;
	}
}
