package storm_test.helloworld;

import java.io.IOException;
import java.io.StringReader;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;


/**
 * 将Counter中相关的对象转换为JSON数据
 * @author jacky
 *
 */
public class BeanJsonConvert {

	private static ObjectMapper __mapper;
	
	private static ObjectMapper getMapper(){
		if(__mapper == null){
			__mapper = new ObjectMapper();
			__mapper.getSerializationConfig().setSerializationInclusion(Inclusion.NON_NULL);
		}
		return __mapper;
	}
	
	public static CounterBean toCounterBean(String json) throws JsonParseException, JsonMappingException, IOException{
		return getMapper().readValue(new StringReader(json), CounterBean.class);
	}
	
	public static String toJson(CounterBean bean) throws JsonGenerationException, JsonMappingException, IOException{
		return getMapper().writeValueAsString(bean);
	}
}
