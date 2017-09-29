package com.marcpoint.elengjing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;





public class TaggedTest {

	public static Map<Long,HashMap<Long, HashMap<String, List<String>>>> attrAllDic = null;
	TaggedTool taggedTool = new TaggedTool();
	
	
	public void process(Object[] args){
		// TODO Auto-generated method stub
		Long itemId = null;
		String message = null;
		try {
			if(!attrAllDic.isEmpty()){
				if (args.length == 4){
					if (null != args[0] && null != args[1]){
						itemId = Long.parseLong(args[0].toString());
						Long categoryID = Long.parseLong(args[1].toString());
						
						String itemName = null == args[2] ? "" : args[2].toString();
						String itemAttrDesc = null == args[3] ? "" : args[3].toString();
						HashMap<Long,HashMap<String, List<String>>> induMap = attrAllDic.get(16L);
						
						if (induMap.containsKey(categoryID)){
							HashMap<String, ArrayList<String>> parasItem = null;
							HashMap<String, List<String>> attrDic = induMap.get(categoryID);
							try {
								
								if(!"".equals(itemAttrDesc) || !"".equals(itemName)){
									if (!"".equals(itemAttrDesc)){//对itemAttrDesc进行打标签
										parasItem = taggedTool.parasDesc(itemId, itemAttrDesc, attrDic);
										
									}else if(!"".equals(itemName)) {//对itemName 进行打标签
										parasItem = taggedTool.parasItemName(itemId, itemName, attrDic);
										
									}
									if( null != parasItem && !parasItem.isEmpty()){
										Iterator<Entry<String, ArrayList<String>>> entrys = parasItem.entrySet().iterator();
										while(entrys.hasNext()){
											Entry<String, ArrayList<String>> entry = entrys.next();
											for(String attrValue:entry.getValue()){
												System.out.println("==itemId="+itemId+"; attrName="+entry.getKey()+"; attrValue=="+attrValue);
//												forward(new String[]{itemId.toString(), entry.getKey(), attrValue, null});
											}
										}
									}else{
										message = "paras is null";
									}
									
								}else{
									message = "itemAttrDesc is null and itemName is null";
								}
							} catch (Exception e) {
								String ss = itemId == null ? "":itemId.toString();
								StringWriter sw = new StringWriter();
								PrintWriter pw = new PrintWriter(sw);
								e.printStackTrace(pw);
//								forward(new String[]{ss, null, null, "unknow error=="+sw.toString()});
								System.out.println("==itemId="+ss+"; error="+sw.toString());
							}
							
						}else{
							message = "categoryID error ,categoryID not in attrDic ";
						}
					}else{
						message = "itemId or categoryID is null";
					}
					
				}else{
					message = "param is error,must have 4";
				}
			}else{
				message = "attrAllDic file is null";
			}
			
			if(null != message){
				if(null == itemId){
					System.out.println(message);
				}else{
					System.out.println(message);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			String ss = itemId == null ? "":itemId.toString();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			System.out.println("==itemId="+ss+"; error="+sw.toString());
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaggedTest testJava = new TaggedTest();
		// TODO Auto-generated method stub
		String itemId = "37504237976";
		String categoryId = "162404";
		String itemName = "2015春装新款女装明星同款真丝包花修身短款外套+九分裤休闲套装";
		String itemAttrDesc = "袖长:无袖&&服装版型:修身&&领型:V领&&袖型:其他&&成分含量:31%(含)-50%(含)&&面料:其他&&适用年龄:18-24周岁&&风格:通勤&&通勤:韩版&&年份季节:2016年夏季&&主要颜色:黑色 白色&&尺码:均码&&";
		itemAttrDesc = "";
//		itemName= "";
		String[] paramresource = new String[]{itemId,categoryId,itemName,itemAttrDesc};
		
		String configPath = "d:\\workspace\\udf\\source\\udf_config.properties";
		Properties udfConfig = TaggedTool.getUdfConfig(configPath);
   	 	String industryConfFilePath = udfConfig.getProperty("elengjing_industry_conf");
		String attrJsonStr = TaggedTool.getAttrDictString(industryConfFilePath);
		attrAllDic = TaggedTool.paraAttrDict(attrJsonStr);
		
		System.out.println("===============================begin==============================");
		if(!attrAllDic.isEmpty()){
			testJava.process(paramresource);
		}else{
			System.out.println("attrAllDic is empty!");
			
		}
	}
}

























