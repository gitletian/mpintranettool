package com.marcpoint.elengjing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map.Entry;

import org.json.JSONException;
import org.json.JSONObject;

public class TaggedTool {
	
	public static Properties getUdfConfig(String configPath){
		Properties udfConfig = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(configPath);
			udfConfig.load(is);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally{
			if (is != null){
				try {
					is.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return udfConfig;
	}
	
	public static String getAttrDictString(String filePath){
		String attrDictStr = null;
		File file = new File(filePath);
        Scanner scanner = null;
        StringBuilder buffer = new StringBuilder();
        try {
            scanner = new Scanner(file, "utf-8");
            while (scanner.hasNextLine()) {
                buffer.append(scanner.nextLine());
            }
            attrDictStr = buffer.toString();
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block  
//        	System.out.println("error"+e.getMessage());
        	e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return attrDictStr;
	}
	
	
	public static Map<Long,HashMap<Long, HashMap<String, List<String>>>> paraAttrDict(String attrDictStr){
		Map<Long,HashMap<Long, HashMap<String, List<String>>>> attrAllDic = new HashMap<Long,HashMap<Long, HashMap<String, List<String>>>>();
		if (null != attrDictStr){
			try {
				JSONObject industryIDjo = new JSONObject(attrDictStr);
				Iterator<String> iit = industryIDjo.keys();
				while(iit.hasNext()){
					String key = iit.next();
					JSONObject cjo = industryIDjo.getJSONObject(key);
					Iterator<String> cit = cjo.keys();
					HashMap<Long, HashMap<String, List<String>>> categoryMap = new HashMap<Long,HashMap<String, List<String>>>();
					while(cit.hasNext()){
						String ckey = cit.next();
						JSONObject djo = cjo.getJSONObject(ckey);
						Iterator<String> dit = djo.keys();
						HashMap<String, List<String>> attrMap = new HashMap<String,List<String>>();
						while(dit.hasNext()){
							String dkey = dit.next();
							String dvalue = djo.getString(dkey);
							
							attrMap.put(dkey, Arrays.asList(dvalue.split(",")));
						}
						categoryMap.put(Long.parseLong(ckey), attrMap);
					}
					attrAllDic.put(Long.parseLong(key), categoryMap);
				}
				
			} catch (JSONException je) {
				// TODO: handle exception
//				System.out.println("parsing error. error message is ="+je.getMessage());
				je.printStackTrace();
			}
		}
		return attrAllDic;
	}
	
	
	public ArrayList<String> parasValue(List<String> attrAllvalueList, String attrName, String attrValue){
		String[]  specialAttr = new String[]{"流行元素", "流行元素/工艺", "图案", "图案文化", "中老年女装图案", "里料图案", "工艺", "制作工艺","服饰工艺", "服装款式细节", "颜色分类", "主要颜色"};
		ArrayList<String> attrValues = new ArrayList<String>();
		if ("材质成分".equals(attrName)){
			attrValues.add(attrName);
		}else if (Arrays.asList(specialAttr).contains(attrName)){
			for(String attr: attrAllvalueList){
				if(!"".equals(attr) && attrValue.indexOf(attr)>-1){
					attrValues.add(attr);
				}
			}
		}else{
			for(String attr: attrAllvalueList){
				if(!"".equals(attr) && attr.equals(attrValue)){
					attrValues.add(attr);
				}
			}
		}
		return attrValues;
	}
	
	
	public HashMap<String, ArrayList<String>> parasDesc(Long itemId, String itemAttrDesc, HashMap<String, List<String>> attrDic){
		HashMap<String, ArrayList<String>> parasItem = new HashMap<String, ArrayList<String>>();
		String[] attrLists = itemAttrDesc.split(";");
		for(String attr:attrLists){
			String[] attrList = attr.split(":");
			if (attrList.length > 1){
				String attrName = attrList[0];
				String attrValue = attrList[1];
				if (attrValue.length()<=512){
					if(attrDic.containsKey(attrName)){
						List<String> attrAllvalueList = attrDic.get(attrName);
						ArrayList<String> attrValues = parasValue(attrAllvalueList, attrName, attrValue);
						
						if(!attrValues.isEmpty()){
							if(parasItem.keySet().contains(attrName)){
								parasItem.get(attrName).addAll(attrValues);
							}else{
								parasItem.put(attrName, attrValues);
							}
						}
					}
				}
			}
		}
		return parasItem;
	}
	
	
	public HashMap<String, ArrayList<String>> parasItemName(Long itemId, String itemName, HashMap<String, List<String>> attrDic){
		HashMap<String, ArrayList<String>> parasItem = new HashMap<String, ArrayList<String>>();
		Iterator<Entry<String, List<String>>> entries = attrDic.entrySet().iterator();
		while(entries.hasNext()){
			Entry<String, List<String>> entrie = entries.next();
			List<String> attrList = entrie.getValue();
			for(String attr:attrList){
				if(!attr.equals("") && itemName.indexOf(attr) > -1){
					String key = entrie.getKey();
					if(parasItem.keySet().contains(key)){
						parasItem.get(key).add(attr);
					}else{
						ArrayList<String> attrValues = new ArrayList<String>();
						attrValues.add(attr);
						parasItem.put(key, attrValues);
					}
					itemName = itemName.replace(attr, "");
				}
			}
		}
		return parasItem;
		
	}
	
	
	


}
