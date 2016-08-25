package com.alibaba.middleware.unused;

import java.util.LinkedHashMap;
import java.util.Map;

public class FixedsizeLinkedHashMap extends LinkedHashMap<Long, Double> {

	private static final long serialVersionUID = -4177193127069010004L;
	private static int MAX_ENTRIES = 2670000;
	
	public FixedsizeLinkedHashMap(int initCapacity){
		super(initCapacity);
	}

	 /**
	  * 获得允许存放的最大容量
	  * @return int
	  */
	 public static int getMAX_ENTRIES() {
	  return MAX_ENTRIES;
	 }

	 /**
	  * 设置允许存放的最大容量
	  * @param int max_entries
	  */
	 public static void setMAX_ENTRIES(int max_entries) {
	  MAX_ENTRIES = max_entries;
	 }

	 /**
	  * 如果Map的尺寸大于设定的最大长度，返回true，再新加入对象时删除最老的对象
	  * @param Map.Entry eldest
	  * @return int
	  */
	 protected boolean removeEldestEntry(Map.Entry eldest) {
	        return size() > MAX_ENTRIES;
	     }
}
