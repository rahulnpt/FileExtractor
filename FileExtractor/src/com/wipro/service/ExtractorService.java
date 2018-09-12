package com.wipro.service;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.wipro.dao.ExtractorDAO;

/**
 * @author rahul.gupta25
 *
 */
@Service("extractorService")
public class ExtractorService {
	
	@Autowired
	private ExtractorDAO extractorDAO;
	
	private static final Logger LOGGER = Logger.getLogger(ExtractorService.class);
	
	/**
	 * Gets the std col name from master mapping table.
	 *
	 * @return the std col name from master mapping table
	 * @throws Exception the exception
	 */
	public Map<String,Integer> getStdColNameFromMasterMappingTable() throws Exception{
		LOGGER.info("ExtractorService-getStdColNameFromMasterMappingTable-STARTS");
		Map<String,Integer> map = extractorDAO.getStdColNameFromMasterMappingTable();
		LOGGER.info("ExtractorService-getStdColNameFromMasterMappingTable-ENDS");
		return map;
	}
	
	/**
	 * Gets the master mapping table data.
	 *
	 * @return the master mapping table data
	 * @throws Exception the exception
	 */
	public Map<String,String> getMasterMappingTableData() throws Exception{
		LOGGER.info("ExtractorService-getMasterMappingTableData-STARTS");
		Map<String,String> map = extractorDAO.getMasterMappingTableData();
		LOGGER.info("ExtractorService-getMasterMappingTableData-ENDS");
		return map;
	}
	
	/**
	 * Sort by value.
	 *
	 * @param <K> the key type
	 * @param <V> the value type
	 * @param map the map
	 * @return the map
	 * @throws Exception the exception
	 */
	public  <K, V> Map<K, V> sortByValue(Map<K, V> map) throws Exception {
		LOGGER.info("ExtractorService-sortByValue-STARTS");
	    List<Entry<K, V>> list = new LinkedList<>(map.entrySet());
	    Collections.sort(list, new Comparator<Object>() {
	        @SuppressWarnings("unchecked")
	        public int compare(Object o1, Object o2) {
	            return ((Comparable<V>) ((Map.Entry<K, V>) (o1)).getValue()).compareTo(((Map.Entry<K, V>) (o2)).getValue());
	        }
	    });

	    Map<K, V> result = new LinkedHashMap<>();
	    for (Iterator<Entry<K, V>> it = list.iterator(); it.hasNext();) {
	        Map.Entry<K, V> entry = (Map.Entry<K, V>) it.next();
	        result.put(entry.getKey(), entry.getValue());
	    }
	    LOGGER.info("ExtractorService-sortByValue-ENDS");
	    return result;
	}
	
	/**
	 * Verify source columns in mapping table.
	 *
	 * @param StandardColName the standard col name
	 * @param masterMappingTableData the master mapping table data
	 * @param fileMap the file map
	 * @return the map
	 * @throws Exception the exception
	 */
	public Map<String,Map<Integer,Integer>> verifySourceColumnsInMappingTable(Map<String,Integer>
	StandardColName,Map<String,String> masterMappingTableData,
			Map<String,List<String []>> fileMap) throws Exception{
		LOGGER.info("ExtractorService-verifySourceColumnsInMappingTable-STARTS");
		 Map<String,Map<Integer,Integer>> indexMapStack = new HashMap<>();	
		for(Map.Entry<String, List<String[]>> entry:fileMap.entrySet()){
			String[] firstLine = entry.getValue().get(0);
			Map<Integer,Integer> indexMap = new HashMap<>();
			for(int i=0;i<firstLine.length;i++){
				if(masterMappingTableData.containsKey(firstLine[i].toLowerCase())){
					indexMap.put(i, StandardColName.get((masterMappingTableData.get((firstLine[i]).toLowerCase())).toLowerCase()));
				}else{
					LOGGER.info("Source file columns not found in mapping table");
				}
			}
			indexMapStack.put(entry.getKey(), indexMap);
		}
		LOGGER.info("ExtractorService-verifySourceColumnsInMappingTable-ENDS");
		return indexMapStack;
	}
	
	/**
	 * Delegate batch insert request.
	 *
	 * @param tablesPresentInDB the tables present in DB
	 * @param sortedFileMap the sorted file map
	 * @throws Exception the exception
	 */
	public void delegateBatchInsertRequest(String[] tablesPresentInDB,Map<String,List<String []>> sortedFileMap) throws Exception{
		LOGGER.info("ExtractorService-delegateBatchInsertRequest-STARTS");
		extractorDAO.delegateBatchInsertRequest(tablesPresentInDB,sortedFileMap);
		LOGGER.info("ExtractorService-delegateBatchInsertRequest-ENDS");
	}
	
	
	/**
	 * Check if table exist in DB.
	 *
	 * @param fileNames the file names
	 * @return the string[]
	 * @throws Exception the exception
	 */
	public String[] checkIfTableExistInDB(String[] fileNames) throws Exception {
		LOGGER.info("ExtractorService-checkIfTableExistInDB-STARTS");
		String [] tablePresentInDB = extractorDAO.checkIfTableExistInDB(fileNames);
		LOGGER.info("ExtractorService-checkIfTableExistInDB-ENDS");
		return tablePresentInDB;
	}
}
