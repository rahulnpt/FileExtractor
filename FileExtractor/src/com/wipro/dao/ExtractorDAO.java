package com.wipro.dao;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Repository;

import com.wipro.util.CommonUtilities;

import net.sf.json.JSONObject;

/**
 * @author rahul.gupta25
 *
 */
@Repository("extractorDAO")
@PropertySource(value={"classpath:query.properties"})
public class ExtractorDAO {
	
	@Autowired
	private Environment env;
	
	@Autowired
	private CommonUtilities commonUtilities;
	
	@Autowired
	private JdbcTemplate jdbcTemplate;
	
	private Properties props;
	
	ExtractorDAO() throws IOException{
		Properties props = new Properties();
		InputStream inStream = new FileInputStream("src/sqlServer.properties");
		props.load(inStream);
		this.props = props;
	}
	
	private static final Logger LOGGER = Logger.getLogger(ExtractorDAO.class);

	/**
	 * Check if table exist in DB.
	 *
	 * @param fileNames the file names
	 * @return the string[]
	 */
	public String[] checkIfTableExistInDB(String[] fileNames) {
		LOGGER.info("ExtractorDAO-checkIfTableExistInDB-STARTS");
		boolean isTableThere = false;
		String [] tablePresentInDb=new String[fileNames.length];
		int i=0;
		for(String fileName:fileNames){
			int tableCount = jdbcTemplate.queryForObject(env.getProperty("check.if.table.exist"),new Object[]{props.getProperty("jdbc.tableSchema"),fileName,props.getProperty("jdbc.databaseName")},Integer.class);
			if(tableCount > 0){
				tablePresentInDb[i++]=fileName;
				isTableThere = true;
			}
		}
		LOGGER.info("ExtractorDAO-checkIfTableExistInDB-ENDS");
		if(isTableThere){
			return tablePresentInDb;	
		}else{
			return null;
		}
	}
	
	/**
	 * Gets the std col name from master mapping table.
	 *
	 * @return the std col name from master mapping table
	 * @throws Exception the exception
	 */
	public Map<String,Integer> getStdColNameFromMasterMappingTable() throws Exception {
		LOGGER.info("ExtractorDAO-getStdColNameFromMasterMappingTable-STARTS");
		Map<String,Integer> metadataMap = null;
			metadataMap = jdbcTemplate.query(env.getProperty("master.file.mapping.table.data"),new ResultSetExtractor<Map<String,Integer>>(){
				@Override
				public Map extractData(ResultSet rs) throws SQLException,DataAccessException {
					Map<String,Integer> map = new HashMap<>();
					while(rs.next()){
						map.put(rs.getString("Standard_Column_Name").toLowerCase().trim(), rs.getInt("column_id"));
					}
					return map;
				}
			});
		LOGGER.info("ExtractorDAO-getStdColNameFromMasterMappingTable-ENDS");
		return metadataMap;
	}
	
	
	/**
	 * Gets the master mapping table data.
	 *
	 * @return the master mapping table data
	 * @throws Exception the exception
	 */
	public Map<String,String> getMasterMappingTableData() throws Exception {
		LOGGER.info("ExtractorDAO-getMappingTableData-STARTS");
		Map<String,String> mappingTableMap = null;
			mappingTableMap  = jdbcTemplate.query(env.getProperty("master.file.mapping.table.data"),new ResultSetExtractor<Map<String,String>>(){
				@Override
				public Map extractData(ResultSet rs) throws SQLException,DataAccessException {
					Map<String,String> map = new HashMap<>();
					while(rs.next()){
						map.put(rs.getString("Source_Sheet_column_name").toLowerCase().trim(), rs.getString("Standard_Column_Name").trim());
					}
					return map;
				}
				
			});
		LOGGER.info("ExtractorDAO-getMappingTableData-ENDS");
		return mappingTableMap ;
	}
	
	/**
	 * Delegate batch insert request.
	 *
	 * @param tablesPresentInDB the tables present in DB
	 * @param sortedFileMap the sorted file map
	 * @throws Exception the exception
	 */
	public void delegateBatchInsertRequest(String[] tablesPresentInDB,Map<String,List<String []>> sortedFileMap) throws Exception{
		LOGGER.info("ExtractorDAO-delegateBatchInsertRequest-STARTS");
		List<String> ExistingTableList = null;
		if(tablesPresentInDB != null){
			ExistingTableList = Arrays.asList(tablesPresentInDB);	
		}
		for(Object key : sortedFileMap.keySet()){
			String tableName=key.toString().split("#")[0];
			if(null != ExistingTableList && ExistingTableList.contains(tableName)){
				batchInsertIntoSQLServer(tableName,sortedFileMap.get(key));
			}else{
				createTableQuery(tableName,sortedFileMap.get(key));
				batchInsertIntoSQLServer(tableName,sortedFileMap.get(key));
			}
		}
		LOGGER.info("ExtractorDAO-delegateBatchInsertRequest-ENDS");
	}
	
	/**
	 * Creates the table query.
	 *
	 * @param tableName the table name
	 * @param fileArray the file array
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public void createTableQuery(String tableName,List<String[]> fileArray) throws Exception{
		LOGGER.info("ExtractorDAO-createTableQuery-STARTS");
		StringBuilder query = new StringBuilder(env.getProperty("create.new.table"));
		query.replace(query.indexOf("@@"), query.indexOf("@@")+2,tableName);
		Map<String,String> colNameMap =  getMasterMappingTableData();
		JSONObject result = getColumnDataType();
		String colNamesAndTypes = " (";
		String[] colList = fileArray.get(0);
		Map<String, String> dataTypeMap = (Map<String, String>) result.get("colNameDataTypeMap");
		Map<String, String> colNameConstraintMap = (Map<String, String>) result.get("colNameConstraintMap");
		for(int i=0;i<colList.length;i++){
			colNamesAndTypes += "["+colNameMap.get(colList[i].toLowerCase())+"] "+dataTypeMap.get(colList[i])+" "+colNameConstraintMap.get(colList[i].toLowerCase())+" ,";
		}
		colNamesAndTypes += "[Sentiment Rating] float, [Sentment Bucketing] NVARCHAR(255) )";
		query.append(colNamesAndTypes);
		jdbcTemplate.execute(query.toString());
		LOGGER.info("ExtractorDAO-createTableQuery-ENDS");
	}
	
	/**
	 * Batch insert from array list.
	 *
	 * @param fileName the file name
	 * @param fileData the file data
	 * @throws Exception the exception
	 */
	public void batchInsertFromArrayList(String fileName,final List<String[]> fileData) throws Exception{
		LOGGER.info("ExtractorDAO-batchInsertFromArrayList-STARTS");
		Map<String,String> colNameMap = getMasterMappingTableData();
		
		StringBuilder query = new StringBuilder(env.getProperty("insert.into.table"));
		query.replace(query.indexOf("@@"), query.indexOf("@@")+2,fileName);
		final String[] colNames= fileData.get(0);
		String colNamesForQuery = " (";
		String placeHoldersForParam = "(";
		String duplicateKeyParams = "";
		for(int i=0;i<colNames.length;i++){
			colNamesForQuery +=colNameMap.get(colNames[i].toLowerCase())+",";
			placeHoldersForParam += "?,";
			duplicateKeyParams +=colNameMap.get(colNames[i].toLowerCase())+" = ?,";
		}
		placeHoldersForParam = placeHoldersForParam.substring(0, placeHoldersForParam.length()-1)+")";
		colNamesForQuery = colNamesForQuery.substring(0, colNamesForQuery.length()-1)+")";
		duplicateKeyParams = duplicateKeyParams.substring(0, duplicateKeyParams.length()-1);
		query.replace(query.indexOf("##"), query.indexOf("##")+2,colNamesForQuery).append(placeHoldersForParam+" ON DUPLICATE KEY UPDATE "+duplicateKeyParams);
		fileData.remove(0);
		final Map<String, String> dataTypeMap = (Map<String, String>) getColumnDataType().get("colNameDataTypeMap");
		jdbcTemplate.batchUpdate(query.toString(),
				new BatchPreparedStatementSetter() {
			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				
				for(int j=0;j<colNames.length;j++){
					if(dataTypeMap.get(colNames[j]).toUpperCase().contains("VARCHAR")){
						ps.setString(j+1, fileData.get(i)[j]);
					}else if(dataTypeMap.get(colNames[j]).toUpperCase().contains("NUMERIC")){
						ps.setInt(j+1, Integer.parseInt(fileData.get(i)[j]));
					}else if(dataTypeMap.get(colNames[j]).toLowerCase().contains("date")){
						ps.setDate(j+1, new java.sql.Date(commonUtilities.parseDate(fileData.get(i)[j]).getTime()));
					}
				}
				int colCount=0;
				for(int j=colNames.length;j<2*colNames.length;j++){
					if(colCount<colNames.length){
						if(dataTypeMap.get(colNames[colCount]).toUpperCase().contains("VARCHAR")){
							ps.setString(j+1, fileData.get(i)[colCount]);
						}else if(dataTypeMap.get(colNames[colCount]).toUpperCase().contains("NUMERIC")){
							ps.setInt(j+1, Integer.parseInt(fileData.get(i)[colCount]));
						}else if(dataTypeMap.get(colNames[colCount]).toLowerCase().contains("date")){
							ps.setDate(j+1, new java.sql.Date(commonUtilities.parseDate(fileData.get(i)[colCount]).getTime()));
						}
					}
					colCount++;
				}
			}
			@Override
			public int getBatchSize() {
				return fileData.size();
			}
		});
		LOGGER.info("ExtractorDAO-batchInsertFromArrayList-ENDS");
	}
	
	public void batchInsertIntoSQLServer(String fileName,final List<String[]> fileData) throws Exception{
		LOGGER.info("ExtractorDAO-batchInsertIntoSQLServer-STARTS");
		final Map<String,String> colNameMap = getMasterMappingTableData();
		List<String> primaryColList = getPrimaryColNames();
		StringBuilder query = new StringBuilder(" UPDATE @@ SET ##");
		query.replace(query.indexOf("@@"), query.indexOf("@@")+2,fileName);
		final  String[] colNames= fileData.get(0);
		System.out.println(Arrays.toString(colNames));
		String colNamesForQuery = "";
		String placeHoldersForParam = "(";
		String duplicateKeyParams = "(";
		boolean primaryColFound = false;
		String primaryColName = "";
		int primaryColIndex = 0;
		for(int i=0;i<colNames.length;i++){
			colNamesForQuery +="["+colNameMap.get(colNames[i].toLowerCase())+"]=?,";
			placeHoldersForParam += "?,";
			duplicateKeyParams +="["+colNameMap.get(colNames[i].toLowerCase())+"] ,";
			if(!primaryColFound){
				if(primaryColList.contains(colNames[i].trim())){
					primaryColName = colNameMap.get(colNames[i].toLowerCase());
					primaryColIndex = i;
					primaryColFound = true;
				}
			}
		} 
		final String primeColName = primaryColName;
		final int primeColIndex = primaryColIndex;
		colNamesForQuery = colNamesForQuery + "[Sentiment Rating] = null, [Sentment Bucketing] = 'Not_Evaluated',";
		placeHoldersForParam = placeHoldersForParam +"NULL,'Not_Evaluated')";
		colNamesForQuery = colNamesForQuery.replace("["+primaryColName+"]=?,", "").replace("(,", "(");
		colNamesForQuery = colNamesForQuery.replace(",,", ",");
		colNamesForQuery = colNamesForQuery.replaceAll(",$", "");
		duplicateKeyParams = duplicateKeyParams+ "[Sentiment Rating] , [Sentment Bucketing])";
		query.replace(query.indexOf("##"), query.indexOf("##")+2,colNamesForQuery).append(" where ["+
		primaryColName+"]=? IF @@ROWCOUNT = 0 INSERT INTO "+ fileName+ " "+duplicateKeyParams+" values "+placeHoldersForParam);
		fileData.remove(0);
		final  String[] columnNames=colNames;
		@SuppressWarnings("unchecked")
		final Map<String, String> dataTypeMap = (Map<String, String>) getColumnDataType().get("colNameDataTypeMap");
		jdbcTemplate.batchUpdate(query.toString(),
				new BatchPreparedStatementSetter() {
			@Override
			public void setValues(PreparedStatement ps, int i) throws SQLException {
				int counter = 0;
				for(int j=0;j<columnNames.length;j++){
					if(dataTypeMap.get(columnNames[j]).toUpperCase().contains("VARCHAR") && !primeColName.equalsIgnoreCase(colNameMap.get(columnNames[j].toLowerCase()))){
						++counter;
						ps.setString(counter, fileData.get(i)[j]);
					}else if(dataTypeMap.get(columnNames[j]).toUpperCase().contains("FLOAT") && !primeColName.equalsIgnoreCase(colNameMap.get(columnNames[j].toLowerCase()))){
						++counter;
						if(!fileData.get(i)[j].isEmpty()){
							ps.setFloat(counter, Float.parseFloat(fileData.get(i)[j]));
						}else{
							ps.setObject(counter, null);
						}
						
					}else if(dataTypeMap.get(columnNames[j]).toLowerCase().contains("datetime") && !primeColName.equalsIgnoreCase(colNameMap.get(columnNames[j].toLowerCase()))){
						++counter;
						ps.setTimestamp(counter, (fileData.get(i)[j] == null || fileData.get(i)[j].trim().equals(""))?null:new java.sql.Timestamp(commonUtilities.parseDate(fileData.get(i)[j]).getTime()));
					}
				}
				if(dataTypeMap.get(columnNames[primeColIndex]).toUpperCase().contains("VARCHAR")){
					ps.setString(columnNames.length, fileData.get(i)[primeColIndex]);
				}else if(dataTypeMap.get(columnNames[primeColIndex]).toUpperCase().contains("FLOAT")){
					ps.setFloat(columnNames.length, Float.parseFloat(fileData.get(i)[primeColIndex]));
				}
				
				int colCount=0;
				for(int j=columnNames.length;j<2*columnNames.length;j++){
					if(colCount<columnNames.length){
						if(dataTypeMap.get(columnNames[colCount]).toUpperCase().contains("VARCHAR")){
							ps.setString(j+1, fileData.get(i)[colCount]);
						}else if(dataTypeMap.get(columnNames[colCount]).toUpperCase().contains("FLOAT")){
							if(!fileData.get(i)[colCount].isEmpty()){
								ps.setFloat(j+1, Float.parseFloat(fileData.get(i)[colCount]));
							}else{
								ps.setObject(j+1, null);
							}
						}else if(dataTypeMap.get(columnNames[colCount]).toLowerCase().contains("datetime")){
							ps.setTimestamp(j+1, (fileData.get(i)[colCount] == null || fileData.get(i)[colCount].trim().equals(""))?null:new java.sql.Timestamp(commonUtilities.parseDate(fileData.get(i)[colCount]).getTime()));
						}
					}
					colCount++;
				}
			}
			@Override
			public int getBatchSize() {
				return fileData.size();
			}
		});
		LOGGER.info("ExtractorDAO-batchInsertIntoSQLServer-ENDS");
	}
	
	private List<String> getPrimaryColNames() {
		List<String> primaryColList = jdbcTemplate.queryForList(env.getProperty("get.primary.col.names"),String.class);
		List <String> newCols = new ArrayList<>();
		for(String col:primaryColList){
			newCols.add(col.trim());
		}
		return newCols;
	}

	/**
	 * Gets the column data type.
	 *
	 * @return the column data type
	 * @throws Exception the exception
	 */
	public JSONObject getColumnDataType() throws Exception{
		LOGGER.info("ExtractorDAO-getColumnDataType-STARTS");
		JSONObject resultSet = jdbcTemplate.query(env.getProperty("master.file.mapping.table.data"), new ResultSetExtractor<JSONObject>(){
			@Override
			public JSONObject extractData(ResultSet rs)
					throws SQLException, DataAccessException {
				Map<String,String> colIdDataTypeMap = new HashMap<>();
				Map<String,String> colNameDataTypeMap = new HashMap<>();
				Map<String,String> colNameConstraintMap = new HashMap<>();
				JSONObject result = new JSONObject();
				while(rs.next()){
					colIdDataTypeMap.put(""+rs.getInt("column_id"), rs.getString("column_data_type").trim());
					colNameDataTypeMap.put(rs.getString("Source_Sheet_column_name").trim(), rs.getString("column_data_type").trim());
					colNameConstraintMap.put(rs.getString("Source_Sheet_column_name").toLowerCase().trim(), rs.getString("column_constraint") == null?"":rs.getString("column_constraint").trim());
				}
				result.put("colIdDataTypeMap", colIdDataTypeMap);
				result.put("colNameDataTypeMap", colNameDataTypeMap);
				result.put("colNameConstraintMap", colNameConstraintMap);
				return result;
			}});
		LOGGER.info("ExtractorDAO-getColumnDataType-STARTS");
		return resultSet;
	}
	
	/**
	 * Gets the col count.
	 *
	 * @return the col count
	 */
	public int getColCount(){
		return jdbcTemplate.queryForObject(env.getProperty("get.table.column.count"),Integer.class);
	}
	
}
