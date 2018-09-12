package com.wipro.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.monitorjbl.xlsx.StreamingReader;

import de.siegmar.fastcsv.reader.CsvContainer;
import de.siegmar.fastcsv.reader.CsvReader;

/**
 * @author rahul.gupta25
 *
 */
@Service("commonUtilities")
//@PropertySource(value={"classpath:application.properties"})
public class CommonUtilities {
	
	/*@Autowired
	private Environment env;*/
	
	private Properties env;
	
	private static final Logger LOGGER = Logger.getLogger(CommonUtilities.class);
	
	CommonUtilities() throws IOException{
		Properties props = new Properties();
		InputStream inStream = new FileInputStream("src/application.properties");
		props.load(inStream);
		this.env = props;
	}
	/**
	 * Check folder for files.
	 *
	 * @return the string[]
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String[] checkFolderForFiles() throws IOException{
		String filePath = env.getProperty("source.path");
		File file = new File(filePath);
		String [] fileNames = null;
		if(file.isDirectory()){
			fileNames = file.list();
		}else{
			LOGGER.info("Source folder not found, creating one now");
			FileUtils.forceMkdir(file);
		}
		if(fileNames.length > 0){
			return fileNames;
		}else{
			return null;
		}
	}
	
	/**
	 * Prepare source folder for incoming files.
	 *
	 * @param properties the properties
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public void prepareSourceFolderForIncomingFiles() throws Exception {
		LOGGER.info("FileWatcher-prepareSourceFolderForIncomingFiles-STARTS");
		String sourceFolder = env.getProperty("source.path");
		File projectPath = new File(sourceFolder);
		if(projectPath.exists()){
			File[] fileList=projectPath.listFiles();
			if(fileList.length>0){
				String unprocessedFolderPath = System.getProperty("user.dir")+"/unprocessedFiles";
				File unprocessedFolder = new File(unprocessedFolderPath);
				if(!unprocessedFolder.exists()){
					FileUtils.forceMkdir(unprocessedFolder);
				}
				List<File> fileArrayList = Arrays.asList(fileList);
				for(int i=0;i<fileList.length;i++){
					String fileName;
					if(!fileList[i].getAbsoluteFile().toString().contains(".rdy")){
						fileName = fileList[i].getAbsoluteFile().toString()+".rdy";
						if(!fileArrayList.contains(new File(fileName)) && fileArrayList.contains(fileList[i].getAbsoluteFile()) ){
							FileUtils.moveFile(fileList[i],new File(unprocessedFolderPath+"/"+fileList[i].getName()));
						}
					}else{
						fileName = fileList[i].getAbsoluteFile().toString().replace(".rdy", "");
						if(fileArrayList.contains(fileList[i]) && !fileArrayList.contains(new File(fileName)) ){
							FileUtils.moveFileToDirectory(fileList[i],new File(System.getProperty("user.dir")+"/trashFiles"), true);
						}
					}
				}
			}
		}
		LOGGER.info("FileWatcher-prepareSourceFolderForIncomingFiles-ENDS");
	}
	
	
	/**
	 * Gets the files ready to be processed.
	 *
	 * @param fileList the file list
	 * @return the files ready to be processed
	 * @throws InterruptedException the interrupted exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String[] getFilesReadyToBeProcessed(String[] fileList) throws InterruptedException, IOException {
		LOGGER.info("CommonUtilities-getFilesReadyToBeProcessed-STARTS");
		List<String> existingFilesList = new ArrayList<>(); 
		for(String fileName:fileList){
			if(!fileName.contains(".rdy")){
				File markerFile = new File(env.getProperty("source.path")+"/"+fileName+".rdy");
				while(true){
					if(markerFile.exists() && markerFile.length() == 0){
						if(new File(env.getProperty("source.path")+"/"+fileName).exists()){
							boolean isDeleted = markerFile.delete();
							if(isDeleted == true){
								LOGGER.info("marker file deleted successfully");
							}else{
								LOGGER.error("Could not delete marker file");
							}
						}else{
							LOGGER.error("marker file exists but source file not found");
							FileUtils.moveFileToDirectory(markerFile, new File(System.getProperty("user.dir")+"/trashFiles"), true);
						}
						break;
					}else{
						Thread.sleep(200);
					}
				}
			}else{
				existingFilesList.add(fileName.replace(".rdy", ""));
			}
		}
		for(int i =0;i<existingFilesList.size();i++){
			System.out.println(existingFilesList.get(i));
		}
		LOGGER.info("CommonUtilities-getFilesReadyToBeProcessed-ENDS");
		return existingFilesList.toArray(new String[existingFilesList.size()]);
	}
	
	/**
	 * Convert files to array list.
	 *
	 * @param fileNames the file names
	 * @return the map
	 * @throws IOException Signals that an I/O exception has occurred.
	 * @throws EncryptedDocumentException the encrypted document exception
	 * @throws InvalidFormatException the invalid format exception
	 */
	public Map<String,List<String[]>> convertFilesToArrayList(String[] fileNames) throws IOException, EncryptedDocumentException, InvalidFormatException {
		LOGGER.info("CommonUtilities-convertFilesToArrayList-STARTS");
		CsvReader reader = null;
		Map<String,List<String[]>> fileListMap = new HashMap<>();
		List<String[]> fileContent = new ArrayList<>();
		for(String fileName:fileNames){
			String filePath = env.getProperty("source.path")+"/"+fileName;
			if(FilenameUtils.getExtension(fileName).equalsIgnoreCase("csv")){
				reader = new CsvReader();
				CsvContainer csvContainer = reader.read(new File(filePath),StandardCharsets.UTF_8);
				for(int i=0; i<csvContainer.getRows().size();i++){
					String[] currLine = new String[csvContainer.getRow(i).getFields().size()];
					for(int j=0; j<csvContainer.getRow(i).getFields().size();j++){
						currLine[j]=csvContainer.getRow(i).getField(j);
					}
					fileContent.add(currLine);
				}
				if(!fileContent.isEmpty()){
					fileListMap.put(fileName,fileContent);
				}
			}else if(FilenameUtils.getExtension(fileName).equalsIgnoreCase("xls") || FilenameUtils.getExtension(fileName).equalsIgnoreCase("xlsx")){
				List<List<String[]>> workBookData = readExcelFile(filePath);
				List<String[]> mergedSheets = new ArrayList<>();
				for(int i=0;i<workBookData.size();i++){
					mergedSheets.addAll(workBookData.get(i));
				}
				fileListMap.put(fileName,mergedSheets);
			}
		}
		LOGGER.info("CommonUtilities-convertFilesToArrayList-ENDS");
		return fileListMap;
	}
	/**
	 * Read excel file.
	 *
	 * @param filePath the file path
	 * @return the list
	 * @throws EncryptedDocumentException the encrypted document exception
	 * @throws InvalidFormatException the invalid format exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	private List<List<String[]>> readExcelFile(String filePath) throws EncryptedDocumentException, InvalidFormatException, IOException {
		LOGGER.info("CommonUtilities-readExcelFile-STARTS");
		File workBookFile = new File(filePath);
		FileInputStream fileinputStream = new FileInputStream(workBookFile);
		Workbook workbook = null;
		if(filePath.endsWith("xlsx")){
			workbook = StreamingReader.builder()
					.rowCacheSize(100)
					.bufferSize(4096)
					.open(fileinputStream);
		}else if(filePath.endsWith("xls")){
			workbook = WorkbookFactory.create(fileinputStream);
		}
		List<List<String[]>> workBookData=new ArrayList<>();
		for(int i=0;i<workbook.getNumberOfSheets();i++){
			Sheet sheet = workbook.getSheetAt(i);
			if(i != 0){
				sheet.removeRow(sheet.getRow(sheet.getFirstRowNum()));
			}
			List<String[]> sheetData=new ArrayList<>();
			Iterator<Row> sheetIterator =  sheet.iterator();
			while(sheetIterator.hasNext()){
				Row row = sheetIterator.next();
				Iterator<Cell> cellIterator = row.cellIterator();
				String[] rowData = new String[row.getLastCellNum()];
				int cellCount =0;
				while(cellIterator.hasNext()){
					cellIterator.next();
					rowData[cellCount] = new DataFormatter().formatCellValue(row.getCell(cellCount));
					cellCount++;
				}
			sheetData.add(rowData);}
		workBookData.add(sheetData);
		}
		fileinputStream.close();
		workbook.close();
		LOGGER.info("CommonUtilities-readExcelFile-ENDS");
		return workBookData;
	}
	
	/**
	 * Read excel file.
	 *
	 * @param filePath the file path
	 * @return the list
	 * @throws EncryptedDocumentException the encrypted document exception
	 * @throws InvalidFormatException the invalid format exception
	 * @throws IOException Signals that an I/O exception has occurred.
	 *//*
	private List<List<String[]>> readExcelFile(String filePath) throws EncryptedDocumentException, InvalidFormatException, IOException {
		LOGGER.info("CommonUtilities-readExcelFile-STARTS");
		File workBookFile = new File(filePath);
		FileInputStream fileinputStream = new FileInputStream(workBookFile);
		Workbook workbook = WorkbookFactory.create(fileinputStream);
		List<List<String[]>> workBookData=new ArrayList<>();
		for(int i=0;i<workbook.getNumberOfSheets();i++){
			Sheet sheet = workbook.getSheetAt(i);
			if(i != 0){
				sheet.removeRow(sheet.getRow(sheet.getFirstRowNum()));
			}
			List<String[]> sheetData=new ArrayList<>();
			Iterator<Row> sheetIterator =  sheet.iterator();
			
			while(sheetIterator.hasNext()){Row row = sheetIterator.next();
			Iterator<Cell> cellIterator = row.cellIterator();
			String[] rowData = new String[row.getPhysicalNumberOfCells()];
			int cellCount =0;
			while(cellIterator.hasNext()){
				cellIterator.next();
				String cellValue = "";
				cellValue = new DataFormatter().formatCellValue(row.getCell(cellCount));
				rowData[cellCount] = cellValue;
				cellCount++;
			}
			sheetData.add(rowData);}
			workBookData.add(sheetData);
		}
		fileinputStream.close();
		LOGGER.info("CommonUtilities-readExcelFile-ENDS");
		return workBookData;
	}*/

	/**
	 * Creates the sorted file.
	 *
	 * @param fileMap the file map
	 * @param IndexMapStack the index map stack
	 * @return the map
	 */
	public Map<String,List<String []>> createSortedFile(Map<String,List<String []>> fileMap, Map<String,Map<Integer,Integer>> IndexMapStack){
		LOGGER.info("CommonUtilities-createSortedFile-STARTED");
		List<String[]> sortedFileArray;
		for(Map.Entry<String, List<String[]>> entry:fileMap.entrySet()){
			sortedFileArray = new ArrayList<>();
			Map<Integer,Integer> reversedIndexMap = reverseMap(IndexMapStack.get(entry.getKey()));
			for(int i=0;i<entry.getValue().size();i++){
				String[] currentLine = entry.getValue().get(i);
				List<String> sortedLine = new ArrayList<>();
				String[] finalSortedLine = null;
				for(Map.Entry<Integer,Integer> indexEntry:reversedIndexMap.entrySet()){
					Integer val = reversedIndexMap.get(indexEntry.getKey());
					if(val != null){
						sortedLine.add(currentLine[reversedIndexMap.get(indexEntry.getKey())]);
					}
				}
				finalSortedLine = new String[sortedLine.size()];
				sortedFileArray.add(sortedLine.toArray(finalSortedLine));
			}
			fileMap.put(entry.getKey(), sortedFileArray);
		}
		LOGGER.info("CommonUtilities-createSortedFile-ENDS");
		return fileMap;
	}
	
	/**
	 * Reverse map.
	 *
	 * @param map the map
	 * @return the map
	 */
	public Map<Integer,Integer> reverseMap(Map<Integer,Integer> map){
		LOGGER.info("CommonUtilities-reverseMap-STARTED");
		Map<Integer,Integer> reversedMap = new LinkedHashMap<Integer, Integer>();
		for(Map.Entry<Integer,Integer> entry:map.entrySet()){
			reversedMap.put(entry.getValue(),entry.getKey());
		}
		LOGGER.info("CommonUtilities-reverseMap-ENDS");
		return reversedMap;
	}
	
	/**
	 * Removes the file extensions.
	 *
	 * @param fileNames the file names
	 * @return the string[]
	 */
	public String[] removeFileExtensions(String[] fileNames){
		LOGGER.info("CommonUtilities-removeFileExtensions-STARTED");
		String[] tableNames = new String[fileNames.length];
		for(int i=0;i<fileNames.length;i++){
			tableNames[i]=fileNames[i].split("#")[0];
			LOGGER.info("fileName modified "+tableNames[i]);
		}
		LOGGER.info("CommonUtilities-removeFileExtensions-ENDS");
		return tableNames;
	}
	
	/**
	 * Parses the date.
	 *
	 * @param date the date
	 * @return the java.util. date
	 */
	public java.util.Date parseDate(String date) {
		LOGGER.info("CommonUtilities-parseDate-STARTED");
		Date parsedDate=null;
		if (date != null) { 
			String dateFormats = env.getProperty("date.formats");
			String[] formats = dateFormats.split("\\|");
            for (String format : formats) {
            	if(!format.isEmpty()){
            		DateFormat sdf = new SimpleDateFormat(format,Locale.ENGLISH);
            		sdf.setLenient(false); 
            		try {
            			parsedDate = sdf.parse(date);
            			LOGGER.info("parsed date is "+parsedDate);
            			break;
            		} catch (ParseException e) {
            			LOGGER.error("Exception occured while parsing date");
            			LOGGER.info("parsing for "+format);
                		LOGGER.info("input date is "+date);
            		}
            	}
            }
        }
		LOGGER.info("CommonUtilities-parseDate-ENDS");
        return parsedDate;
	}
	
	/**
	 * Take file back up.
	 *
	 * @param fileList the file list
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void takeFileBackUp(String[] fileList) throws IOException {
		LOGGER.info("CommonUtilities-takeFileBackUp-STARTED");
		String sourcePath = env.getProperty("source.path");
		String destinationPath = env.getProperty("backup.path");
		
		File destinationFolder = new File(destinationPath);
		if (!destinationFolder.exists()) {
			FileUtils.forceMkdir(new File(destinationPath));
		}
		for(String fileName:fileList){
			File sourceFile = new File(sourcePath+"/"+fileName);
			FileUtils.moveFile(sourceFile,new File(destinationFolder+"/"+fileName));
		}
		LOGGER.info("CommonUtilities-takeFileBackUp-ENDS");
	}
}
