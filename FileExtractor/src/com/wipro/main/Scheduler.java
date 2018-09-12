package com.wipro.main;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.ftpserver.ftplet.FtpException;
import org.apache.log4j.Logger;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.support.AbstractApplicationContext;

import com.wipro.config.ApplicationConfig;
import com.wipro.service.ExtractorService;
import com.wipro.util.CommonUtilities;

/**
 * @author rahul.gupta25
 *
 */

public class Scheduler {
	
	private static CommonUtilities utilities;
	private static ExtractorService service;
	private static Properties appProperties;
	private static final Logger LOGGER = Logger.getLogger(Scheduler.class);
		
	public static void main(String[] args) {
		LOGGER.info("Scheduler-main-STARTED");
		AbstractApplicationContext context = null;
		try {
			System.out.println(System.getProperty("user.dir"));
			context = new AnnotationConfigApplicationContext(ApplicationConfig.class);
			service = (ExtractorService)context.getBean("extractorService");
			utilities = (CommonUtilities)context.getBean("commonUtilities");
			utilities.prepareSourceFolderForIncomingFiles();
			appProperties = new Properties();
			//InputStream inputStream = ClassLoader.class.getResourceAsStream(System.getProperty("user.dir")+"/application.properties");
			InputStream inStream = new FileInputStream("src/application.properties");
			appProperties.load(inStream);
			boolean isStarted= new FTPServer().startFTPServer(appProperties.getProperty("ftp.server.username"),
					appProperties.getProperty("ftp.server.password"), 
					appProperties.getProperty("source.path"));
			if(!isStarted){
				LOGGER.error("Could not start the FTP server");
				throw new Exception();
			}else{
				new Scheduler().startJob();	
			}
		} catch (FtpException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		LOGGER.info("Scheduler-main-ENDS");
	}
	
	public void startJob() throws Exception{
		LOGGER.info("Scheduler-startJob-STARTED");
		final Scheduler scheduler = new Scheduler();
		try {
			Timer timer = new Timer();
			timer.scheduleAtFixedRate(
			    new TimerTask()
			    {
			        public void run()
			        {	LOGGER.info("#####WAITING FOR FILES TO ARRIVE#####");
			        	if(scheduler.ifFilesArrived()){
			        		try {
								scheduler.runJob();
							} catch (Exception e) {
								e.printStackTrace();
							}
			        	}
			        }
			    },
			    0,
			    Long.parseLong(appProperties.getProperty("sleep.time"))); 
		}catch (Exception e) {
			e.printStackTrace();
	}
		LOGGER.info("Scheduler-startJob-ENDS");
	}
	
	public boolean ifFilesArrived(){
	boolean hasArrived = false;
		try {
			String[] fileList = utilities.checkFolderForFiles();
			if(fileList != null){
				LOGGER.info("New Files arrived");
				hasArrived = true;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return hasArrived;	
	}
	
	/**
	 * Run job.
	 * @throws Exception 
	 */
	public void runJob() throws Exception{
		LOGGER.info("Scheduler-runJob-STARTED");
		
		try{
			String[] fileList = utilities.checkFolderForFiles();
			String[] ReadyFileList=utilities.getFilesReadyToBeProcessed(fileList);
			if(fileList.length == ReadyFileList.length){
				LOGGER.info("All the files are ready to be processed");
			}else{
				LOGGER.error("Some files could not be processed and were moved to trash trashFiles folder");
			}
			LOGGER.info("File Processing STARTED");
			Map<String,List<String []>> fileMap = utilities.convertFilesToArrayList(ReadyFileList);
			Map<String,Integer> StandardColName = service.getStdColNameFromMasterMappingTable();
			Map<String,String> masterMappingTableData = service.getMasterMappingTableData();		
			
			Map<String,Map<Integer,Integer>> IndexMapStack = service.verifySourceColumnsInMappingTable(StandardColName,masterMappingTableData,fileMap);
			
			for(Map.Entry<String, Map<Integer,Integer>> entry:IndexMapStack.entrySet()){
				Map<Integer,Integer> sortedMap = service.sortByValue(entry.getValue());
				IndexMapStack.put(entry.getKey(), sortedMap);
			}
			
			Map<String,List<String []>> sortedFileMap = utilities.createSortedFile(fileMap, IndexMapStack);
			
			String[] newFileNames = utilities.removeFileExtensions(ReadyFileList);
			String[] tablesPresentInDB = service.checkIfTableExistInDB(newFileNames);
			service.delegateBatchInsertRequest(tablesPresentInDB,sortedFileMap);
			
			utilities.takeFileBackUp(ReadyFileList);
			
		}catch(Exception e){
			LOGGER.info("Exception caught::");
			e.printStackTrace();
			utilities.prepareSourceFolderForIncomingFiles();
		}
		LOGGER.info("Scheduler-runJob-ENDS");
	}
}
	
	
