package com.wipro.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * @author rahul.gupta25
 *
 */
public class FileWatcher {

	private static final Logger _LOGGER = Logger.getLogger(FileWatcher.class);

	static Properties properties;
	
	private static JSONObject targetMappingJson;
	
	public static void main(String[] args){
		_LOGGER.info("FileWatcher-Main-STARTS");
		try {
			properties = new Properties();
			//InputStream inStream = ClassLoader.class.getResourceAsStream("/application.properties");
			InputStream inStream = new FileInputStream(System.getProperty("user.dir")+"/src/application.properties");
			properties.load(inStream);
			
			String targetTableMapping = properties.getProperty("target.table.mapping");
			JSONParser parser = new JSONParser();
			targetMappingJson = (JSONObject)parser.parse(targetTableMapping);
			
			final String sourcePath = properties.getProperty("source.path");
			final File sourceDir = new File(sourcePath);
			FileWatcher fileWatcher= new FileWatcher();
			if(fileWatcher.prepareSourceFolderForWatchService()){
				_LOGGER.info("Source folder prepared");
				new FileWatcher().watchDirectoryPath(sourceDir.toPath());
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		_LOGGER.info("FileWatcher-Main-END");
	}

	/**
	 * Watch directory path.
	 *
	 * @param path the path
	 * @throws ParseException the parse exception
	 */
	@SuppressWarnings("unchecked")
	public void watchDirectoryPath(Path path) throws ParseException {
		_LOGGER.info("FileWatcher-watchDirectoryPath-STARTS");
		try {
			boolean isFolder = (boolean) Files.getAttribute(path,
					"basic:isDirectory", LinkOption.NOFOLLOW_LINKS);
			if (!isFolder) {
				throw new IllegalArgumentException("Path: " + path
						+ " is not a directory");
			}
			_LOGGER.info("Watching path: " + path);
			FileSystem fileSystem = path.getFileSystem();
			try (WatchService service = fileSystem.newWatchService()) {
				path.register(service, StandardWatchEventKinds.ENTRY_CREATE);
				WatchKey key = null;
				while (true) {
					key = service.take();
					Kind<?> kind = null;
					for (WatchEvent<?> watchEvent : key.pollEvents()) {
						kind = watchEvent.kind();
						if (StandardWatchEventKinds.OVERFLOW == kind) {
							_LOGGER.info("Overflow might have occured");
							continue; // loop
						} else if (StandardWatchEventKinds.ENTRY_CREATE == kind) {
							Path newPath = ((WatchEvent<Path>) watchEvent)
									.context();
							if(!checkIfFileCopyingIsInProcess(newPath)){
								checkForSpecifiedFileName(newPath);
								if(allSourceFileDone()){
									if(anyFilePresentInTempFolder()){
										moveFileFromTempToSource();
									}	
								}
							}
						}
					}
					if (!key.reset()) {
						break;
					}
				}	
			} 
		} catch (InterruptedException ie) {
			ie.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	/**
	 * Prepare source folder for watch service.
	 *
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean prepareSourceFolderForWatchService() throws Exception {
		_LOGGER.info("FileWatcher-prepareSourceFolderForWatchService-STARTS");
		boolean isPrepared = false;
		String sourceFolder = properties.getProperty("source.path");
		File projectPath = new File(sourceFolder);
		if(projectPath.exists()){
			_LOGGER.info("source folder exists");
			File[] fileList=projectPath.listFiles();
			_LOGGER.info(fileList.length);
			if(fileList.length>0){
				String tempFolderPath = System.getProperty("user.dir")+"/tempFolder";
				_LOGGER.info(System.getProperty("user.dir"));
				File tempFolder = new File(tempFolderPath);
				if(!tempFolder.exists()){
					FileUtils.forceMkdir(tempFolder);
					_LOGGER.info("temp folder created successfully, path is "+tempFolder.getAbsolutePath());
				}
				for(int i=0;i<fileList.length;i++){
					FileUtils.moveFile(fileList[i],new File(tempFolderPath+"/"+fileList[i].getName()+"##"+i));
				}
				if(tempFolder.listFiles().length>0){
					isPrepared = true;
				}else{
					isPrepared = false;
					_LOGGER.info("Facing issue in moving the file to temp folder");
				}
			}else{
				isPrepared = true;
			}
		}else{
			isPrepared = true;
		}
		_LOGGER.info("FileWatcher-prepareSourceFolderForWatchService-ENDS");
		return isPrepared;
	}
	
	
	/**
	 * Check if all source files processed.
	 *
	 * @return true, if source files are processed
	 */
	public boolean allSourceFileDone () {
		_LOGGER.info("FileWatcher-allSourceFileDone-STARTS");
		String sourcePath = properties.getProperty("source.path");
		File file = new File(sourcePath);
		boolean isDone = false;
		if(file.listFiles().length==0){
			isDone = true;
		}
		_LOGGER.info("FileWatcher-allSourceFileDone-ENDS");
		return isDone;
	}
	
	
	/**
	 * Check if file copying is in process.
	 *
	 * @param filePath the file path
	 * @return true, if successful
	 * @throws InterruptedException the interrupted exception
	 */
	public boolean checkIfFileCopyingIsInProcess (Path filePath) throws InterruptedException {
		_LOGGER.info("FileWatcher-checkIfFileCopyingIsInProcess-STARTS");
		String sourcePath = properties.getProperty("source.path");
		File file = new File(sourcePath+"/"+filePath.toString());
		boolean inProgress = true;
		while(true){
			long lastModifiedTime = file.lastModified();
			long lastFileSize = file.length();
			Thread.sleep(10000);
			long nextModifiedTime = file.lastModified();
			long nextFileSize = file.length();
			if(lastModifiedTime!=nextModifiedTime || nextFileSize!=lastFileSize){
				_LOGGER.info("File is being copied");
				Thread.sleep(10000);
			}else{
				_LOGGER.info("File copying completed");
				inProgress=false;
				break;
			}
		}
		_LOGGER.info("FileWatcher-checkIfFileCopyingIsInProcess-ENDS");
		return inProgress;
	}
	
	/**
	 * Any file present in temp folder.
	 *
	 * @return true, if successful
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public boolean anyFilePresentInTempFolder() throws IOException {
		_LOGGER.info("FileWatcher-anyFilePresentInTempFolder-STARTS");
		boolean fileExists = false;
		String tempFolderPath = System.getProperty("user.dir")+"/tempFolder";
		File tempFolder = new File(tempFolderPath);
		if(tempFolder.exists()){
			File[] allFiles = tempFolder.listFiles();
			if(allFiles.length>0){
				fileExists = true;
			}
		}else{
			fileExists = false;
		}
		_LOGGER.info("FileWatcher-anyFilePresentInTempFolder-ENDS");
		return fileExists;
	}
	
	/**
	 * Move file from temp to source.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void moveFileFromTempToSource() throws IOException {
		_LOGGER.info("FileWatcher-moveFileFromTempToSource-STARTS");
		String tempFolderPath = System.getProperty("user.dir")+"/tempFolder";
		File tempFolder = new File(tempFolderPath);
		if(tempFolder.exists()){
			File[] allFiles = tempFolder.listFiles();
			if(allFiles.length>0){
				String sourcePath = properties.getProperty("source.path");
				for(int i = 0;i<allFiles.length;i++){
					FileUtils.moveFile(new File(tempFolderPath+"/"+allFiles[i].getName()),
										new File(sourcePath+"/"+allFiles[i].getName().split("##")[0]));
				}
			}
			_LOGGER.info("allFiles.length "+allFiles.length);
			if(new File(tempFolderPath).listFiles().length == 0){
				FileUtils.forceDelete(tempFolder);
			}
		}
		_LOGGER.info("FileWatcher-moveFileFromTempToSource-ENDS");
	}
	
	/**
	 * Check for specified file name.
	 *
	 * @param filePath the file path
	 * @throws Exception the exception
	 */
	@SuppressWarnings("unchecked")
	public void checkForSpecifiedFileName(Path filePath) throws Exception {
		_LOGGER.info("FileWatcher-checkForSpecifiedFile-STARTS");
		String fileName = filePath.toString();
		Set<String> keys = targetMappingJson.keySet();
		boolean fileMappingFound = false;
		for(String key:keys){
			if(fileName.contains(key)){
				_LOGGER.info("file found");
				fileMappingFound = true;
				String sourcePath = properties.getProperty("source.path");
				String stagingDirPath = properties.getProperty("stagingDir.path");
				String backupPath = properties.getProperty("backup.path");
				String targetTableName = (String) targetMappingJson.get(key);
				String newFileName = getNewNameForSourceFile(fileName,targetTableName);
				if(!newFileName.isEmpty()){
					moveFileToStagingFolder(fileName,newFileName,sourcePath,stagingDirPath);
					FTPFileToDestination(newFileName);
					takeSourceFileBackup(newFileName, stagingDirPath, backupPath);
				}else{
					_LOGGER.error("Problem while renaming source file");
				}
				break;
			}
		}
		if(!fileMappingFound){
			pushFileToUnmappedFolder(fileName);
		}
		_LOGGER.info("FileWatcher-checkForSpecifiedFile-ENDS");
	}
	
	/**
	 * Push file to unmapped folder.
	 *
	 * @param fileName the file name
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void pushFileToUnmappedFolder(String fileName) throws IOException {
		_LOGGER.info("FileWatcher-pushFileToUnmappedFolder-STARTS");
		String sourcePath = properties.getProperty("source.path");
		String hostname = InetAddress.getLocalHost().getHostName();
		File sourceFile = new File(sourcePath+"/"+fileName);
		String extention = FilenameUtils.getExtension(fileName);
		String fileNameWithOutExt = FilenameUtils.removeExtension(fileName);
		String unmappedFolderPath = System.getProperty("user.dir")+"/unmappedFiles/"+fileNameWithOutExt+"#"+hostname+"#"+(new SimpleDateFormat("ddMMMyyyyHHmmss").format(new Date())+"."+extention);
		FileUtils.moveFile(sourceFile, new File(unmappedFolderPath));
		_LOGGER.info("FileWatcher-pushFileToUnmappedFolder-ENDS");
	}
	
	/**
	 * Gets the new name for source file.
	 *
	 * @param fileName the file name
	 * @param targetTableName the target table name
	 * @return the new name for source file
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public String getNewNameForSourceFile(String fileName,String targetTableName) throws IOException {
		_LOGGER.info("FileWatcher-getNewNameForSourceFile-STARTS");
		String hostname = InetAddress.getLocalHost().getHostName();
		String[] nameParts = fileName.split("\\.(?=[^.]*$)");
		if(nameParts.length == 2){
			fileName = targetTableName+"#"+hostname+"#"+(new SimpleDateFormat("ddMMMyyyyHHmmss").format(new Date()))+"."+nameParts[1];
		}else{
			_LOGGER.info("Problem detected while splitting source file name");
		}
		_LOGGER.info("FileWatcher-getNewNameForSourceFile-ENDS");
		return fileName;
	}
	
	/**
	 * Move file to staging folder.
	 *
	 * @param oldFileName the old file name
	 * @param newFileName the new file name
	 * @param sourceFolder the source folder
	 * @param stagingFolderPath the staging folder path
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void moveFileToStagingFolder(String oldFileName,String newFileName,String sourceFolder,String stagingFolderPath) throws IOException {
		_LOGGER.info("FileWatcher-moveFileToStagingFolder-STARTS");
		File stagingFolder = new File(stagingFolderPath);
		if(!stagingFolder.exists()){
			FileUtils.forceMkdir(stagingFolder);
		}
		File srcFile = new File(sourceFolder+"/"+oldFileName);
		File destFile = new File(stagingFolderPath+"/"+newFileName);
		FileUtils.moveFile(srcFile, destFile);
		/*FileUtils.copyFile(srcFile, destFile);
		srcFile.delete();*/
		_LOGGER.info("FileWatcher-moveFileToStagingFolder-END");
	}
	
	/**
	 * FTP file to destination.
	 *
	 * @param fileName the file name
	 * @throws Exception the exception
	 */
	public void FTPFileToDestination(String fileName) throws Exception {
		_LOGGER.info("FileWatcher-copyFileUsingJcifs-STARTS");
		_LOGGER.info("FTPing:: "+fileName);
		FTPUploader ftpUploader = new FTPUploader(properties.getProperty("destination.ip"), 
													properties.getProperty("username"), 
													properties.getProperty("password"));
		ftpUploader.uploadFile(properties.getProperty("stagingDir.path")+"/"+fileName, fileName, "");
		ftpUploader.disconnect();
		_LOGGER.info("FileWatcher-copyFileUsingJcifs-ENDS");
	}
	
	/**
	 * Take source file backup.
	 *
	 * @param fileName the file name
	 * @param stagingPath the staging path
	 * @param backupPath the backup path
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public void takeSourceFileBackup(String fileName,String stagingPath,String backupPath) throws IOException {
		_LOGGER.info("FileWatcher-takeSourceFileBackup-STARTS");
		File backupFolder = new File(backupPath);
		if (!backupFolder.exists()) {
			FileUtils.forceMkdir(new File(backupPath));
		}
		FileUtils.moveFile(new File(stagingPath+"/"+fileName),new File(backupPath+"/"+fileName));
		_LOGGER.info("FileWatcher-takeSourceFileBackup-ENDS");
	}
}