package com.wipro.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

/**
 *
 * @author rahul.gupta25
 */
public class FTPUploader {

	private static final Logger _LOGGER = Logger.getLogger(FTPUploader.class);

	FTPClient ftp = null;

	/**
	 * Instantiates a new FTP uploader.
	 *
	 * @param hostIP the host IP
	 * @param userName the user name
	 * @param password the password
	 * @throws Exception the exception
	 */
	public FTPUploader(String hostIP, String userName, String password) throws Exception {
		_LOGGER.info("FTPUploader-FTPUploader-STARTS");
		ftp = new FTPClient();
		ftp.addProtocolCommandListener(new PrintCommandListener(
				new PrintWriter(System.out)));
		int reply;
		ftp.connect(hostIP, 21);
		reply = ftp.getReplyCode();
		if (!FTPReply.isPositiveCompletion(reply)) {
			ftp.disconnect();
			throw new Exception("Exception in connecting to FTP Server");
		}
		ftp.login(userName, password);
		ftp.setFileType(FTP.BINARY_FILE_TYPE);
		ftp.enterLocalPassiveMode();
		_LOGGER.info("FTPUploader-FTPUploader-ENDS");
	}

	/**
	 * Upload file.
	 *
	 * @param localFileFullName the local file full name
	 * @param fileName the file name
	 * @param hostDir the host dir
	 * @throws Exception the exception
	 */
	public void uploadFile(String localFileFullName, String fileName,String hostDir) throws Exception {
		_LOGGER.info("FTPUploader-uploadFile-STARTS");
		try (InputStream input = new FileInputStream(new File(localFileFullName))) {
			this.ftp.storeFile(hostDir + fileName, input);
			File markerFile = new File(localFileFullName + ".rdy");
			if (!markerFile.exists()) {
				markerFile.createNewFile();
				_LOGGER.info("marker File created successfully");
			} else {
				_LOGGER.error("marker file could not be created");
			}
			FileInputStream markerInputStream = new FileInputStream(new File(localFileFullName + ".rdy"));
			this.ftp.storeFile(hostDir + fileName + ".rdy",markerInputStream);
			markerInputStream.close();
			input.close();
			if (markerFile.delete()) {
				_LOGGER.info("marker file deleted successfully");
			} else {
				_LOGGER.info("Could not delete marker file");
			}
		}
		_LOGGER.info("FTPUploader-uploadFile-ENDS");
	}

	/**
	 * Disconnect.
	 */
	public void disconnect() {
		_LOGGER.info("FTPUploader-disconnect-STARTS");
		if (this.ftp.isConnected()) {
			try {
				this.ftp.logout();
				this.ftp.disconnect();
			} catch (IOException e) {
			}
		}
		_LOGGER.info("FTPUploader-disconnect-ENDS");
	}
}