package com.wipro.main;

import java.util.ArrayList;
import java.util.List;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.ftpserver.ftplet.UserManager;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.log4j.Logger;
// TODO: Auto-generated Javadoc

/**
 * The Class FTPServer.
 */
public class FTPServer{
	
	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(FTPServer.class);
	
	/**
	 * Start FTP server.
	 *
	 * @param userName the user name
	 * @param password the password
	 * @param homeDir the home dir
	 * @return true, if successful
	 * @throws FtpException the ftp exception
	 */
	public  boolean startFTPServer(String userName,String password,String homeDir) throws FtpException {
		LOGGER.info("FTPServer-startFTPServer-STARTED");
        	boolean isStarted = false;
        	PropertiesUserManagerFactory userManagerFactory = new PropertiesUserManagerFactory();
        	UserManager userManager = userManagerFactory.createUserManager();
        	BaseUser user = new BaseUser();
        	user.setName(userName);
        	user.setPassword(password);
        	user.setHomeDirectory(homeDir);
        	List<Authority> authorities = new ArrayList<Authority>();
        	authorities.add(new WritePermission());
        	user.setAuthorities(authorities);
			userManager.save(user);
			ListenerFactory listenerFactory = new ListenerFactory();
			listenerFactory.setPort(21);
			FtpServerFactory factory = new FtpServerFactory();
			factory.setUserManager(userManager);
			factory.addListener("default", listenerFactory.createListener());
			FtpServer server = factory.createServer();
			server.start();
			if(!server.isStopped()){
				isStarted=true;
				LOGGER.info("FTP Server Started successfully");
			}else{
				isStarted=false;
				LOGGER.error("Failed to start FTP server");			}
        LOGGER.info("FTPServer-startFTPServer-ENDS");
        return isStarted;
    }
}
