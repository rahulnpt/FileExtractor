package com.wipro.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/**
 * @author rahul.gupta25
 *
 */
@Configuration
@ComponentScan(basePackages = "com.wipro")
//@PropertySource(value = { "classpath:sqlServer.properties" })
public class ApplicationConfig {

	/*@Autowired
	private Environment env;*/
	private Properties env;
	
	ApplicationConfig() throws IOException{
		Properties props = new Properties();
		InputStream inStream = new FileInputStream("src/sqlServer.properties");
		props.load(inStream);
		this.env = props;
	}
	/**
	 * Data source.
	 *
	 * @return the data source
	 */
	@Bean
	public DataSource dataSource() {
		DriverManagerDataSource dataSource = new DriverManagerDataSource();
		dataSource.setDriverClassName(env.getProperty("jdbc.driverClassName"));
		dataSource.setUrl(env.getProperty("jdbc.url"));
		dataSource.setUsername(env.getProperty("jdbc.username"));
		dataSource.setPassword(env.getProperty("jdbc.password"));
		return dataSource;
	}

	/**
	 * Jdbc template.
	 *
	 * @param dataSource the data source
	 * @return the jdbc template
	 */
	@Bean
	public JdbcTemplate jdbcTemplate(DataSource dataSource) {
		JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
		jdbcTemplate.setResultsMapCaseInsensitive(true);
		return jdbcTemplate;
	}
}
