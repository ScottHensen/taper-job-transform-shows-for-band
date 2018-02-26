package io.taper.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;


@Component
@ConfigurationProperties("io")
@Data
public class IOConfig {
	
	private String path;
	private int    dynamoDbBatchWriteMax;
	
}
