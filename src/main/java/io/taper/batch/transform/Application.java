package io.taper.batch.transform;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class Application {

	public static void main(String[] args) {
		
		String commandLineArgs = Arrays.stream(args).collect(Collectors.joining("|"));
		log.info("App started with args=" + commandLineArgs);
		
		SpringApplication.run(Application.class, args);

	}
}
