package io.taper.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class Song {

	private String name;
	private String track;
	private String format;
	private String title;
	
}
