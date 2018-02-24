package io.taper.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class ShowMetadata {

	private String identifier;
	private String addeddate;
	private String coverage;
	private String date;
	private String mediatype;
	private String title;
	private String venue;
	private String source;
	private String lineage;
	private String taper;
	private String transferer;
	private String runtime;
	private String description;
	
}
