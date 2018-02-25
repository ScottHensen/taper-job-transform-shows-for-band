package io.taper.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown=true)
public class Show {

	private String avg_rating;
	private int    num_reviews;
	private int    downloads;
	private ShowMetadata metadata;
	
}
