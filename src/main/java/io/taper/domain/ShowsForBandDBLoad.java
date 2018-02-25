package io.taper.domain;

import java.util.List;

import lombok.Data;

@Data
public class ShowsForBandDBLoad {

	private String band;
	private String name;
	private List<Show> shows;
//	private List<ShowMetadata> shows;

}
