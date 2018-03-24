package io.taper.batch.transform;

//import static org.junit.Assert.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;

import org.junit.Test;

public class TransformShowDetailsMetadataToDBLoadFileTests {

	@Test
	public void givenEmptyValue_whenBuildElementHashMap_thenValueDefaultsToUnknown() {

		// Given
		String type     = "foo";
		String value    = null;
		String expected = "unknown";
		// When
		HashMap<String,Object> result = new HashMap<String,Object>();
		result = TransformShowDetailsMetadataToDBLoadFile.buildElementHashMap(type,value);
		// Then
		assertThat(result.get(type)).isEqualTo(expected);
	}

	@Test
	public void givenStringValue_whenBuildElementHashMap_thenValueIsString() {

		// Given
		String type     = "foo";
		String value    = "bar";
		String expected = "bar";
		// When
		HashMap<String,Object> result = new HashMap<String,Object>();
		result = TransformShowDetailsMetadataToDBLoadFile.buildElementHashMap(type,value);
		// Then
		assertThat(result.get(type)).isEqualTo(expected);
	}

	@Test
	public void givenIntValue_whenBuildElementHashMap_thenValueIsIntConvertedToString() {

		// Given
		String type     = "foo";
		int    value    =  8;
		String expected = "8";
		// When
		HashMap<String,Object> result = new HashMap<String,Object>();
		result = TransformShowDetailsMetadataToDBLoadFile.buildElementHashMap(type,value);
		// Then
		assertThat(result.get(type)).isEqualTo(expected);
	}

}
