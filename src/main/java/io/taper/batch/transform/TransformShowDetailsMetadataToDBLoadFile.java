package io.taper.batch.transform;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.taper.config.IOConfig;
import io.taper.domain.Show;
import io.taper.domain.ShowMetadata;
import io.taper.domain.ShowsForBandDBLoad;
import lombok.extern.slf4j.Slf4j;

@Component
@Order(1)
@ComponentScan("io.taper.config")
@Slf4j
public class TransformShowDetailsMetadataToDBLoadFile implements CommandLineRunner {
	
	@Autowired
	private IOConfig ioConfig;

	@Override
	public void run(String... args) throws Exception {
		String commandLineArgs    = Arrays.stream(args).collect(Collectors.joining("|"));
		String bandName           = Arrays.stream(args).findFirst().get(); 
		String pathName           = ioConfig.getPath();
		String outFileName        = pathName + "ZZ_" + bandName + ".json";
		String showDBLoadFileName = pathName + "ZDB_" + bandName + "_TaperShow.json";

		log.info("App started with args =" + commandLineArgs);	
		log.debug("Band Name=" + bandName);
		
		ShowsForBandDBLoad showsForBand = new ShowsForBandDBLoad();
		showsForBand.setBand(bandName);
		
		List<Path> showFiles = listFiles( pathName, bandName+"_*" );
		List<Show> showList = new ArrayList<>();
		ObjectMapper mapper = new ObjectMapper();
		

		JSONObject tableLoadObj = new JSONObject();
		List<JSONObject> putRequestArray = new ArrayList<>();
		
		//TODO: MAX FOR BATCH LOAD IS 25.  GOTTA ADD A CONTROL BREAK TO WRITE THEN RENETER LOOP.
		// replace this with a while (showFiles iter hasNext)
		showFiles.forEach(showFile -> {
			try {
				Show show = mapper.readValue(new File(showFile.toString()), Show.class);  
				showList.add(show);
				
				JSONObject putRequest = new JSONObject();
				putRequest.put("PutRequest", buildDynamoDBItem(show));
				putRequestArray.add(putRequest);
			}
			catch (IOException | JSONException e) {
				log.error("mapShowDBLoad threw exception for showFile =" + showFile.toString(), e);
				//add a push msg here we will know the data is incomplete
			}
		});
		
		showsForBand.setShows(showList);
		mapObjectToFile(mapper, showsForBand, outFileName);

		tableLoadObj.put("TaperShow", putRequestArray);
		FileWriter fileWriter = new FileWriter(showDBLoadFileName);
		fileWriter.write(tableLoadObj.toString(4));
		fileWriter.flush();
		fileWriter.close();
		log.debug("tableLoadObject = " + tableLoadObj.toString());
	}

	private JSONObject buildDynamoDBItem(Show show) throws JSONException {
		
		ShowMetadata meta  = show.getMetadata();
		JSONObject itemObj = new JSONObject();
		
		HashMap<String, HashMap<String,String>> strHashMap = new HashMap<String, HashMap<String, String>>();
		
			strHashMap.put("identifier",  buildElementHashMap("S", meta.getIdentifier())  );
			strHashMap.put("addeddate",   buildElementHashMap("S", meta.getAddeddate())   );
			strHashMap.put("coverage",    buildElementHashMap("S", meta.getCoverage())    );
			strHashMap.put("date",        buildElementHashMap("S", meta.getDate())        );
			strHashMap.put("mediatype",   buildElementHashMap("S", meta.getMediatype())   );
			strHashMap.put("title",       buildElementHashMap("S", meta.getTitle())       );
			strHashMap.put("venue",       buildElementHashMap("S", meta.getVenue())       );
			strHashMap.put("source",      buildElementHashMap("S", meta.getSource())      );
			strHashMap.put("lineage",     buildElementHashMap("S", meta.getLineage())     );
			strHashMap.put("taper",       buildElementHashMap("S", meta.getTaper())       );
			strHashMap.put("transferer",  buildElementHashMap("S", meta.getTransferer())  );
			strHashMap.put("runtime",     buildElementHashMap("S", meta.getRuntime())     );
			strHashMap.put("description", buildElementHashMap("S", meta.getDescription()) );
			strHashMap.put("avg_rating",  buildElementHashMap("S", show.getAvg_rating())  );
			strHashMap.put("num_reviews", buildElementHashMap("N", show.getNum_reviews()) );
			strHashMap.put("downloads",   buildElementHashMap("N", show.getDownloads())   );
			
		itemObj.put("Item", strHashMap);
		
		return itemObj;
		
	}

	private HashMap<String, String> buildElementHashMap(String type, String value) {
		HashMap<String, String> strStrMap = new HashMap<String, String>();
		if ( StringUtils.isEmpty(value) ) {value="unknown";}
		strStrMap.put(type, value);						
		return strStrMap;
	}

	private HashMap<String, String> buildElementHashMap(String type, int value) {
		HashMap<String, String> strStrMap = new HashMap<String, String>();
		strStrMap.put(type, String.valueOf(value));						
		return strStrMap;
	}

	private List<Path> listFiles(String dirName, String str)
			throws IOException
	{
		Path dir = Paths.get(dirName);
		List<Path> result = new ArrayList<>();
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, str)) {
			for ( Path entry : stream ) {
				result.add(entry);
			}
		}
		catch (DirectoryIteratorException e ) {
			throw e.getCause();
		}
		return result;
	}

	private void mapObjectToFile(ObjectMapper mapper, Object object, String fileName) 
			throws JsonGenerationException, JsonMappingException, IOException 
	{
		File file = new File(fileName);
		mapper.writeValue(file, object);
	}
	

}
