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
import java.util.Comparator;
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

import com.fasterxml.jackson.databind.ObjectMapper;

import io.taper.config.IOConfig;
import io.taper.domain.Show;
import io.taper.domain.ShowMetadata;
import io.taper.domain.Song;
import lombok.extern.slf4j.Slf4j;

@Component
@Order(1)
@ComponentScan("io.taper.config")
@Slf4j
public class TransformShowDetailsMetadataToDBLoadFile implements CommandLineRunner {
	
	@Autowired
	private IOConfig ioConfig;

	@Override
	public void run(String... args) 
		   throws Exception 
	{	
		String           commandLineArgs    = Arrays.stream(args).collect(Collectors.joining("|"));
		String           bandName           = Arrays.stream(args).findFirst().get(); 
		String           pathName           = ioConfig.getPath();
		String           showDBLoadFileName = pathName + "ZDB_" + bandName + "_TaperShow.json";
		ObjectMapper     mapper             = new ObjectMapper();
		List<Path>       showFiles          = listFiles( pathName, bandName+"_*" );
		List<JSONObject> putRequestArray    = new ArrayList<>();
		int              BATCH_WRITE_MAX    = ioConfig.getDynamoDbBatchWriteMax();	
		int              readCnt            = 0;
		int              procCnt            = 0;
		int              writeCnt           = 0;

		log.info("App started with args =" + commandLineArgs);	
		log.debug("Band Name =" + bandName);
		
		// spin through extracted show files; write a load file for every 25 shows
		while ( readCnt < showFiles.size() ) {
			log.debug("readCnt=" + readCnt + " of " + showFiles.size() + ", procCnt=" + procCnt + ", writeCnt" + writeCnt);
			
			if (procCnt >= BATCH_WRITE_MAX) {
				showDBLoadFileName = pathName + "ZDB" + ++writeCnt + "_" + bandName + "_TaperShow.json";
				writeAwsBatchLoadFile(showDBLoadFileName,putRequestArray);
				putRequestArray.clear();
				procCnt = 0;
			}
			
			Show show = mapper.readValue(new File(showFiles.get(readCnt).toString()), Show.class);  
			readCnt++;
			
			show.setBandId(bandName);		
			putRequestArray.add(buildDynamoPutRequest(show));			
			procCnt++;			
		}
		
		showDBLoadFileName = pathName + "ZDB" + ++writeCnt + "_" + bandName + "_TaperShow.json";
		writeAwsBatchLoadFile(showDBLoadFileName,putRequestArray);
	}

	private void writeAwsBatchLoadFile(String fileName, List<JSONObject> putRequestArray) 
			throws JSONException, IOException 
	{
		JSONObject tableLoadObj = new JSONObject();
		tableLoadObj.put("dev-taper-shows", putRequestArray);  //TODO: make db-name & -env properties
		
		FileWriter fileWriter = new FileWriter(fileName);
		fileWriter.write(tableLoadObj.toString(4));
		fileWriter.flush();
		fileWriter.close();
	}

	private JSONObject buildDynamoPutRequest(Show show) 
			throws JSONException 
	{
		JSONObject       putRequest = new JSONObject();
		JSONObject       itemObj    = new JSONObject();
		ShowMetadata     meta       = show.getMetadata();
		
		HashMap<String, HashMap<String,Object>> strHashMap = new HashMap<String, HashMap<String, Object>>();
		
		strHashMap.put("showId",         buildElementHashMap("S", meta.getIdentifier())            );
		strHashMap.put("addedDate",      buildElementHashMap("S", meta.getAddeddate())             );
		strHashMap.put("geoLocation",    buildElementHashMap("S", meta.getCoverage())              );
		strHashMap.put("showDate",       buildElementHashMap("S", meta.getDate())                  );
		strHashMap.put("mediaType",      buildElementHashMap("S", meta.getMediatype())             );
		strHashMap.put("title",          buildElementHashMap("S", meta.getTitle())                 );
		strHashMap.put("venue",          buildElementHashMap("S", meta.getVenue())                 );
		strHashMap.put("source",         buildElementHashMap("S", meta.getSource())                );
		strHashMap.put("lineage",        buildElementHashMap("S", meta.getLineage())               );
		strHashMap.put("taperName",      buildElementHashMap("S", meta.getTaper())                 );
		strHashMap.put("transfererName", buildElementHashMap("S", meta.getTransferer())            );
		strHashMap.put("runtime",        buildElementHashMap("S", meta.getRuntime())               );
		strHashMap.put("description",    buildElementHashMap("S", meta.getDescription())           );
		strHashMap.put("bandId",         buildElementHashMap("S", show.getBandId())                );
		strHashMap.put("posterId",       buildElementHashMap("S", show.getPosterId())              );
		strHashMap.put("avgRating",      buildElementHashMap("S", show.getAvg_rating())            );
		strHashMap.put("numReviews",     buildElementHashMap("N", show.getNum_reviews())           );
		strHashMap.put("downloads",      buildElementHashMap("N", show.getDownloads())             );
		strHashMap.put("tracks",         buildElementHashMap("L", buildListOfSetListSongs(
										  						   getSetListFromShowFiles(show))) );
			
		itemObj.put("Item", strHashMap);	
		putRequest.put("PutRequest", itemObj);
		
		return putRequest;		
	}

	private ArrayList<Song> getSetListFromShowFiles(Show show) 
	{
		Comparator<Song> comp   = Comparator
				  					  .comparing(
				  							  Song::getTrack, 
				  							  Comparator.nullsLast(Comparator.naturalOrder()))
				  					  .thenComparing(
				  							  Song::getName);

		ArrayList<Song> setList = 
				(ArrayList<Song>) show.getFiles()
									  .stream()
				 					  .filter(file -> ".FLAC".equals(
				 							  file.getName().toUpperCase()
				 							      .substring(Math.max(0,  file.getName().length() - 5))))
									  .sorted(comp)
									  .collect(Collectors.toList());
		
		log.debug("setList.valueOf =" + String.valueOf(setList));
		//TODO: If we get shows with no flacs, we need to look for Ogg Vorbis or something	

		return setList;
	}

	private List<JSONObject> buildListOfSetListSongs(ArrayList<Song> setList) 
	{
		List<JSONObject> songArray = new ArrayList<>();
		setList.forEach(n -> {
			try {
				songArray.add(buildSong(n));
			} catch (JSONException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		});
		
		return songArray;
	}

	private JSONObject buildSong(Song n) throws JSONException {
		
		JSONObject      mapObj    = new JSONObject();
		HashMap<String, HashMap<String,Object>> strHashMap = new HashMap<String, HashMap<String, Object>>();
		
		strHashMap.put("name",    buildElementHashMap("S", n.getName())   );
		strHashMap.put("track",   buildElementHashMap("S", n.getTrack())  );
		strHashMap.put("format",  buildElementHashMap("S", n.getFormat()) );
		strHashMap.put("title",   buildElementHashMap("S", n.getTitle())  );
		mapObj.put("M", strHashMap);
		return mapObj;
	}

	protected static HashMap<String, Object> buildElementHashMap(String type, String value) {
		HashMap<String, Object> strStrMap = new HashMap<String, Object>();
		if ( StringUtils.isEmpty(value) ) {value="unknown";}
		strStrMap.put(type, value);						
		return strStrMap;
	}

	protected static HashMap<String, Object> buildElementHashMap(String type, int value) {
		HashMap<String, Object> strStrMap = new HashMap<String, Object>();
		strStrMap.put(type, String.valueOf(value));						
		return strStrMap;
	}

	protected static HashMap<String, Object> buildElementHashMap(String type, List<JSONObject> value) {
		HashMap<String, Object> strStrMap = new HashMap<String, Object>();
		strStrMap.put(type, value);						
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

}
