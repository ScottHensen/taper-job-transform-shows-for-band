package io.taper.batch.transform;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
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
		String commandLineArgs = Arrays.stream(args).collect(Collectors.joining("|"));
		String bandName        = Arrays.stream(args).findFirst().get(); 
		String pathName        = ioConfig.getPath();
		String outFileName     = pathName + "ZZ_" + bandName + ".json";

		log.info("App started with args =" + commandLineArgs);	
		log.debug("Band Name=" + bandName);
		
		ShowsForBandDBLoad showsForBand = new ShowsForBandDBLoad();
		showsForBand.setBand(bandName);
		
		List<Path> showFiles = listFiles( pathName, bandName+"_*" );
		List<ShowMetadata> showMetadata = new ArrayList<>();
		
		ObjectMapper mapper = new ObjectMapper();
		
		showFiles.forEach(showFile -> {
			try {
				Show show = mapper.readValue(new File(showFile.toString()), Show.class);  
				showMetadata.add(show.getMetadata());			
			}
			catch (IOException e) {
				log.error("mapShowDBLoad threw exception for showFile =" + showFile.toString(), e);
				//add a push msg here we will know the data is incomplete
			}
			
		});
		
		showsForBand.setShows(showMetadata);
		mapObjectToFile(mapper, showsForBand, outFileName);

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
