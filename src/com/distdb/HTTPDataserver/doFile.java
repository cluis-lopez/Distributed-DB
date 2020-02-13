package com.distdb.HTTPDataserver;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

public class doFile {
	Logger log;
	String adminRootPath;

	public doFile(Logger log, String adminRootPath) {
		this.log = log;
		this.adminRootPath = adminRootPath;
	}
	
	public String[] doGet(String resource) {
		String[] ret = new String[2];
		ret[0] = "text/plain"; //Default response if cannot determne mimetype
		try {
			ret[0] = Files.probeContentType(Paths.get(resource));
			ret[1] = new String (Files.readAllBytes(Paths.get(resource)));
			if (ret[0] == null) {
				log.log(Level.WARNING, "Mimetype of " + resource + " cannot be determined");
				log.log(Level.WARNING, "Assuming the content matches suffix of file");
				String fileExt = "";
				int i = resource.lastIndexOf('.');
				if (i > 0) {
					fileExt = resource.substring(i+1);
					ret[0] = GetMimeType.getMimeType(fileExt);
				}
			}
		} catch (FileNotFoundException e) {
			ret[0]="";
			ret[1] = "File not found";
		} catch (IOException e) {
			ret[0]="";
			ret[1] = "Cannot read file";
		}
		
		return ret;
	}

}
