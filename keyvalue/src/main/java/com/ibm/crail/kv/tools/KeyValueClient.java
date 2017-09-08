package com.ibm.crail.kv.tools;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ibm.crail.CrailLocationClass;
import com.ibm.crail.CrailStorageClass;
import com.ibm.crail.kv.CrailKVStore;

public class KeyValueClient implements Runnable {
	private int tables;
	private int keys;
	private int size;
	
	public KeyValueClient(int tables, int keys, int size){
		this.tables = tables;
		this.keys = keys;
		this.size = size;
	}
	
	@Override
	public void run() {
		try {
			CrailKVStore kvStore = new CrailKVStore();
			
			System.out.println("creating tables...");
			for (int i = 0; i < tables; i++){
				String table = "table" + i;
				kvStore.createTable(table, CrailStorageClass.DEFAULT, CrailLocationClass.DEFAULT);
			}
			
			System.out.println("writing keys...");			
			ByteBuffer buffer = ByteBuffer.allocateDirect(size);
			for (int i = 0; i < keys; i++){
				int index = i % tables;
				String table = "table" + index;
				String key = "key" + i;
				buffer.clear();
				kvStore.writeKey(table, key, buffer);
			}
			
			System.out.println("reading keys...");
			for (int i = 0; i < keys; i++){
				int index = i % tables;
				String table = "table" + index;
				String key = "key" + i;
				buffer.clear();
				kvStore.readKey(table, key, buffer);
			}	
			
			System.out.println("closing...");
			kvStore.close();
			System.out.println("done...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) throws InterruptedException, ParseException{
		int tables = 32;
		int keys = 0;
		int size = 32;
		
		Option tableOption = Option.builder("t").desc("number of tables").hasArg().build();
		Option keyOption = Option.builder("k").desc("number of keys").hasArg().build();
		Option sizeOption = Option.builder("s").desc("size of value").hasArg().build();
		
		Options options = new Options();
		options.addOption(tableOption);
		options.addOption(keyOption);
		options.addOption(sizeOption);
		
		CommandLineParser parser = new DefaultParser();
		CommandLine line = parser.parse(options, Arrays.copyOfRange(args, 0, args.length));
		if (line.hasOption(tableOption.getOpt())) {
			tables = Integer.parseInt(line.getOptionValue(tableOption.getOpt()));
		}
		if (line.hasOption(keyOption.getOpt())) {
			keys = Integer.parseInt(line.getOptionValue(keyOption.getOpt()));
		}
		if (line.hasOption(sizeOption.getOpt())) {
			size = Integer.parseInt(line.getOptionValue(sizeOption.getOpt()));
		}
		
		if (keys <= 0){
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("crail kv client", options);
			System.exit(-1);			
		}
		
		KeyValueClient client = new KeyValueClient(tables, keys, size);
		Thread thread = new Thread(client);
		thread.start();
		thread.join();
		System.out.println("exiting..");
	}
	
}
