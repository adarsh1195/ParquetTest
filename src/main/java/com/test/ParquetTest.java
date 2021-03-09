package com.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class ParquetTest {

	public static void main(String[] args) throws IOException {
		
		
		  Parquet parquet = ParquetReaderUtils.getParquetData("C:/Users/praveen.kumar3/Desktop/Adarsh/sample.parquet");
		  SimpleGroup simpleGroups = parquet.getData().get(0);
		 // String storedString = simpleGroups.get(0).getString("theFieldIWant", 0);
		  String storedString = simpleGroups.getString("name", 0);
		  
		  
		  
		  System.out.println("--- " + storedString);
		  
		  System.out.println("--- " + simpleGroups.getInteger("age", 0));
		  
		  
		  
		  
		  
		  
		  
		  //System.out.println(simpleGroups.getInteger("age", 0));

		  
		  ParquetFileReader reader = ParquetFileReader
					.open(HadoopInputFile.fromPath(new Path("C:/Users/praveen.kumar3/Desktop/Adarsh/sample.parquet"), new Configuration()));
		  
		  MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		  
		  List<Type> fields = schema.getFields();
		  
		  for (int i = 0 ; i < fields.size() ; i++) {
			  
			 
			  System.out.println("------------------------");
			 
			  System.out.println(fields.get(i).getId());

			  System.out.println(fields.get(i).getName());

			  System.out.println(fields.get(i).getOriginalType());

			  System.out.println(fields.get(i).getRepetition());
			  
		  }

	}

}
