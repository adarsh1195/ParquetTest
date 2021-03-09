package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetReaderUtils {

	public static Parquet getParquetData(String filePath) throws IOException {
		List<SimpleGroup> simpleGroups = new ArrayList<>();
		ParquetFileReader reader = ParquetFileReader
				.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
		
		

		List<Type> fields_1 = new ArrayList<>();

		fields_1.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "name", OriginalType.UTF8));

		MessageType schema_1 = new MessageType("ERM_INTERNAL_EKP.customer", fields_1);
		
		//Configuration conf = new Configuration();
		
		//conf.set(ReadSupport.PARQUET_READ_SCHEMA, schema_1);
		
		// MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		// System.out.println("Schema name - " + schema.getName());
		// List<Type> fields = schema.getFields();

		List<Type> fields = new ArrayList<>();

		fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.BINARY, "name", OriginalType.UTF8));

		fields.add(new PrimitiveType(Type.Repetition.OPTIONAL, PrimitiveTypeName.INT32, "age"));

		MessageType schema = new MessageType("ERM_INTERNAL_EKP.customer", fields);

		System.out.println("Schema name - " + schema.getName());
	
		
		//MessageType schema_2 = new MessageType(Read, fields);

		
		
		PageReadStore pages;
		while ((pages = reader.readNextRowGroup()) != null) {
			long rows = pages.getRowCount();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema_1);
			RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

			for (int i = 0; i < rows; i++) {
				SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
				simpleGroups.add(simpleGroup);
			}
		}
		reader.close();
		return new Parquet(simpleGroups, fields);
	}
}