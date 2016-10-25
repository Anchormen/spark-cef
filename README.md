# Spark-CEF

An Apache Spark DataSource used to read and parse Common Event Format (CEF) files according to [CEF version 23](https://www.protect724.hpe.com/servlet/JiveServlet/downloadBody/1072-102-9-20354/CommonEventFormatv23.pdf). CEF files are parsed into a DataFrame with CEF specific schema. Fields with specific types (Integer, Long, Float, Double or Timestamp) are cast and/or parsed into the right type.

#### Usage

The DataSource can be used in the following way which will infer the schema based on all the data (i.e. it performs two passes over the data!)

```sbt
val cefDataFrame = sqlContext.read.format("nl.anchormen.spark.cef.CefSource")
    .load("path/to/your_cef_data.log")
```

This will result in a DataFrame with the following schema:

```terminal
root
 |-- CEF_Version: integer (nullable = true)
 |-- CEF_Device Vendor: string (nullable = true)
 |-- CEF_Device Product: string (nullable = true)
 |-- CEF_Device Version: string (nullable = true)
 |-- CEF_Device Event Class ID: string (nullable = true)
 |-- CEF_Name: string (nullable = true)
 |-- CEF_Severity: string (nullable = true)
 |-- extension_field_1: <field1 type> (nullable = true)
 |-- extension_field_2: <field2 type> (nullable = true)
 |-- other extension fields...
```

Anything before 'CEF', like syslog metadata, will be dropped. The schema is always inferred, hence it is not possible to set a schema using the DataSources schema(…) function.

***The current implementation requires each CEF record to be on a single line and allows all fields to be empty/null.***

 The current implementation supports a number of options:

1. schema.lines (default = -1): the number of lines to use to infer the schema. This can be used to avoid a full pass over the data. Note: the specified number of lines is taken into the driver! Specifying a number smaller than 0 indicates that a full pass must be made in order to determine the schema. 
2. partitions (default is determined by spark): the number of partitions the result should have. This number is passed to the sc.textFile(…) operation used to read the CEF file(s).
3. epoch.millis.fields: a comma separated list of fields that contain a unix timestamp in milliseconds and should be cast to a Timestamp. Example: *"epoch.millis.fields"->"field1, field2, field3"*
4. string.trim (default = false): specifies that all string values should be trimmed
5. ignore.exception (default = false): specifies if parsing exceptions should be ignored. By default the parser will fail fast on the first exception it encounters. When set to 'true' the reader will drop lines it fails to parse
6. exception.log (default: true): only applies when ignore.exception is set to 'true' and indicates that parse failures should be logged. 

Characters that are escaped (like \n, \\| and \\=) are converted into the right character or linefeed. 

#### Timestamps

The CEF specification supports different timestamp patterns. During schema inference the DataSource determines the pattern used for each timestamp field found in the data. The pattern that was determined for the first occurrence of a timestamp field is used to parse all the values for that field. The current implementation does not support multiple patterns for the same field! 