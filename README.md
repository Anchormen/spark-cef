# Spark-CEF

An Apache Spark DataSource used to read and parse Common Event Format (CEF) files according to [CEF version 23](https://www.protect724.hpe.com/servlet/JiveServlet/downloadBody/1072-102-9-20354/CommonEventFormatv23.pdf). CEF files are parsed into a DataFrame with CEF specific schema. Fields with specific types (Integer, Long, Float, Double or Timestamp) are cast and/or parsed into the right type.

#### Usage

The DataSource can be used in the following way which will infer the schema based on all the data (i.e. it performs two passes over the data!)

```sbt
val cefDataFrame = sqlContext.read.format("nl.anchormen.spark.cef.CefSource")
    .load("path/to/yourdata.cef")
```

This will result in a DataFrame with the following schema:

```terminal
root
 |-- Version: integer (nullable = false)
 |-- Device Vendor: string (nullable = false)
 |-- Device Product: string (nullable = false)
 |-- Device Version: string (nullable = false)
 |-- Device Event Class ID: string (nullable = false)
 |-- Name: string (nullable = false)
 |-- Severity: string (nullable = false)
 |-- extension_field_1: <field1 type> (nullable = true)
 |-- extension_field_2: <field2 type> (nullable = true)
 |-- other extension fields...
```

The schema is always inferred, hence it is not possible to set a schema using the DataSources schema(…) function.

 The current implementation supports two options:

1. scanLines: the number of lines to use to infer the schema. This can be used to avoid a full pass over the data. Note: the specified number of lines is taken into the driver
2. partitions: the number of partitions the result should have. This number is passed to the sc.textFile(…) operation used to read the CEF file(s)

Characters that are escaped (like \n, \\| and \\=) are converted into the right character or linefeed. 

#### Timestamps

The CEF specification supports different timestamp patterns. During schema inference the DataSource determines the pattern used for each timestamp field found in the data. The pattern that was determined for the first occurrence of a timestamp field is used to parse all the values for that field. The current implementation does not support multiple patterns for the same field! 