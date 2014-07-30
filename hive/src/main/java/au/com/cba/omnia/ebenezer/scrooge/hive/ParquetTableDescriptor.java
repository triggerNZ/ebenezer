//   Copyright 2014 Commonwealth Bank of Australia
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package au.com.cba.omnia.ebenezer.scrooge.hive;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import cascading.tap.hive.HiveTableDescriptor;

/**
 * A table descriptor for Hive where underlying data is stored using Parquet.
 */
public class ParquetTableDescriptor extends HiveTableDescriptor {
    /** default input format used by Hive */
    public static final String PARQUET_INPUT_FORMAT      = "parquet.hive.DeprecatedParquetInputFormat";

    /** default output format used by Hive */
    public static final String PARQUET_OUTPUT_FORMAT     = "parquet.hive.DeprecatedParquetOutputFormat";

    /** default serialization lib name*/
    public static final String PARQUET_SERIALIZATION_LIB = "parquet.hive.serde.ParquetHiveSerDe";

    /**
     * Constructs a new ParquetHiveTableDescriptor object.
     *
     * @param tableName        The table name.
     * @param columnNames      Names of the columns.
     * @param columnTypes      Hive types of the columns.
     * @param partitionColumns The columns used for partitioning. Must be a subset of `columnNames`.
     */
    public ParquetTableDescriptor(String databaseName, String tableName, String[] columnNames,
                                  String[] columnTypes, String[] partitionColumns) {
        super(databaseName, tableName, columnNames, columnTypes,
              partitionColumns, HiveTableDescriptor.HIVE_DEFAULT_DELIMITER,
              PARQUET_SERIALIZATION_LIB, null);
    }

    /**
     * Constructs a new ParquetHiveTableDescriptor object.
     *
     * @param tableName        The table name.
     * @param columnNames      Names of the columns.
     * @param columnTypes      Hive types of the columns.
     * @param partitionColumns The columns used for partitioning. Must be a subset of `columnNames`.
     * @param location         Override the location of the table
     */
    public ParquetTableDescriptor(String databaseName, String tableName, String[] columnNames,
                                  String[] columnTypes, String[] partitionColumns, Path location) {
        super(databaseName, tableName, columnNames, columnTypes,
              partitionColumns, HiveTableDescriptor.HIVE_DEFAULT_DELIMITER,
              PARQUET_SERIALIZATION_LIB, location);
    }
    
    /**
     * Converts the instance to a Hive Table object, which can be used with the MetaStore API.
     *
     * Overrides the input and output format of [[HiveTableDescriptor]].
     *
     * @return a new Table instance.
     */
    @Override 
    public Table toHiveTable() {
        Table table          = super.toHiveTable();
        StorageDescriptor sd = table.getSd();

        sd.setInputFormat(PARQUET_INPUT_FORMAT);
        sd.setOutputFormat(PARQUET_OUTPUT_FORMAT);
        table.setSd(sd);
        
        return table;
    }

    @Override
    public String toString() {
        return "Parquet" + super.toString();
    }
}
