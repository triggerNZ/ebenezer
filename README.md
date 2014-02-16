ebenezer
========

```
Scalding and Cascading support for using scrooge with parquet.
```

Parquet Integrations
--------------------

Lets just say they are horribly complex. And even more so when you are dealing with
things like thift/scrooge that don't easily map onto the underlying structure.

From top to bottom we have these components:

```
*Source                 -- Scalding integration

*Scheme                 -- Cascading integration

*{Input,Output}Formats  -- Hadoop integration, note that there are 4 provided versions,
                           ParquetInputFormat, ParquetOutputFormat, DeprecatedParquetInputFormat and
                           DeprecatedParquetOutputFormat. There Deprecated* versions are for Hadoop
                           1.x, the other versions are for Hadoop 2.x. However, there are some
                           configuration mechanisms that are done under ParquetInputFormat/
                           ParquetOutputFormat that are for _both_ the 1.x and 2.x versions (for
                           example, setReadSupport).

{Read,Write}Support     -- Used by the {Deprecated}Parquet{Input,Output}Formats for customization of
                           record reading and writing.

*{Read,Write}Support    -- Implementations of {Read,Write}Support for a specific integration.

*RecordMaterializer     -- Top level of ReadSupport responsible for reading all records via a
                           RecordConverter

*RecordConverter        -- Conver one parquet record into one target record.

*StructConverter        -- Map integration library structs (e.g. Thrift) onto the Parquet schema,
                           used in both read and write. Closely related to *SchemaConverter just
                           at a different layer. *StructConverters are normally for intra format
                           conversion (i.e. thrift to parquet-thrift).

*SchemaConverter        -- Map integration library structs (e.g. Thrift) onto the Parquet schema,
                           used in both read and write. Closely related to *StructConverter just
                           at a different layer. *StructConverters are normally for inter format
                           conversion (i.e. parquet-thrift to raw parquet).

*WriteProtocol          -- Does actual conversion from data structure to bytes.

*ReadProtocol           -- Does actual conversion from byte to data structure.

```

This is a lot of things that fit together to do not very much. But they are all essential to
getting something integrated with parquet. The parquet-mr project already provides many
integrations, and in practice you can leverage those quite heavily. For example, the scrooge
support in ebenezer works on top of the parquet-thrift integration and doesn't start from
scratch.

The integrations included in ebenezer are largely based on code structures from the parquet-mr
project (and its submodules). For example the scrooge integration closely matches what exists
for the libthrift integration included in parquet-mr.

NOTE: parquet-mr includes a scrooge integration but it is not complete (no write support) and
      will unlikely ever be generally useable as there is no infrastructure for doing cross-compiles
      and other essentials for existing in a scala world.

Introspection
-------------

The purpose of the introspection integration is to be able to read files in a parquet format
with an unknown structure.

The core of introspection is the `com.cba.omnia.ebenezer.introspect.Record` data type. It
represents a single parquet record of any schema.

The most common use of the introspection is for simple command line tools like a `cat` like
command that will stream the contents of parquet file (but in a readable way). An example
of this is included in ebenezer, see `com.cba.ombnia.ebenezer.cli.Cat`.

The introspection integration is somewhat straight forward, the most complicated part are
the `Introsepction*Converter` types which are used to recursively decode the parquet structure.
To understand the converters, the entry point is `IntrospectionRecordConverter`. which matches
out enums, primitives, lists, maps and nested records. Enum and Primitive converters do just
what they say on the box, but List and Map converters will match on their internal structure
and may loop back onto any of the other converters.


Scrooge
-------

The purpose of the scrooge integration is to be able to read files in a parquet format
with a structure defined by Scrooge generate Struct/Codecs.

The most straight forward way to do this is via `scalding` with the `ParquetScroogeSource`.
This source, can be used for both reading and writing with a `TypedPipe`. The test cases in
`com.cba.omnia.ebenezer.scrooge.ParquetScroogeSourceSpec` provide reasonable examples for
doing so.

A rough mud map of the integration is (note that `*` indicated parquet-mr provided components):

High level:

```

      [ ParquetScroogeSource: Scalding integration ]
                          |
                          | (depends on)
                          v
      [ ParquetScroogeScheme: Cascading integration ] - (inherits from) -> [ ParquetValueScheme*: Base parquet-cascading integration ]
           |                                                                                                      |
           | (uses for the heavy lifting)                                         (configures read/write support) |
           |                                                                                                      |
           |-  [ DeprecatedParquetOutputFormat* ]--- (configured via ) -----> [ DeprecatedParquetOutputFormat* ] -|
           \-  [ DeprecatedParquetInputFormat* ] --- (configured via ) -----> [ ParquetInputFormat* ]            -/


```

Read pipeline (driven by `DeprecatedParquetInputFormat`):

```
      [ ScroogeReadSupport: Wires together read pipeline ]--- (configured by) ---> [ ScroogeReadWriteSupport: holds thrift struct class name ]
                        |       \
                        |        \ (Builds a parquet-thrift StructType via a scrooge.ThriftStructCodec)
      (creates to read  |         \
         all records)   |          \- [ ScroogeStructConverter ]
                        |          /
                        v         / (Output StructType is used by Materializer/Converter to read records)a
      [ ScroogeRecordMaterializer ]
                  |
                  | (creates to read each record, via the ThriftStructCodec and the parquet-thrift integration)
                  v
      [ ScroogeRecordConverter ]
                  |
                  | (relies on parquet-thrift integration for everything _except_ decoding the actual bytes)
                  v
      [ ThriftRecordConverter* ]

```

Write pipeline (driven by `DeprecatedParquetOutputFormat`):

```
      [ ScroogeWriteSupport: Wires together write pipeline ]--- (configured by) ---> [ ScroogeReadWriteSupport: holds thrift struct class name ]
                        |       \
                        |        \ (Builds a parquet-thrift StructType via a scrooge.ThriftStructCodec)
      (creates to write |         \
         all records)   |          \- [ ScroogeStructConverter ]
                        |          |
                        |          | (Uses the parquet-thrift StructType to build a parquet MessageType)
                        |          |
                        |          /- [ ThriftSchemaConverter ]
                        |         /
                        |        / (Output StructType and MessageType is used by parquet ColumnIO / ParquetWriteProtocol.
                        v       /
      [ ParquetWriteProtocol*: Does actual writing of records ] --- (wraps) --> [ ColumnIO*: manages read / write with column schema  }
```

Future
------

 - Add sbt-assembly and a shell exec wrapper around cli tools so they are easier to deploy.
 - Add more interesting introspection / query tools.
 - Add a more direct way of writing data (at the moment, reads can happen via MR or directly,
   but writes always go via MR).
 - Push down stream changes into scrooge which would help remove some of the reflection code
   used to interogate the generated ThriftCodec.
