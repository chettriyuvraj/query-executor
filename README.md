# README

## Query Executor

### Plan

- So I went over the Postgres Query executor source code a number of times and ended up with some loose thoughts (very loose :P):
- PlanNode interface (iterator)
    - Scan interface
        - TableScan
        - ObjectScan
    - Limit
- QueryExecutor a sep obj will have a number of methods

### Thoughts post implementation

- Am I thinking about this the right way? I watched the lecture after implementation and feel like the direction is probably ok.
- Do the abstractions I have come with with make sense? I don't really have much experience with OOP and the structs and interfaces I came up with were mostly what _felt right_ to me. 
- What can I do to feel more confident in terms of OOP and the overall structure? Are there resources, books, patterns? Not really sure. The C implementation felt very VAST!


## Custom Data Format

Q: Answering questions from the prework instructions, lets try to be very minimal with requirements and then try to relax them.

A: A fairly strict set of criteria:
- What does the data look like? Fixed-width string values comprising of 'rows' or 'tuples'
- How will it be accessed? random access must be possible
- Can we generate the files in a streaming manner? If not, what data do we need beforehand (e.g. number of rows, contents of rows)? We can generate it in a streaming manner
- Can we change the files after they’ve been written, or are they immutable? Immutable
- Should the format be optimized for writes or reads? Can we support both efficiently? We want to be efficient at reading - our purpose is query execution
- Do the files need to be “self-describing”, or can the schema be stored somewhere else? Let's have the schema within the file itself
- Should the files be divided into “chunks” or sections? Chunks - we want to optimize for random access, this would be a step in that direction, right (?)
- Will the data be compressed? How? Does that have any implications for our ability to write and read the files? No compression
- Do we need to guard against data corruption due to bit rot? How much should we do so? No consideration for bit rot


Q: Why would JSON be a good/bad choice for storage?
A: 
- Bad choice as:
- Would require large amount of parsing? In general a bad choice for storing database data as even though we might store some of the actual 'data' in pages as UTF-8, remaining things are not UTF-8 and we can interpret them as is.
- The byte-arithmetic that we do to determine things such as offset of a tuple in a binary encoding would become be difficult to do in JSON
- The metadata would be stored in JSON too, isn't this unnecessary? The to-and-fro parsing when it can simply be stored in binary as-is, in a predefined order and interpreted
- Would increase the storage size
- Limited data types (?)


Q: Now let's think about a simple binary format, with our set of constrained features
A: 
Thoughts:
- Fixed width data: Since we are assuming fixed-width data, we wouldn't need info on lengths. We have a set of predefined formats and they will occupy that space for sure eg. all strings = 64 bytes. This means we don't need to store any length metadata for fields. In our case we are considering only string data, so let's assume fixed width fields
- Self-contained metadata: Unlike postgres, we don't want a separate schema to store metadata about our data. We want a sort of header at the start of the file indicating which table the data belongs to + fields in that table. But how would we know which file contains data for which page if all the metadata is self-contained....hmmm
- Supporting random access: What does random access mean in this context? Is it like a pointer and slot concept where there exists a directory at the start of page that points to records? We would need some sort of offset into for records then at the start/end whereever
- Storing as chunks: Each chunk would either have to be fixed length or if not then length info would have to be stored for them. Since we are considering fixed width data, fixed-length chunks should work
- Optimizing for reads? Fixed length chunks themselves would lead to fast reads (?)

Design:
- All our data is string and fixed width, let's have 3 diff types of strings, distinguished only by their widths
    - ss: 16 bytes (string short)
    - sm: 32 bytes (string medium)
    - sl: 64 bytes (string large)
- Our format makes it easy to support random access, because of the fixed-width fields, any random access to ith record is a simple matter of arithmetic
- In essence, they are chunks
- Completely self contained is difficult. Lets assume all files will be stored in the same (current) directory, name of file will be same as name of directory, and all pages will be in a single file. 8KB pages, so again distinguishing between pages will be simple arithmetic
- De-facto header:  
    - magic number 4 bytes
    - Reserve 8 bytes at the start for the number of records filled so far
    - 1 byte for number of fields
    - Next 1 byte * number of field bits for indicating their type (00 ss, 01 sm, 10 sl) (not very efficient)
    - First record indicates the column names, all of type sl (always)
    - next 'n' records are the actual tuples
- We aren't considering the problem of capping a file size as of now (eg. pages must always be completely within a file)



## Plan

- Let's have an underlying struct YcFile represent the file. Fields:
    - tableName representing the table name
    - file underlying os file - if not nil then it is open
    - Write method()
        - calls open first
        - validates the byte data depending on the table, fields etc (must be one complete tuple, anything greater not written)
        - finally writes 
    - Open() first checks if file is null, checks assets folder if file with tableName exists, if not then creates and generates header, sets file field to opened file
    - validate()
    - Read() - similar to write, call open, then read


- YcFileWriter
    - 



