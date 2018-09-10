PS D:\BB2> .\wlgen.exe -h

Usage: D:\BB2\wlgen.exe [OPTION]...

Generate data for WLBench

General options:

-s <n> -- set scale factor to <n> (default: 1.0)
 
-j     -- output clicks table in JSON format

-r <n> -- exclusively run thread number <n>

-M     -- do not merge the resulting files

-h     -- display this message

Options to print specific tables:

-C     -- print the customers table

-I     -- print the items table

-L     -- print the clicks table

-W     -- print the web sales table

-S     -- print the store sales table

-P     -- print the web pages table

-T     -- print the stores table

If none are specified, all tables will be printed


----------------------------------
 Generate data in text for scale factor 10
-----------------------------------
 .\wlgen.exe -s 10

  PS D:\BigBenchV2> .\wlgen.exe -s 10

number of stores in each GMT offset: 100

number of items: 1217

number of categories: 25

number of customers: 19900

generating items

generating inventory

generating clicks, web sales, and product reviews

Running thread 0

Running thread 1

Running thread 2
Running thread 3
Running thread 4
Running thread 5
Running thread 6
Running thread 7
merging review files
 
 ----------------------------------
 Generate data in json for scale factor 10
-----------------------------------
 .\wlgen.exe -s 10 -j
 
PS D:\BigBenchV2> .\wlgen.exe -s 10 -j
number of stores in each GMT offset: 100
number of items: 1217
number of categories: 25
number of customers: 19900
generating items
generating inventory
generating clicks, web sales, and product reviews
Running thread 0
Running thread 1
Running thread 2
Running thread 3
Running thread 4
Running thread 5
Running thread 6
Running thread 7
merging review files
