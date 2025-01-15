# report_comparison

This takes two CSVs of the expected report type. They are expected as new.csv and old.csv but can be changed in the config at the top of the diffy file. This will import the files, put them into tables, create a header and label table, and then create a difference table of records that do not match between the new table and the old table.

The easiest way to look at the records is 'select * from Differences order by index, source'
