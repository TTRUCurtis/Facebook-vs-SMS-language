Keyboard_Data_PII_Removal.py

Coded by Douglas Bellew for
NIDA (a division of NIH) under PI Dr. Brenda Curtis
supervised by Salvatore Giorgi

PURPOSE:

This file was designed to remove Personally Identifiable Information (PII) from AWARE Keyboard data.  
Since the program acts as a keylogger (including timestamp for each keypress) it captures everything that
the user inputs for all of their applications, which may include data which would not be for public 
distribution (Proper Names, Social Security Numbers, Credit Card Numbers, Passwords, Phone Numbers, etc.) 
We needed to remove this data, while still retaining the context.  We do this by replacing the PII with 
a tag indicating what the data was. (ex: "John Smith" would be replaced with <NAME>, "5555-5555-5555-5555"
would be replased with <CREDIT CARD NUMBER>)

USAGE:

The first thing you will need is a python environment which contains pandas, sqlalchemy and spacy.  I have included
a conda package list under "conda_env_list_file.txt" and a location explicit file (you can use this directly if you have
ubuntu 18) under "conda_env_list_explicit_file.txt".  

You will also need a MYSQL 5 database and tables to be on the same machine as the code execution and accessable for 
automated login using .my.cnf credentials.  Note: It might possible to use a remote machine (by changing 127.0.0.1 
to the remote machine name), but that wasn't tested.

A basic run of Keyboard_Data_PII_Removal.py is just:

python Keyboard_Data_PII_Removal.py

This will execute the code, however, if you look at the code itself, you will notice there are lots of things you can change.  
Some of these you probably won't want to change and were included because requirements were changing rapidly as development 
was continuing (See "OTHER PARAMETERS" for details) but some are required.

Here are the ones that you will need or want to change.
    "mysqlconfig" The location of your .my.cnf file to allow for access to the mysql database
    "database_name" The name of the database containing your data
    "source_table"  You will need to update this with the table name of your uncleaned AWARE keyboard data
    "transition_table" This is the destination for the cleaned data, a modified version of the basic AWARE keyboard table
    "complete_row_table" This is the destination for only the complete rows of keyboard data. (In case you're not looking 
                         specifically for keystroke data) 
    "batch_fetch" This is the amount of rows to pull from the database at a time for processing
    "fetch_count" This is the maximum amount of rows to pull from the database for this run.
    "last_processed_id" This is a row offset into the data table to begin pulling data from (to allow for multiple sequential runs)
    "replace_database_tables" This drops the destination tables (if they exist) before writing any data.  If you are appending data to an
                              existing table, set this to False 

All parameters can be changed on the command line.  Using: 

"python Keyboard_Data_PII_Removal.py --help" 

will give you a usage containing examples all available switches and data parameters

When running this program on the machine I had access to, due to the way Pandas deals with changing the size of dataframes, the program
would slow down dramatically after processing 100K rows.  (100K would take 4-6 hrs, 200K would take 20-24 hrs) This was a problem for 16MM
rows of data.   I dealt with this by using a virtual terminal (on ubuntu I would use screen) and would run 5 200K runs each day.  I would
create a directory with all of the files for each run (labeled for the row count, i.e. "Keyboard_Data_PII_Removal_10_0M_10_2_M", 
"Keyboard_Data_PII_Removal_10_2M_10_4_M" etc.) and then in each directory, alter the "transition_table" "complete_row_table" fields to write to
different table names (with the proper row segment attached to keep the processes from overwriting each other) and "last_processed_id" to 
be the proper starting row.  Your machine power may vary, so you can try some different values if you want.
    NOTE: As the code is not thread safe, running multiple versions of the program and saving to the same table can either fail or give
    non-deterministic results.  When running multiple instances, always save to different tables.
    
After the runs would complete, I would the check each table to see if the amount of rows returned matched the expected row count 
(sometimes data would fail and there would be skipped rows) and the first 10/last 10 rows of each file to make sure they weren't 
corrupted. Once sanity checks were complete, I would concatenate the 5 transition tables and 5 final row tables together, into a 
single 1MM count file. Then I would update the directory names, "transition_table" "complete_row_table" and "last_processed_id" fields
to the next 1MM count, then restart for the next 24 hr run.  

Then once all runs were complete, I concatenated all the 1MM row files together to create a final cleaned file.

If you want to have "non-technical" people doing the file runs, you can set the "cfg_file" in the code and set the parameters you want
to override in the config file (see Example_Keyboard_Data_PII_Removal_Options.json).

PROCESSING:

Tag Replacemnt takes place in 2 passes.  The first pass is a forward pass where each row is subjected to spaCy NER, as well as
interrogation by the patterns in the commonregex file.  Tag replacment segments are created for each row.  This continues until 
a "complete" row is detected, at which point a backwards pass is done. Using the matches from the last row as the "most complete" 
information for each row, we pass those tag replacement segments backwards for each row (we now know that "J" was actually just 
an incomplete "John" so it needs to be replaced) and combine the row-specific tags with the final tags (combining any tags that
overlap - so you might see something like "NAME OR GPE" as a tag) and replacing the segment data with the new TAG.  

OTHER PARAMETERS:

Other changeable parameters:
"debug_level" Setting this to 1 will print out A LOT of information about what is running and being changed.
"ping_count" Gives a heartbeat every "x" processed rows
"exclude_dict"  This is a list of tags that we do not want to replace.  In testing, the only spaCy tag that we found 
                that we didn't want systematically change was the "DATE" flag, so we exclude that from substitution
"rename_dict" This changes the returned spaCy tags to other tag names (more useful for our data processing)
"commit_to_db" Setting this to False keeps the program from writing data to the database.
"write_to_csv"  Setting this to True writes out a csv file of pre and post change data (useful for 3rd party checking 
                to see if your replacements are acceptable)
"batches_complete" You can use this to start ("batches_complete" * "batch_fetch" + last_processed_id) rows into the database.
                   Was used for auto-recovery from crashes.  For a human, it's probably easier to just update last_processed_id 
                   to what you want.
"cfg_file"  This is the path name (can be relative, i.e. "./filename" ) to the configuration override file. If you don't want to 
            change code while doing multiple runs, you can use this file instead to make changes.  (see Example_Keyboard_Data_PII_Removal_Options.json)
"recover_file" This will allow for some automated crash recovery. (This file will contain an updated "last_processed_id" so you can just restart)  
               Can be more hassle than it's worth if your data is okay, as if it's not set to "", it will use the value in this file when the program starts
