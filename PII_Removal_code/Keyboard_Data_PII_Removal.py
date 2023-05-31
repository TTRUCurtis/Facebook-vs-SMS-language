#
# note: To run profiling on any of the functions below, uncomment the @profile and run the following item:
# script -c "kernprof -l -v  Keyboard_Data_PII_Removal.py" /dev/null | tee ./CET_Profile.txt
#  
import sys
import os
import datetime as dt
import getopt
from pathlib import Path
import sqlalchemy as db
import pandas as pd
import spacy
import re
import json

# NOTE if commonregex is broken
# The standard pip install version of commonregex is old.  The newer version containing more regexes is here:
# https://github.com/madisonmay/CommonRegex  on the main page, However, for this project, we had to augment the
# regex list with new categories.  Use the local verison.
import commonregex as Crx

#@profile
def main():

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", ["exclude", "rename"])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)
        
    startTime = dt.datetime.now()
    prev_ping_time = dt.datetime.now()
    prev_batch_time = dt.datetime.now()
    completed_row_id = 0
    # SET RUN DEFAULTS
    options = {}
    options["debug_level"] = 0
    options["exclude_dict"] = {"DATE":True}
    options["rename_dict"] = {"DATE_STRING_MATCH":"DATE",
                              "TIME_STRING_MATCH":"TIME",
                              "PHONE_NUMBER_STRING_MATCH": "PHONE_NUMBER",
                              "URL_SRING_MATCH":"URL",
                              "EMAIL_ADDRESS_STRING_MATCH":"EMAIL_ADDRESS",
                              "MONEY_STRING_MATCH":"MONEY"}
    options["commit_to_db"] = True
#    options["write_to_csv"] = str(str(Path.home()) + "/pii_tagged_removed_data_"+dt.datetime.now().strftime("%Y%m%d%H%M%S")+".csv")
    options["write_to_csv"] = ""
    options["mysqlconfig"] = "<FULL_PATH_TO_DOTMYDOTCNF_MYSQL_LOGIN_CONFIG_FILE>"
#    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["database_name"] = "<REPLACE_WITH_DATABASE_NAME>"
    options["source_table"] = '<REPLACE_WITH_SOURCE_DATA_TABLE>'
    options["transition_table"] = "<REPLACE_WITH_TABLE_NAME_FOR_TABLE_WITH_ALL_CLEANED_ROWS>"
    options["complete_row_table"] = "<REPLACE_WITH_TABLE_NAME_FOR_TABLE_WITH_COMPLETE_ROWS_ONLY>"
    #Set option to True if you want to replace the destination tables if they exist, or set to False if you only want to append (i.e. you're
    # adding more data into a previously completed run).
    #Note, after the first batch, this is always False so subsequent batch saves don't overwrite the previous ones 
    options["replace_database_tables"] = True
    #This is the row in the database to start pulling data from (0 based)
    #For runs after the first, you'll want to increase this, generally by "fetch_count" * number of runs. (So run 2 will start at "fetch_count",
    # run 3 will start at "fetch_count" * 2, run 4 will start at "fetch_count" * 3, etc.)
    options["last_processed_id"] = 0
    # How many batches have been completed in this run
    options["batches_complete"] = 0
    # How many rows should you process from the database at a single time. (Note, the first row of a batch is always marked as "complete")
    options["batch_fetch"] = 20000
    # How many total rows to process for this run
    options["fetch_count"] = 100000
    # Print out a "heartbeat" ping every ping_count rows
    options["ping_count"] = 2500
    # If you want to override 
    options["cfg_file"] = ""
    #options["cfg_file"] = "./Keyboard_Data_PII_Removal_Options.json"
    options["recover_file"] = ""
    #options["recover_file"] = "./Keyboard_Data_PII_Removal_Recover_File.json"
    
    # Interesting segments in the data: 50000-52500 contains an email.  WIll try and use this section for optimization since it parses into like 20 tags.
    # Possible larger segments: 47500 - 57500
    #                           75000 - 85000
    # Individual Rollback test cases: 
    #               132, 133, 428,  should stay because the next row does not have more characters (deletion or same)
    #               206, 244, 457, 476, 541 (Person Only), 629, 726, 728, 764, 766, 940, 973, 1072 should be removed
    
    # OVERRIDE GENERAL DEFAULTS WITH COMMAND LINE ARGUMENTS
    for option_tuple in optlist:
        if (option_tuple[0] == "--exclude"):
            for exclude_list_item in option_tuple[1].split(","):
                options["exclude_dict"][exclude_list_item] = True
        elif (option_tuple[0] == "--rename"):
            for rename_list_item in option_tuple[1].split(","):
                if (len(rename_list_item.split(":")) == 2):
                    orig_tag, rename_tag = rename_list_item.split(":")
                    options["rename_dict"][orig_tag] = rename_tag
        elif (option_tuple[0] == "--debug"):
            options["debug_level"] = 1
        elif (option_tuple[0] == "--replaceDb"):
            options_["replace_database_tables"] = True            
        elif (option_tuple[0] == "--noDbCommit"):
            options_["commit_to_db"] = False
        elif (option_tuple[0] == "--lastProcessedId"):
            options["last_processed_id"] = option_tuple[1]
        elif (option_tuple[0] == "--getRowCount"):
            options["fetch_count"] = option_tuple[1]
        elif (option_tuple[0] == "--pingRowCount"):
            options["ping_count"] = option_tuple[1]
        elif (option_tuple[0] == "--batchCount"):
            options["batches_complete"] = option_tuple[1]
        elif (option_tuple[0] == "--batchRowCount"):
            options["batch_fetch"] = option_tuple[1]              
        elif (option_tuple[0] == "--write_to_csv"):
            options["write_to_csv"] = option_tuple[1]
        elif (option_tuple[0] == "--mysqlconfig"):
            options["mysqlconfig"] = option_tuple[1]
        elif (option_tuple[0] == "--recoverFile"):
            options["recover_file"] = option_tuple[1]
        elif (option_tuple[0] == "--cfgFile"):
            options["cfg_file"] = option_tuple[1]                      

    # OVERRIDE COMMAND LINE ARGUMENTS WITH CONFIG FILE ARGUMENTS
    if (options["cfg_file"] != ""):
        if (os.path.exists(options["cfg_file"])):
            try:
                with open(options["cfg_file"], encoding = 'utf-8') as f:
                    cfg_json_text = f.read(None) 
            except:
                print("Failed to read Config file:"+str(options["cfg_file"]))
                sys.exit(2)
            cfg_json = json.loads(cfg_json_text)
            for opt_key in cfg_json.keys():
                options[opt_key] = cfg_json[opt_key]
        else:
            print("Expected External Config file:"+str(options["cfg_file"])+" not found.")
            usage()
            sys.exit(2)
            
    # OVERRIDE CONFIG FILE WITH RECOVER FILE
    if (options["recover_file"] != ""):
        if (os.path.exists(options["recover_file"])):
            try:
                with open(options["recover_file"], encoding = 'utf-8') as f:
                    recover_json_text = f.read(None)
                recover_json = json.loads(recover_json_text)
                for opt_key in recover_json.keys():
                    options[opt_key] = recover_json[opt_key]
            except:
                print("Failed to read Recovery File:"+str(options["recover_file"]))                
        else:
            print("Expected Recover file:"+str(options["recover_file"])+" not found.")
            print("Will create new one at specified location.")
            
    # chokes on "localhost" (Possible remote ssh issue?).  Force Connection type by using Loopback IP address.
    myDB = db.engine.url.URL(drivername="mysql",
                            host="127.0.0.1",
                            database = options["database_name"],
                            query={"read_default_file" : options["mysqlconfig"]})

    engine = db.create_engine(name_or_url=myDB, pool_pre_ping=True)            
   

    # load up the nlp data
    nlp = spacy.load('en_core_web_lg')
    matcher = spacy.matcher.Matcher(nlp.vocab)  
      
    # Pull data from the DB
    # Note: .limit(XXXXX) is there so that I don't have to wait 45 seconds each time I try and test,
    #       will update and remove later when want the whole table. 
    #query = db.select([orig_data])
    data_remaining = True  
    while (((options["batches_complete"] * options["batch_fetch"]) < options["fetch_count"]) and data_remaining):
        result_set = {}
        with engine.connect() as connection:
            metadata  = db.MetaData()
            #orig_data = db.Table('kb_test_data', metadata, autoload=True, autoload_with=engine)  
            orig_data = db.Table(str(options["source_table"]), metadata, autoload=True, autoload_with=engine)         
            query = db.select([orig_data]).where(orig_data.columns._id > options["last_processed_id"]).offset(options["batches_complete"] * options["batch_fetch"]).limit(options["batch_fetch"])
            result_proxy = connection.execute(query)
            result_set = result_proxy.fetchall()


#        print(repr(metadata.tables['kb_test_data']))
#        print(result_set[0:10])

        if (len(result_set) > 0):
        # Put Result into PANDAS dataframe
            pd.set_option("max_colwidth",30)
            pd.set_option("large_repr", "truncate")
            pd.set_option("display.width", None)
            orig_df = pd.DataFrame(result_set)
            orig_df.columns = result_set[0].keys()
            if (options["debug_level"] != 0):
                print("Retrieved "+ str(len(orig_df))+ " rows.")

            # add columns for result data
            orig_df.insert(6, "scrubbed_text", "", True )    
            orig_df.insert(len(orig_df.columns), "is_complete", 0, True)
            orig_df.insert(len(orig_df.columns), "has_changed", 0, True)
            orig_df.insert(len(orig_df.columns), "change_segments_text", "", True)    
            orig_df.insert(len(orig_df.columns), "change_segments", [[] for x in range(len(orig_df))], True)
        
            # create substitution dataframe
            # Try and pre-build the DF for the size so you don't end up recreating the Df for each append
            replacement_bundle = {}
            replacement_bundle["index"] = 0
            replacement_bundle["df"] = pd.DataFrame(data={"Replacement ID" : ["" for x in range(options["fetch_count"]*2)], 
                                                    "Replaced_Text" : ["" for x in range(options["fetch_count"]*2)]})

            # Interrogate data
            if ((len(orig_df) == 0) and (options["batches_complete"] == 0)):
                exit("No data in table")
            elif (len(orig_df) == 0):
                # Don't do any processing, just write cleanup files
                pass
            else:
                if (options["batches_complete"] > 0):     
                    batch_delta = dt.datetime.now() - prev_batch_time
                    prev_batch_time=dt.datetime.now()
                    print("Batch finish time: "+str(batch_delta))
                print("Processing Batch:"+str(options["batches_complete"])+" Row ID:"+str(orig_df.at[0,"_id"]))
                parse_single_row(orig_df, nlp, matcher, 0, options)
                if (len(orig_df) > 1):
                    for current_row in range(1,len(orig_df)):
                        if ((options["ping_count"] != 0) and
                            (current_row % options["ping_count"] == 0)):
                            ping_delta = dt.datetime.now() - prev_ping_time
                            prev_ping_time = dt.datetime.now() 
                            print("Processing Row:"+str(current_row + (options["batches_complete"] * options["batch_fetch"]))+
                                  " with ID: "+ str(orig_df["_id"][current_row])+","+ 
                                  str(ping_delta)+"s since last ping.")
                        parse_single_row(orig_df, nlp, matcher, current_row, options)                    
                        check_for_input_change(orig_df, current_row, current_row - 1)
                        do_token_change_work(orig_df, current_row)
                        if (orig_df.at[current_row-1, "is_complete"] == 1) :
                            scrub_rows_backwards(orig_df,current_row - 1, replacement_bundle, options )
                if (orig_df.at[len(orig_df)-1, "is_complete"] != 1) :        
                    orig_df.at[len(orig_df)-1, "is_complete"] = 1
                    scrub_rows_backwards(orig_df, len(orig_df)-1, replacement_bundle, options)
                
                # If Looping through, append (replace loop 0)
                if (options["replace_database_tables"] and options["batches_complete"] == 0):
                    db_if_exists = "replace"
                else:
                    db_if_exists = "append"

                if (options["commit_to_db"]):
                    temp_df = orig_df[["_id","timestamp", "device_id", "package_name", "before_text", "current_text", 
                                    "scrubbed_text", "is_password", "is_complete", "has_changed", "change_segments_text"]]
                    temp_df.to_sql(name=options["transition_table"], con=engine, if_exists=db_if_exists, index=False)
            
        #       Create the "scrubbed" data table with just the complete messages with PII removed.
                scrubbed_df = orig_df.loc[orig_df["is_complete"]==1, ["_id","timestamp", "device_id", "package_name", "scrubbed_text"]]
                if (options["commit_to_db"]):
                    scrubbed_df.to_sql(name=options["complete_row_table"], con=engine, if_exists=db_if_exists, index=False)


        #       Put partial data into recovery file
                options["batches_complete"] = options["batches_complete"] + 1
                completed_row_id = orig_df.at[len(orig_df)-1,"_id"]
                if (options["recover_file"] != ""):
                    try:
                        with open(options["recover_file"], "w",  encoding = 'utf-8') as f:
                            recover_options = {}
                            #NOTE: json.dump will produce invalid json for large integers
                            #unless you specifically cast them to <long> int() type.
                            recover_options["last_processed_id"] = int(completed_row_id)
                            recover_options["fetch_count"] = options["fetch_count"]
                            # Since we're updating the last processed id, each batch, we don't need to
                            # store the batches complete
                            #recover_options["batches_complete"] = options["batches_complete"]
                            recover_options["batch_fetch"] = options["batch_fetch"]                     
                            json.dump(recover_options, f)
                    except:
                        print("Failed to write Recovery File:"+str(options["recover_file"]))
                        print("Don't crash, but could indicate problems.")
                
    #   Dump data removed along with Tag to CSV file
            if (options["write_to_csv"] != ""):
                replacement_bundle["df"].iloc[:replacement_bundle["index"]].to_csv(path_or_buf=options["write_to_csv"], index=False)
    #       print(orig_df.iloc[0:10])
    #       print(orig_df.iloc[995:])                    
        else:
            data_remaining = False
        #end if (len>0) 
    #end while (have more batches)
    # At end of run, put the final row as "start row" to be read by the next run.    
    if (options["recover_file"] != ""):
        try:
            with open(options["recover_file"], "w",  encoding = 'utf-8') as f:
                recover_options = {}
                #NOTE: json.dump will produce invalid json for large integers
                #unless you specifically cast them to <long> int() type.                
                recover_options["last_processed_id"] = int(completed_row_id)                    
                json.dump(recover_options, f)
        except:
            print("Failed to write Recovery File:"+str(options["recover_file"]))
            print("Don't crash, but could indicate problems.")     

    
    print("Final Run time = :"+str(dt.datetime.now() - startTime)+". ")

def usage():
    print("python " + str(sys.argv[0]) + '--exclude "TAG,TAG,TAG,TAG" --rename "TAG:REPLACEMENT,TAG:REPLACEMENT" \
                                          --debug --noDbCommit --replaceDb --lastProcessedId ### --getRowCount ### --pingRowCount #### \
                                          --batchCount #### --batchRowCount #### \
                                          --writeToCsv <full-path-filename> --mysqlconfig <Full Path to .my.cnf equivalent> \
                                          --cfgFile <full-path-filename> --recoverFile <full-path-filename>')
    print("            Defaults: --exclude 'DATE' ")
    print("                      --rename 'DATE_STRING_MATCH:DATE,TIME_STRING_MATCH:TIME,PHONE_NUMBER_STRING_MATCH:PHONE_NUMBER,")
    print("                                URL_SRING_MATCH:URL,EMAIL_ADDRESS_STRING_MATCH:EMAIL_ADDRESS,MONEY_STRING_MATCH:MONEY'")
    print("                      --lastProcessedId 0 --getRowCount 100000 --pingRowCount 0 --batchCount 0 --batchRowCount 20000")
    print("                      --writeToCsv ~/pii_tagged_removed_data_YYYYMMDDHHMMSS.csv")
    print("                      --mySqlConfig ~/.my.cnf --cfgFile ./CompleteEntry_test.json")
    print("                      --recoverFile ''")  

   

#@profile    
def check_for_input_change(df, current_row, prev_row):
    # Easy Cases:
    # Different Device
    # Different Program
    # No previous input and the user hadn't backspaced to nothing
    if (df.at[current_row, "device_id"] != df.at[prev_row, "device_id"] or
        df.at[current_row, "package_name"] != df.at[prev_row, "package_name"] or
        (df.at[current_row, "before_text"] == "") and (df.at[prev_row, "current_text"] != "[]")):
            df.at[prev_row, "is_complete"] = 1
    # Edge Cases:
    # Catch new messages with non-blank "before_text" fields
    # Current text less than 8 (magic number) characters     
            # (try to remove false positives from autocorrect and full word deletes) 
    # Last text input more than 5 (magic number) plus '[]' characters longer than current text
    elif ((len(df.at[current_row, "before_text"]) < 8) and
          (len(df.at[current_row, "before_text"]) < len(df.at[prev_row, "current_text"]) - 7 )):
        df.at[prev_row, "is_complete"] = 1
    return

def remove_brackets(df,row_id):
    # if current_text_field not empty, remove the added brackets and move to scrubbed_text field
    # Not changing the content of the data, so don't set the "has_changed" field
    if (len(df.at[row_id, "current_text"]) >= 2):
        df.at[row_id, "scrubbed_text"] = df.at[row_id, "current_text"][1:-1]
    return

#@profile
def scrub_by_structural_data(df, row_id, options):
    # Easy Cases:
    # Substitute full text because of non-text clues
    # Detected Password Field        
    if ((df.at[row_id, "is_password"] == 1) and not options["exclude_dict"].get("PASSWORD", False)):
        df.at[row_id, "change_segments"][:0] = [("PASSWORD", 0, len(df.at[row_id, "scrubbed_text"]))]
        df.at[row_id, "scrubbed_text"] = options["rename_dict"].get("PASSWORD","{PASSWORD}")
        df.at[row_id, "has_changed"] = 1
    # Typing Number into Phone Dialer App
    elif ((df.at[row_id, "package_name"].find("dialer") >-1)  and not options["exclude_dict"].get("PHONE NUMBER", False)):
        df.at[row_id, "change_segments"][:0] = [("PHONE NUMBER", 0, len(df.at[row_id, "scrubbed_text"]))]
        df.at[row_id, "scrubbed_text"] = options["rename_dict"].get("PHONE NUMBER","{PHONE NUMBER}")
        df.at[row_id, "has_changed"] = 1    
    return

#@profile 
def create_scrub_segments(df, nlp, matcher, row_id, options):
    # Harder Cases
    # TODO: Implement spaCy.io NER - Looking to remove PII:
    #   Proper names, Personal ID #s (SSN, Drivers License #, etc.) @-mentions
    #   Time/Date Strings, URLS, CC#s, Phone Numbers 
    #print("Row before:"+str(row_id))     
    #print(df.at[row_id, "change_segments"])      
#    doc = nlp(df.at[row_id, "scrubbed_text"])

#   Add Pattern for Scrubbing URLs
#   Note: Whatever is in the .add statement ("URL") is what will replace the item in the string
    if (not options["exclude_dict"].get("URL", False)):
        pattern = [{"LIKE_URL":True}]
        matcher.add("URL", [pattern])
#   Add Pattern for Scrubbing Emails
    if (not options["exclude_dict"].get("EMAIL_ADDRESS", False)):
        pattern = [{"LIKE_EMAIL":True}]
        matcher.add("EMAIL_ADDRESS", [pattern])    
         
    doc = nlp(df.at[row_id, "scrubbed_text"]) 
#   Use reversed so that you're not modifying character positions for
#   and other entities in the same item. 
    for ent in reversed(doc.ents):
#        May need to eventually create a subset of Replacement items, if so create list here            
        if (not options["exclude_dict"].get(ent.label_, False)):
            df.at[row_id, "change_segments"].append((options["rename_dict"].get(ent.label_,ent.label_), ent.start_char, ent.end_char))
    
    matches = matcher(doc)
    for match_id, start_token, end_token in reversed(matches):
        string_id = nlp.vocab.strings[match_id] # Retreive the Match name
        if (not options["exclude_dict"].get(string_id, False)):
            span = doc[start_token:end_token]  # The matched span text
            start_char = df.at[row_id, "scrubbed_text"].rfind(span.text)
            if (start_char > -1):
                df.at[row_id, "change_segments"].append((options["rename_dict"].get(string_id,string_id), start_char, start_char + len(span.text)))  
    #print("Row after:"+str(row_id))      
    #print(df.at[row_id, "change_segments"])
    # Search for common syntax for things
    # NOTE: If some of the regex's break, see NOTE at the 
    # import commonregex statement above
    string_match_func_list = [("DATE_STRING_MATCH", Crx.date.finditer),
                              ("TIME_STRING_MATCH", Crx.date.finditer),
                              ("PHONE_NUMBER_STRING_MATCH", Crx.phone.finditer),
                              ("PHONE_NUMBER_STRING_MATCH", Crx.phones_with_exts.finditer),
                              ("URL_SRING_MATCH", Crx.link.finditer),
                              ("EMAIL_ADDRESS_STRING_MATCH", Crx.email.finditer),
                              ("IP ADDRESS", Crx.ip.finditer),                              
                              ("IP ADDRESS", Crx.ipv6.finditer),
                              ("MONEY_STRING_MATCH", Crx.price.finditer),
                              ("CREDIT CARD NUMBER", Crx.credit_card.finditer),
                              ("BITCOIN ADDRESS", Crx.btc_address.finditer),
                              ("STREET ADDRESS", Crx.street_address.finditer),
                              ("ZIP CODE", Crx.zip_code.finditer),
                              ("PO BOX", Crx.po_box.finditer),
                              ("SSN", Crx.ssn.finditer),
                              ("@_SYMBOL", Crx.at_symbol_data.finditer),
                              ("GPE", Crx.state_2char.finditer),
                              ("GPE", Crx.state_abbr.finditer)
                              ]
    for string_match_func in string_match_func_list:
        if (not options["exclude_dict"].get(string_match_func[0], False)):    
            for match_item in string_match_func[1](df.at[row_id, "scrubbed_text"]):
                df.at[row_id, "change_segments"].append((options["rename_dict"].get(string_match_func[0],string_match_func[0]), 
                                                         match_item.start(), match_item.end()))
    return

#@profile
def parse_single_row(df, nlp, matcher, row_id, options):
    # Do single row structural scrubbing items for the forward pass
    remove_brackets(df, row_id)
    scrub_by_structural_data(df,row_id, options)
    # Structural Data matches already remove the entire string.  Don't bother interrogating.
    if (df.at[row_id, "has_changed"] != 1):
        create_scrub_segments(df, nlp, matcher, row_id, options)    
    return

def do_token_change_work(df, row_id):
    
    #We have a completed token on the previous line for the following cases:
    #   1) Previous row was complete
    #   2) Previous row was as long or longer than the current row (deletions or autocorrects)
    #   3) Current Row key entry is a delimiter (currently " ") and previous row has less charcters than the
    #      current row.
    #   4) We reached the end of the dataframe
    start_char = 0
    end_char = 0
    data_row = row_id
#    print("Checking Row: "+ str(data_row+1))
    if ((df.at[row_id - 1, "is_complete"] == 1) or
        (len(df.at[row_id,"scrubbed_text"]) <= len(df.at[row_id - 1, "scrubbed_text"]))):
        # If previous row was complete or current row is shorter than the preivous row, set the
        # end character to the end of the previous string
#        print("Found Complete")
#        print (str(len(df.at[row_id,"scrubbed_text"]))+","+str(len(df.at[row_id - 1, "scrubbed_text"])))        
        end_char = len(df.at[row_id - 1, "scrubbed_text"]) 
        data_row = row_id - 1
    elif ((df.at[row_id, "scrubbed_text"][-1:] == " ") and
          (len(df.at[row_id - 1, "scrubbed_text"]) < len(df.at[row_id,"scrubbed_text"]))):
        # If current row is end of token by " ", set end character to length of this string - 1
#        print("Found Space")
        end_char = len(df.at[row_id, "scrubbed_text"]) - 1
        data_row = row_id
    elif (len(df) - 1 == row_id):
#        print("Found Dataframe End")
        end_char = len(df.at[row_id, "scrubbed_text"])
        data_row = row_id        
    else:
        # Not a token change, return
        return
            

    # We don't want to include the token change indicator row in our backwards passing
    curr_row = data_row
#    print("Using data in row:"+str(data_row+1)+ " label: ["+df.at[data_row, "scrubbed_text"]+"]")
    token_break = False
    start_char = 0    
    while (not token_break):
        #print("Looking backwards for token change for row:"+str(curr_row)) 
        # Backwards looking cases for token break:
        # 1) No more backwards rows
        # 2) Previous row is another message
        # 3) Previous row has 0 characters (deleted back to nothing)
        # 4) Previous row ends has the same or more character than the current row 
        #      (deletion or substition, need to keep in case deleted PII)
        # 5) Previous rown ends in a " " and has less characters than current row       
        if  (curr_row - 1 < 0):
            token_break = True
        elif (df.at[curr_row - 1, "is_complete"] == 1): 
            token_break = True
        elif (len(df.at[curr_row - 1, "scrubbed_text"]) == 0): 
            token_break = True            
        elif (len(df.at[curr_row,"scrubbed_text"]) <= len(df.at[curr_row - 1, "scrubbed_text"])):
            start_char = len(df.at[curr_row, "scrubbed_text"])
            token_break = True           
        elif ((df.at[curr_row - 1, "scrubbed_text"][-1:] == " ") and
              (len(df.at[curr_row - 1, "scrubbed_text"]) < len(df.at[curr_row,"scrubbed_text"]))):
            start_char = len(df.at[curr_row - 1, "scrubbed_text"])
            token_break = True
        else:
            curr_row = curr_row  - 1        
          
#    print("Found complete token in row:"+str(data_row+1)+ " characters:"+str(start_char)+" to "+str(end_char) + " label: ["+df.at[data_row, "scrubbed_text"][start_char:end_char]+"]")
    # Check to see if a segment has been created for a completed token.
    # If so, propagate that backwards to the next token break.
    # If not, check backwards that there are no segments for this token
    apply_segments = []
    for segment in df.at[data_row, "change_segments"]:
        if (((segment[1] >= start_char) and (segment[1] <= end_char)) or
            ((segment[2] >= start_char) and (segment[2] <= end_char))):
#            print("Found a segment to roll back: "+str(segment))
            apply_segments.append(segment)
#    print("Scrub tokens in range of:"+str(curr_row+1)+" to "+str(data_row+1))
    for mod_row in range(curr_row, data_row):
        scrub_token_backwards(df, apply_segments, mod_row, start_char, end_char)
#        print(df.at[mod_row, "change_segments"])
    return
     
def segment_contained_in(check_segment, base_segment ):
    if ((check_segment[1] >= base_segment[1]) and
        (check_segment[1] <= base_segment[2]) and
        (check_segment[2] >= base_segment[1]) and
        (check_segment[2] <= base_segment[2])):
        return True
        
    return False

def segment_overlaps(check_segment, base_segment):
    if (((check_segment[1] >= base_segment[1]) and
         (check_segment[1] <= base_segment[2]) and
         (check_segment[2] > base_segment[2])) or
        ((check_segment[1] < base_segment[1]) and
         (check_segment[2] >= base_segment[1]) and
         (check_segment[2] <= base_segment[2]))):
        return True
        
    return False

def scrub_token_backwards(df, apply_segments, row_id, start_char, end_char) :
    # Find all possibly affected segments
#    print("Scrub tokens on row:"+str(row_id+1)+" between chars:"+str(start_char)+" and "+str(end_char))
    mod_segments =[]
#    print("Check segments for row:"+str(row_id+1))
    for segment_id in range(len(df.at[row_id, "change_segments"])):
        if (segment_overlaps(df.at[row_id, "change_segments"][segment_id], (" ", start_char, end_char)) or
            segment_contained_in(df.at[row_id, "change_segments"][segment_id], (" ", start_char, end_char))):
                mod_segments.append(segment_id)
#                print("Found local segment:"+str(df.at[row_id, "change_segments"][segment_id])) 
    # if no segments in overlay, remove segments.
    remove_segments = []
    append_segments = []
    if (len(mod_segments) == 0):
#        print("For row:"+str(row_id + 1)+" change segments from:"+str(df.at[row_id, "change_segments"]))
        df.at[row_id, "change_segments"] = df.at[row_id, "change_segments"] + apply_segments
#        print("                   to:"+str(df.at[row_id, "change_segments"]))
    else:
        for segment_num in mod_segments:
            segment_intersects = False
            segment_contained = False
            for apply_segment_num in range(len(apply_segments)):
                # item wholy contained in the apply item, replace it.
#                print("Compare local segment:"+str(df.at[row_id, "change_segments"][segment_num])+" with "+"overlay segment:"+str(apply_segments[apply_segment_num]))
                if (segment_contained_in(df.at[row_id, "change_segments"][segment_num], apply_segments[apply_segment_num])):
#                    print("Found Contained")
                    segment_contained = True
                    segment_intersects = True
                    append_segments.append(apply_segment_num)
                # if it's not contained, but overlaps beginning or end, leave it, it'll get
                # merged later                    
                elif (segment_overlaps(df.at[row_id, "change_segments"][segment_num], apply_segments[apply_segment_num])):
#                    print("Found Overlap")
                    segment_intersects = True
            if (not segment_intersects or segment_contained):
                remove_segments.append(segment_num)
            
        remove_segments = list(dict.fromkeys(remove_segments))
        remove_segments.sort(reverse=True) # make list unique & reverse order sorted
        append_segments = list(dict.fromkeys(append_segments))
        append_segments.sort(reverse=True)
#        print("Remove items:"+str(remove_segments)+", Add items:"+str(append_segments))
#        print("For row:"+str(row_id + 1)+" change segments from:"+str(df.at[row_id, "change_segments"]))        
        for i in remove_segments:
            del df.at[row_id, "change_segments"][i]
        for i in append_segments:
            df.at[row_id, "change_segments"] = df.at[row_id, "change_segments"] + [apply_segments[i]]
#        print("                   to:"+str(df.at[row_id, "change_segments"]))        
    return

def get_segment_start(item):
# sort on start position - tuple (name, start_char, end_char)
#    print("get_segement_start")
#    print(item)
    return item[1]

#@profile
def merge_change_segments(df, current_row, complete_row, options):
#   Concatenate the segment tuples of the current row with the segment tuples of the complete row,
#   making sure to remove duplicates and merge overlaps
#   tuple of the form (name, start character, end character)
    if (options["debug_level"] > 0):
        print("Merging current row:"+str(current_row)+" with complete row:"+str(complete_row))
        print("Current_list ="+str(df.at[current_row, "change_segments"]))
        print("Complete_list ="+str(df.at[complete_row, "change_segments"]))
        print("Length of change list: "+str(len(df.at[current_row, "change_segments"])))
        
    # First, check for duplicates in the current row, then check against the overlay row
    result_row = []
    for change_segment_number in range(len(df.at[current_row, "change_segments"])):
        duplicate_found = False
        for check_segment_number in range(change_segment_number + 1, len(df.at[current_row, "change_segments"])):
            if ((df.at[current_row, "change_segments"][change_segment_number][0] == df.at[current_row, "change_segments"][check_segment_number][0]) and
                (df.at[current_row, "change_segments"][change_segment_number][1] == df.at[current_row, "change_segments"][check_segment_number][1])):
                duplicate_found = True
                # Tuples are immutable. overwrite with with copy but with the max of the
                # two end items (when backtracking, the input can match but with a smaller # of characters)
                df.at[current_row, "change_segments"][check_segment_number] = (df.at[current_row, "change_segments"][check_segment_number][0],
                                                                               df.at[current_row, "change_segments"][check_segment_number][1],
                                                                               max(df.at[current_row, "change_segments"][change_segment_number][2],
                                                                                   df.at[current_row, "change_segments"][check_segment_number][2]))
        if (not duplicate_found):
            result_row.append(df.at[current_row, "change_segments"][change_segment_number])
    df.at[current_row, "change_segments"] = result_row
    for complete_row_segment in df.at[complete_row, "change_segments"]:
        is_duplicate = False
        for current_row_segment_number in range(len(df.at[current_row, "change_segments"])):
            if ((complete_row_segment[0] == df.at[current_row, "change_segments"][current_row_segment_number][0]) and
                (complete_row_segment[1] == df.at[current_row, "change_segments"][current_row_segment_number][1])):
                # '''Note: If you include equal end conditions, you can get into an
                # infinite merge loop if the NER picks up a condition before it's
                # finised: ex - [('TIME', 17, 29), ('TIME', 17, 30)]
                # and (complete_row_segment[2] == current_row_segment[2])):'''
                is_duplicate = True
                # Tuples are immutable. overwrite with with copy but with the max of the
                # two end items (when backtracking, the input can match but with a smaller # of characters)                
                df.at[current_row, "change_segments"][current_row_segment_number] = (df.at[current_row, "change_segments"][current_row_segment_number][0],
                                                                                    df.at[current_row, "change_segments"][current_row_segment_number][1],
                                                                                    max(complete_row_segment[2],
                                                                                        df.at[current_row, "change_segments"][current_row_segment_number][2]))                
        if (not is_duplicate):
            df.at[current_row, "change_segments"].append(complete_row_segment)
#    print("Changed_segments ="+ str(df.at[current_row, "change_segments"]))
    # the current row change segments should now contain only de-duped data
    # but there could still be overlaps.
    # if 1 or 0 items, no merge possible.       
    if (len(df.at[current_row, "change_segments"]) > 1 ): 
        # Algorithm
        # Select item from list
        # check against each other item.  
        #        if it finds an overlap merge the two items together and restart  
        # if it doesn't find a match move to the 2nd item
        # if you reach the last item with no matches, you're done.
        merge_found = True
        while (merge_found):
            merge_found = False
            for i in range(len(df.at[current_row, "change_segments"])):
                # performance takes a massive hit on these if statements for longer pieces of text. (like 80% of total function time)
                # remove the df calls and replace with single assignment for the if statements.
                i_segment = df.at[current_row, "change_segments"][i]
                for j in range(i+1,len(df.at[current_row, "change_segments"])):
                    j_segment = df.at[current_row, "change_segments"][j]
                    # Merge case 1, current segment contained in, or overlaps
                    # end of merged segment
#                    if (((df.at[current_row, "change_segments"][i][1] < df.at[current_row, "change_segments"][j][2]) and
#                         (df.at[current_row, "change_segments"][i][2] > df.at[current_row, "change_segments"][j][1])) or
                    if (((i_segment[1] < j_segment[2]) and
                         (i_segment[2] > j_segment[1])) or                        
                    # Merge case 2, current segment contained in, or overlaps
                    # beginning of merged segment
#                        ((df.at[current_row, "change_segments"][i][2] < df.at[current_row, "change_segments"][j][1]) and
#                         (df.at[current_row, "change_segments"][i][1] > df.at[current_row, "change_segments"][j][2])))                            
                        ((i_segment[2] > j_segment[1]) and
                         (i_segment[1] < j_segment[2]))):
                            # merge found.  Set i item (lower value) to the combined tag then delete the j item
                            replace_text = df.at[current_row, "change_segments"][i][0]
                            # if the segment names are different, check to see if the item (or part of the item) that's
                            # being added is already contained within the the other item 
                            # (ex: "PERSON OR ORG" + "PERSON OR MONEY" -> "PERSON OR ORG OR MONEY")
                            if (df.at[current_row, "change_segments"][i][0] !=
                                df.at[current_row, "change_segments"][j][0]):
                                i_text_items = df.at[current_row, "change_segments"][i][0].split(" OR ")
                                j_text_items = df.at[current_row, "change_segments"][j][0].split(" OR ")
#                                print("Merge Check For Row:" + str(current_row))
#                                print(i_text_items)
#                                print(j_text_items)
                                replace_text = ""
                                combined_items = i_text_items + j_text_items
                                for combined_items_i_count in range(len(combined_items)):
                                    is_text_duplicate = False                                    
                                    for combined_items_j_count in range(combined_items_i_count + 1, len(combined_items)):
                                        if (combined_items[combined_items_i_count] == combined_items[combined_items_j_count]):
                                            is_text_duplicate = True
                                    if (not is_text_duplicate):
                                        if (replace_text == ""):
                                            replace_text = combined_items[combined_items_i_count]
                                        else:
                                            replace_text = replace_text + " OR " + combined_items[combined_items_i_count]                                        
                            df.at[current_row, "change_segments"][i] = (replace_text,
                                                                        min(df.at[current_row, "change_segments"][i][1], 
                                                                            df.at[current_row, "change_segments"][j][1]),
                                                                        max(df.at[current_row, "change_segments"][i][2], 
                                                                            df.at[current_row, "change_segments"][j][2]))
                            del df.at[current_row, "change_segments"][j]
                            if (options["debug_level"] > 0):
                                print("Merged changed_segments list ="+str(df.at[current_row, "change_segments"]))
                            merge_found = True  
                            break # restart while
                if (merge_found): # restart while
                    break
    if (options["debug_level"] > 0):                
        print("Merged_list ="+str(df.at[current_row, "change_segments"]))
    # Should no longer be overlapping items, so sort on start
    df.at[current_row, "change_segments"].sort(key=get_segment_start, reverse=True)
    if (options["debug_level"] > 0):
        print("Final_list = "+str(df.at[current_row, "change_segments"]))
    return

#@profile
def replace_segment_data(df, row_id, replacement_bundle, options):
#   Go through each replacement segment tuple (label, start, end) and swap out with replacement label
#    print("replace_segment_data: Row id="+str(row_id))
#    print(df.at[row_id, "change_segments"])
    df.at[row_id, "change_segments_text"] = str(df.at[row_id, "change_segments"])
    for segment in df.at[row_id, "change_segments"]:
        segment_label, segment_start, segment_end = segment
        if (len(df.at[row_id, "scrubbed_text"]) > segment_end):
            # Note: Capturing replaced data needs to happen _before_ you replace it
            replacement_bundle["df"].at[ replacement_bundle["index"], "Replacement ID" ] = segment_label
            replacement_bundle["df"].at[ replacement_bundle["index"], "Replaced_Text"  ] = df.at[row_id, "scrubbed_text"][segment_start:segment_end]
            replacement_bundle["index"] = replacement_bundle["index"] + 1          
            df.at[row_id, "scrubbed_text"] = df.at[row_id, "scrubbed_text"][:segment_start] + \
                                              "{" + segment_label + "}" + \
                                              df.at[row_id, "scrubbed_text"][segment_end:]
            df.at[row_id, "has_changed"] = 1 

        elif (len(df.at[row_id, "scrubbed_text"]) > segment_start):
            # Note: Capturing replaced data needs to happen _before_ you replace it            
            replacement_bundle["df"].at[ replacement_bundle["index"], "Replacement ID" ] = segment_label
            replacement_bundle["df"].at[ replacement_bundle["index"], "Replaced_Text"  ] = df.at[row_id, "scrubbed_text"][segment_start:segment_end]
            replacement_bundle["index"] = replacement_bundle["index"] + 1                         
            df.at[row_id, "scrubbed_text"] = df.at[row_id, "scrubbed_text"][:segment_start] + \
                                              "{" + segment_label + "}"
            df.at[row_id, "has_changed"] = 1 

        # else
        # leave the string be
#    print(df.at[row_id, "scrubbed_text"], flush=True)  
    return

#@profile
def scrub_rows_backwards(df, row_id, replacement_bundle, options):
# Once we have found a "Complete" message, we take the segments from the completed
# item and traverse backwards, substituting our placeholder text for the information 
# we want to remove.

# We use the "collect data forward, overlay backwards" method because our accuracy of 
# data to remove will be better the further on in the message we get, however, we as we're
# capturing keystroke data, we need to try out best to substitue in for any PII data that gets
# captured upon entry and then "deleted" before a message is complete.
    #print("scrub_rows_backwards: Row id="+str(row_id))
    current_row_id = row_id
    complete_row_id = row_id
    while ((current_row_id >= 0) and
           ((current_row_id == complete_row_id) or
            (df.at[current_row_id, "is_complete"] != 1))):
        if (options["debug_level"] > 0):
            print("Processing row: "+str(current_row_id) +" for complete row:"+str(complete_row_id))  
        if (df.at[current_row_id, "has_changed"] != 1):
            merge_change_segments(df, current_row_id, complete_row_id, options)
            if (options["debug_level"] > 0):
                print("Finished Merging")
            replace_segment_data(df, current_row_id, replacement_bundle, options)
        else:
            if (options["debug_level"] > 0):
                print("Already changed from structural items. Leave text alone.")            
        current_row_id = current_row_id - 1
    return

if __name__ == "__main__":

    main()
