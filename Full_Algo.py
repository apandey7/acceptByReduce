# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 04:10:15 2020

@author: Ankit Pandey
"""

import pandas as pd 
import numpy as np
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
import re


# =============================================================================
# Pseudo Code
# Using trickle down effect, to reduce the size of dataframe after every step
# 
# Process Flow: 
#     Check 1: Name selected from LMB File nearest match algo using checker function  -------- Future - use NLP for NER and accurate Name Matching
#     Name matched to MSIS/SMB file for nearest match - dataframe created containing 
#                                                       the reduced set of policy_year_ref from respective files.
#     Premium Match - Nearest match dataframe is then treated to verify the nearest premium match - next reduced dataframe is created
#     Policy Start Date Match - nearest match df from above step treated to find policy in 3 month range
#     Sub Classes mapping - mapped sub-class to class for MSIS and SMB seperately - Requires another set of coding
#     If len(dataframe) > 1, match the Insurer
#     this should give one value 
#     ---- Not Found to be written if any of the above steps yield no dataframe
#     
#     
# =============================================================================
    



def Master_Lookup(df_cleaned, df_allNames):
    # Match the first column in df_cleaned to the 
    # the employee name in a new column called EmployeeName
    df_cleaned = pd.merge(df_cleaned, df_allNames, how='left',left_on='cleaned_Name', right_on='cleaned_Name')
    return df_cleaned

# =============================================================================
# Name Matching: Check 1
# =============================================================================

def checker(wrong_options,correct_options):
#    Name matching algorithm based on fuzzy match; using token_sort_ratio currently, as it provides best match
    ctr = 0
    names_array=[]
    ratio_array=[]    
    for wrong_option in wrong_options:
        ctr+=1
        print(ctr)
        if wrong_option in correct_options:
           names_array.append(wrong_option)
           ratio_array.append('100')
        else:
            x = process.extractOne(wrong_option,correct_options,scorer=fuzz.token_sort_ratio)
            names_array.append(x[0])
            ratio_array.append(x[1])
    return names_array,ratio_array


stop_words_lst = ['Limited','and','Room','Club','Ltd.', 'Ltd', 'as', 'The', 't/as', 'the', 'Pty', 'LTD', '&','Investments','Properties','Mr','Mrs.',' & ','&/or',' ']

def rmv_stp_wrds(str2Match):
#    This is a primitive function to remove stop words in order to match with client names
    lst_To_beMatched = []
    ctr = 0
    for s in str2Match:
        for w in stop_words_lst:
            pattern = r'\b'+w+r'\b'
            s = re.sub(pattern, '', s.lower())
#            s = re.sub(' ','',s)
            print(s)
        lst_To_beMatched.append(s)
        print (ctr)
        ctr+=1
    return lst_To_beMatched


# =============================================================================
# Check 2:
# Policy Start Date Match
# Reducing the list of found matches, based on Policy Start Date
# Flag is used for MSIS/SMB segregation
# =============================================================================

def st_dt_Match(df_check1, flag):
#    Use client name in df_check1 to find the matching dataframe from MSIS/SMB file.
#    This selects a new dataframe with lower values that 



# =============================================================================
# Check 3:
#   Premium Match: After Name match function
#     ==> Used to find exact matching premium
#     ==> Premium in +/-10% range 
#     ==> Double premium
# Here MS_number is the policy number to be dealt with
# 
# =============================================================================
def prem_Match(MS_number, flag):
#    List of policy_year_ref numbers that match to the premium of the given MS number
    lst_policy_year_ref = []
    
    return lst_policy_year_ref






# =============================================================================
# Process Pipeline
# =============================================================================

#Step 1: Load Master Data
#Data to be matched - Unknown MS numbers
df_Original_List = pd.read_excel("D:\\MS\\Double Debiting\\Not Found\\LMB from Retail - Not_Found.xlsx", sheet_name="Sheet1")
len(df_Original_List) ##748 rows
df_Original_List.columns


df_Orignal_List_ClientNames_MSIS = pd.DataFrame()
df_Orignal_List_ClientNames_SMB = pd.DataFrame()


df_Orignal_List_ClientNames_MSIS["Client Name_LMB"] = df_Original_List["Client Name_LMB"][(df_Original_List["Broker"]!="Square Mile Broking Ltd")]
df_Orignal_List_ClientNames_SMB["Client Name_LMB"] = df_Original_List["Client Name_LMB"][(df_Original_List["Broker"]=="Square Mile Broking Ltd")]

#filling N/A's in LMB data to reduce false positives on blank values
str2Match_MSIS = df_Orignal_List_ClientNames_MSIS["Client Name_LMB"].fillna("######").tolist()
str2Match_SMB = df_Orignal_List_ClientNames_SMB["Client Name_LMB"].fillna("######").tolist()

#Removing the Stop words to obtain a clean list
str2Match_MSIS_cleaned = rmv_stp_wrds(str2Match_MSIS)
str2Match_SMB_cleaned = rmv_stp_wrds(str2Match_SMB)


df_Orignal_List_ClientNames_MSIS["cleaned_Names"] = str2Match_MSIS_cleaned 
df_Orignal_List_ClientNames_SMB["cleaned_Names"]= str2Match_SMB_cleaned

#MSIS Data
#data to be matched to the policy_year_ref numbers
df_To_beMatched_MSIS = pd.read_excel("D:\\MS\\Double Debiting\\Not Found\\MSIS Total.xlsx")
#df_To_beMatched["ClientName"]
len(df_To_beMatched_MSIS)
df_To_beMatched_MSIS.columns
#filling N/A's in MSIS data to reduce false positives on blank values
strOptions_MSIS = df_To_beMatched_MSIS["ClientName"].fillna("######").tolist()
#Removing the Stop words to obtain a clean list
strOptions_MSIS_cleaned = rmv_stp_wrds(strOptions_MSIS)
#Adding the cleaned names to the dataframe back
df_To_beMatched_MSIS["cleaned_MSIS_Names"] = strOptions_MSIS_cleaned


#SMB Data
#data to be matched to thse policy_year_ref numbers
df_To_beMatched_SMB = pd.read_excel("D:\\MS\\Double Debiting\\Not Found\\SMB Total.xlsx")
len(df_To_beMatched_SMB)
df_To_beMatched_SMB.columns
#filling N/A's to reduce false positives on blank values
strOptions_SMB = df_To_beMatched_SMB["ClientName"].fillna("######").tolist()
#Removing the Stop words to obtain a clean list
strOptions_SMB_cleaned = rmv_stp_wrds(strOptions_SMB)

df_To_beMatched_SMB["cleaned_SMB_Names"] = strOptions_SMB_cleaned



#Name Matching for SMB
name_match_SMB,ratio_match_SMB = checker(str2Match_SMB_cleaned,strOptions_SMB_cleaned)

df_Orignal_List_SMB = df_Original_List[(df_Original_List["Broker"]=="Square Mile Broking Ltd")].reset_index()
df_Orignal_List_SMB["Cleaned_SMB_Names"] = str2Match_SMB_cleaned
df_Orignal_List_SMB["Matched_Names_SMBFile"] = name_match_SMB
df_Orignal_List_SMB["Matched_Names_SMBFile_ratio"] = ratio_match_SMB

#df_Orignal_List_SMB.columns

#for key,value in df_Orignal_List_SMB.iterrows():
#    Go to check2 only if the matching ratio is > 90
#    if(value["Matched_Names_SMBFile_ratio"] >90):
#        Putting the value into a dataframe returned from the start date check
#        This contains the reduced dataframe with all records of matching client names.
#        df_check1 = st_dt_Match(value["Matched_Names_SMBFile"])
#    print(value["Policy Ref"])
#    break
    #print(key)
    #if((key == "Matched_Names_SMBFile") | (key == "Cleaned_SMB_Names") | (key == "Client Name_LMB")):
    #    print(value)
        


#Name Matching for MSIS
name_match_MSIS,ratio_match_MSIS = checker(str2Match_MSIS_cleaned,strOptions_MSIS_cleaned)
#df_Orignal_List_ClientNames_MSIS

df_Orignal_List_MSIS = df_Original_List[(df_Original_List["Broker"]!="Square Mile Broking Ltd")] ### 729 line items
df_Orignal_List_MSIS["Matched_Names_MSISFile"] = name_match_MSIS
df_Orignal_List_MSIS["Matched_Names_MSISFile_ratio"] = ratio_match_MSIS



# =============================================================================
# #check 2 
# Matching of premium on a reduced dataframe
# =============================================================================









# =============================================================================
# Final Saving of File with matched values
# =============================================================================

df_Orignal_List_MSIS["Cleaned_Names"] = str2Match_MSIS_cleaned
df_Orignal_List_SMB["Cleaned_Names"] = str2Match_SMB_cleaned


df3 = pd.DataFrame()
df3["old_names"]=pd.Series(str2Match_3)
df3["correct_names"]=pd.Series(name_match_3)
df3["check"] = np.nan
df3["correct_ratio"]=pd.Series(ratio_match_3)
df3.to_excel("D:\\MS\\Double Debiting\\Python Outputs\\matched_names_Unknown.xlsx", engine="xlsxwriter")





#Data for matching to clenaed data
df_To_beMatched_MSIS.to_excel("D:\\MS\\Double Debiting\\Not Found\\MSIS_Lookup.xlsx")
df_To_beMatched_SMB.to_excel("D:\\MS\\Double Debiting\\Not Found\\SMB_Lookup.xlsx")




#matched Data
df_Orignal_List_MSIS.to_excel("D:\\MS\\Double Debiting\\Not Found\\MSIS_Matched.xlsx")
df_Orignal_List_SMB.to_excel("D:\\MS\\Double Debiting\\Not Found\\SMB_Matched.xlsx")
