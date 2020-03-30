# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 11:29:14 2020

@author: Ankit Pandey
"""
import pandas as pd 
import numpy as np
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process
import re



df_LMB_Master = pd.read_excel("D:\\MS\\Double Debiting\\Not Found\\LMB from Retail - Not_Found.xlsx", sheet_name="Sheet1")
df_MSIS_Master = pd.read_excel("D:\\MS\\Double Debiting\\Not Found\\MSIS Total.xlsx")
df_SMB_Master = pd.read_excel("D:\\MS\\Double Debiting\\Not FOund\\SMB Total.xlsx")


df_MSIS_Lookup = pd.read_excel("D:\\MS\\Double Debiting\\Python Outputs\\MSIS_Lookup.xlsx")
df_SMB_Lookup = pd.read_excel("D:\\MS\\Double Debiting\\Python Outputs\\SMB_Lookup.xlsx")


df_remaining_NAs = pd.read_excel("D:\\MS\\Double Debiting\\remaining_NAs.xlsx")


df_LMB_Usable = pd.DataFrame()
df_LMB_Usable["Policy Ref"] = df_LMB_Master["Policy Ref"]
df_LMB_Usable["Client Name"] = df_LMB_Master["Client Name"]
df_LMB_Usable["Insurer"] = df_LMB_Master["Insurer"]
df_LMB_Usable["Premium GBP"] = df_LMB_Master["Premium GBP"]
df_LMB_Usable["Broker"] = df_LMB_Master["Broker"]

df_LMB_Usable["Policy Start Date"] = df_LMB_Master["Policy Start Date"]
df_LMB_Usable["Renewal Date"] = df_LMB_Master["Renewal Date"]

df_tmp = df_LMB_Usable.groupby(['Policy Ref','Insurer','Client Name','Broker','Policy Start Date','Renewal Date'])['Premium GBP'].sum().reset_index()

'''2448 rows Unique Policy Ref in LMB file is 2448, which will be matched to SMB and MSIS file, 
The data is spread across 6 columns'''


''' *************** Please see *********************
On 10/03/2020
adding Insurer in the groupby clause, count came to 2728 rows
'''

df_MSIS_usable = pd.DataFrame()
df_MSIS_usable["policy_year_ref"] = df_MSIS_Master["policy_year_ref"]
df_MSIS_usable["ClientName"] = df_MSIS_Master["ClientName"]
df_MSIS_usable["Overall Insurer"] = df_MSIS_Master["Overall Insurer"]
df_MSIS_usable["Premium"] = df_MSIS_Master["Premium"]

df_MSIS_usable["policy_year_effective_date_key"] = df_MSIS_Master["policy_year_effective_date_key"]
df_MSIS_usable["renewal_date_key"] = df_MSIS_Master["renewal_date_key"]

df_tmp2_MSIS = df_MSIS_usable.groupby(['ClientName','Overall Insurer','policy_year_ref','policy_year_effective_date_key','renewal_date_key'])['Premium'].sum().reset_index()
'''2146 rows across 5 columns'''
df_tmp2_MSIS.to_excel("D:\\MS\\Double Debiting\\Python Outputs\\MSIS_Lookup.xlsx")


df_SMB_usable = pd.DataFrame()
df_SMB_usable["policy_year_ref"] = df_SMB_Master["policy_year_ref"]
df_SMB_usable["ClientName"] = df_SMB_Master["ClientName"]
df_SMB_usable["Overall Insurer"] = df_SMB_Master["Overall Insurer"]
df_SMB_usable["Premium"] = df_SMB_Master["Premium"]

df_SMB_usable["policy_year_effective_date_key"] = df_SMB_Master["policy_year_effective_date_key"]
df_SMB_usable["renewal_date_key"] = df_SMB_Master["renewal_date_key"]



df_tmp2_SMB = df_SMB_usable.groupby(['ClientName','Overall Insurer','policy_year_ref','policy_year_effective_date_key','renewal_date_key'])['Premium'].sum().reset_index()
'''773 rows across 5 columns'''
df_tmp2_SMB.to_excel("D:\\MS\\Double Debiting\\Python Outputs\\SMB_Lookup.xlsx")




# =============================================================================
# Cleaned client names: Method rmv_stopwords
# =============================================================================
stop_words_lst = ['&','The','the','Limited','Services','Ltd','Solutions','and','or','and/or','Mr','Mr.']

#Function to remove stopwords from the client names
def rmv_stopwords(str2Match):
    ctr = 0
    lst_To_beMatched=[]
    for s in str2Match:
        for w in stop_words_lst:
            pattern = r'\b'+w+r'\b'
            s = re.sub(pattern, '', s)
            print(s)
        lst_To_beMatched.append(s)
        print (ctr)
        ctr+=1
    return lst_To_beMatched



def checker(wrong_options,correct_options):
    names_array=[]
    ratio_array=[]    
    ctr = 0
    for wrong_option in wrong_options:
        ctr+=1
        print(ctr)
        if wrong_option in correct_options:
           names_array.append(wrong_option)
           ratio_array.append('100')
        else:   
            x=process.extractOne(wrong_option,correct_options,scorer=fuzz.token_set_ratio)
            if(x[1] > 50):
                names_array.append(x[0])
                ratio_array.append(x[1])
            else:
                names_array.append("")
                ratio_array.append("< 50")
    return names_array,ratio_array

# =============================================================================
# Function ends
# =============================================================================



str2Match_df_tmp = rmv_stopwords(df_tmp["Client Name"].fillna("######").tolist())                             
df_tmp["CleanedLMB_Names"] = str2Match_df_tmp

str2Match_df_tmp2_MSIS = rmv_stopwords(df_tmp2_MSIS["ClientName"].fillna("######").tolist())
df_tmp2_MSIS["cleanedMSIS_records"] = str2Match_df_tmp2_MSIS


str2Match_df_tmp2_SMB = rmv_stopwords(df_tmp2_SMB["ClientName"].fillna("######").tolist())
df_tmp2_SMB["cleanedSMB_records"] = str2Match_df_tmp2_SMB

#Matching LMB Data to MSIS file options
matched2MSIS, match_ratio_value_MSIS = checker(str2Match_df_tmp,str2Match_df_tmp2_MSIS)

df_tmp["Matched2MSIS"] = matched2MSIS
df_tmp["Matched2MSIS_Ratio"] = match_ratio_value_MSIS


#Matching LMB Data to SMB file options
matched2SMB, match_ratio_value_SMB = checker(str2Match_df_tmp,str2Match_df_tmp2_SMB)
df_tmp["Matched2SMB"] = matched2SMB
df_tmp["Matched2SMB_Ratio"] = match_ratio_value_SMB



df_tmp.to_excel("D:\\MS\\Double Debiting\\Python Outputs\\df_tmp_WithInsurer.xlsx")
