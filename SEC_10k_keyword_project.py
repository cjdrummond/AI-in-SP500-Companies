import os.path
from pathlib import Path
from requests import Session
from ratelimit import limits, sleep_and_retry
from bs4 import BeautifulSoup
import pandas as pd
import re
import csv
import numpy as np


class SecAPI(object):
    def __init__(self):
        self.session = Session()
        self.session.headers.update({'User-Agent': 'cjdrummond@crimson.ua.edu'})

    calls = 9
    seconds = 1

    @sleep_and_retry
    @limits(calls=calls, period=seconds)
    def get(self, url):
        return self.session.get(url)

global sec_api  ## this defienes a global variable that can be used in any function
sec_api = SecAPI()


def utl_Step_3A_ParseBusinessSection(input_10k_filing_txt):
    return "TBD - Path to text file with Business section as html"


def utl_Step_3B_ParseRisksSection(input_10k_filing_txt):
    return "TBD - Path to text file with Risks section as html"


def utl_Step_3C_ParseMDA7Section(input_10k_filing_txt):
    return "TBD - Path to text file with MD&A7 section as html"

'''
INPUTS:
input_sp500_xlsx is the sp500excel.xlsx file, 
tgt_csv is the SP500_Master_List.csv file 

OUTPUTS:
returns the populated SP500_Master_List.csv file with the sp500 excel file data with extra columns for TIK, CIK, ID. The security column from excel file is changed to COMPANY in the master list csv file.'''
def Step_01_GetSP500(input_sp500_xlsx, tgt_csv, overwrite=False):
    ## overwrite = False is defalut argument meaning that it will not overwrite the file if it already exists. if you want to overwrite it, you have to set it to True
    print("Step_01_GetSP500()")
    cleared = True

    if os.path.isfile(tgt_csv):
        if overwrite:
            os.remove(tgt_csv)
        else:
            cleared = False
    '''overwrite check to determine what action to take if the target file (tgt_csv) already exists. if overwrite is true, then it will delete the file and create a new one. if overwrite is false, then it will not delete the file and will not create a new one'''        
    if cleared:
        sp500companies = pd.read_excel(input_sp500_xlsx, header=0)
        
        sp500companies['TIK'] = sp500companies['Symbol'].astype(str).str.ljust(5, '_')
        '''changes the symbol column from the excel file to a string and then adds underscores to the end of the string until it is 5 characters long. this becomes the TIK column in the master list csv file'''
        
        sp500companies['CIK'] = sp500companies['CIK'].astype(str).str.zfill(10)
        '''changes the CIK column from the excel file to a string and then adds zeros to the beginning of the string until it is 10 characters long. this becomes the CIK column in the master list csv file'''
        
        sp500companies['ID'] = sp500companies[['TIK', 'CIK']].agg('_'.join, axis=1)
        '''this creates a new column called ID that is the combination of the TIK and CIK columns. the ID column is the primary key in the master list csv file. Each ID is 16 characters in length.''' 
        
        sp500companies['COMPANY'] = sp500companies['Security'].astype(str)
    
        sp500companies = sp500companies[['ID', 'Symbol', 'TIK', 'COMPANY', 'CIK', 'GICS Sector', 'GICS Sub-Industry', 'Headquarters Location', 'Date added', 'Founded']] 
        
        #print(sp500companies.to_string()) 
        
        sp500companies.to_csv(path_or_buf=tgt_csv) 
        #^writes the sp500companies dataframe to csv file in the target directory
        print("Step_01_GetSP500() - Completed")
        return tgt_csv 
    '''returns the masterlist csv file with the sp500 companies in it has the creation of the ID which is used in other functions.
    
    this masterlist is used in the Step_02_Get10kUrls_as_csv function to create the 10k filings csv file path and adds a column to the masterlist csv file with the path to the 10k filings csv file for each company.'''


'''
Step 2 inputs and outputs are as follows,
INPUTS:
SP500_Master_csv is the SP500_Master_List.csv file,
root_target_dir is the target directory where the SP500_Data folder will be created, and is also where the masterlist csv file is located,

OUTPUTS:
creates the SP500_Data folder in the target directory and then creates a folder for each company in the SP500_Data folder.

downloads the 10k filing text for each company in the SP500_Master_List.csv file and saves them to the SP500_Data folder.

adds column to masterlist csv file with the path to the 10k filings csv file for each company.
'''
def Step_02_Get10kUrls_as_csv(SP500_Master_csv, root_target_dir, overwrite):
    base_url = r"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany"
    sp500_df = pd.read_csv(filepath_or_buffer=SP500_Master_csv, dtype={'CIK': str}) 
    #dtype declaration needed since CIK is numeric but we want it to be treated as string. if left out, pandas will infer the type incorrectly and drop leading zeros.
    
    sp500_df.set_index('ID')
    _10k_Yearly_filings_csv_paths = []
    for row in sp500_df.itertuples(index=True, name='Pandas'):
        id = getattr(row, "ID")
        cik = getattr(row, "CIK")
        tik = getattr(row, "TIK")
        print(f"Processing: {id}")

        subdirectory = os.path.join(root_target_dir, "SP500_Data", id) 
        #os.path.join joins the comma separated items into a path. essentially the commans are replaced with the correct path separator for the operating system.
        if not os.path.exists(subdirectory):
            # if the demo_folder directory is not present
            # then create it.
            os.makedirs(subdirectory)
        '''^creates folder SP500_Data in the target directory and then creates a folder for each company in the SP500_Data folder'''

        goodtogo = True
        tgt_csv_path = os.path.join(subdirectory, f"_10k_filings_{id}.csv")
        _10k_Yearly_filings_csv_paths.append(tgt_csv_path)
        if os.path.isfile(tgt_csv_path):
            if overwrite:
                os.remove(tgt_csv_path)
            else:
                sp500_df.at[row.Index, '10K_Yearly_filings_csv'] = tgt_csv_path
                goodtogo = False
        sp500_df.at[row.Index, '10K_Yearly_filings_csv'] = tgt_csv_path 
        ## adds a column to the masterlist csv file with the path to the 10k filings csv file for each company.
        if goodtogo:
            edgar_query_url = base_url + "&CIK=" + str(cik) + "&type=10-K" + "%25" + "&datea=20110101" + "&owner=exclude" + "&start=0" + "&count=50" + "&output=atom"
            response = sec_api.get(edgar_query_url)
            feed10K = BeautifulSoup(response.content, 'xml').feed
            entries = feed10K.find_all('entry')
            ## uses the edgar search system the search is limited to 10K filings and the date range is from 2011 to present. the search is limited to 50 results. the results are returned in xml format and then parsed with beautiful soup.
            tgt_csv_rows = []
            tuple_dict = {}
            for entry in entries:
                filing_htm_url = entry.find('filing-href').text
                filing_txt_url = filing_htm_url.replace('-index.htm', '.txt')
                filing_type = entry.find('filing-type').text
                filing_date = entry.find('filing-date').text
                filing_year = filing_date[:4]

                if filing_type == '10-K': # only cares about 10-k filings. if company has 10-k/a filings, only the original 10-k filing is used.

                    tgt_txt_path = os.path.join(subdirectory, f"10k_filings_{id}_{filing_year}.txt")
                    if os.path.isfile(tgt_txt_path):
                        if overwrite:
                            os.remove(tgt_txt_path)
                    
                    print(f"\t{filing_year} 10k filing downloading: {tgt_txt_path}")
                    start_index, end_index = utl_Step_2A_Get10kDocs_as_txt(
                        input_10k_txt_url=filing_txt_url,
                        output_10k_txt_path=tgt_txt_path,
                        overwrite=overwrite)

                    new_row = [id, cik, tik, filing_year, filing_date, filing_htm_url, filing_txt_url, start_index, end_index, tgt_txt_path]
                    tgt_csv_rows.append(new_row)

            with open(tgt_csv_path, 'w', newline='') as csvfile:
            
                # creating a csv writer object
                csvwriter = csv.writer(csvfile)
                
                # writing the fields
                csvwriter.writerow(['id', 'cik', 'tik', 'filing_year', 'filing_date', 'filing_htm_url', 'filing_txt_url', 'start_index', 'end_index', '10k_txt_path'])

                # writing the data rows
                csvwriter.writerows(tgt_csv_rows)
                
            sp500_df.at[row.Index, '10K_Yearly_filings_csv'] = tgt_csv_path
            ## adds a column to the masterlist csv file with the path to the 10k filings csv file for each company.
            ## each of the 10k filings csv files contains the 10k filing path for each year for the company. 
    

    sp500_df.to_csv(path_or_buf=SP500_Master_csv, index=False) ## writes the changes to the masterlist csv file

    return SP500_Master_csv

'''used in step 2'''
def utl_Step_2A_Get10kDocs_as_txt(input_10k_txt_url, output_10k_txt_path, overwrite):

    # Get the text from URL which includes many documents, but we only want the 10k enclosed by: <DOCUMENT><TYPE> 10-K </TYPE></DOCUMENT>'
    response = sec_api.get(input_10k_txt_url)
    raw_txt = response.text

    # Regex to find <DOCUMENT> tags
    doc_start_pattern = re.compile(r'<DOCUMENT>')
    doc_end_pattern = re.compile(r'</DOCUMENT>')
    # Regex to find <TYPE> tag prceeding any characters, terminating at new line
    type_pattern = re.compile(r'<TYPE>[^\n]+')

    # Create 3 lists with the span idices for each regex

    ### There are many <Document> Tags in this text file, each as specific exhibit like 10-K, EX-10.17 etc
    ### First filter will give us document tag start <end> and document tag end's <start>
    ### We will use this to later grab content in between these tags
    doc_start_is = [x.end() for x in doc_start_pattern.finditer(raw_txt)]
    doc_end_is = [x.start() for x in doc_end_pattern.finditer(raw_txt)]

    ### Type filter is interesting, it looks for <TYPE> with Not flag as new line, ie terminate there, with + sign
    ### to look for any char afterwards until new line \n. This will give us <TYPE> followed Section Name like '10-K'
    ### Once we have have this, it returns String Array, below line will with find content after <TYPE> ie, '10-K'
    ### as section names
    doc_types = [x[len('<TYPE>'):] for x in type_pattern.findall(raw_txt)]

    raw_10k_start_index = 0
    raw_10k_end_index = 0
    raw_10k_txt = ''

    # Create a loop to go through each section type and save only the 10-K section in the dictionary
    for doc_type, doc_start, doc_end in zip(doc_types, doc_start_is, doc_end_is):
        if doc_type == '10-K':
            raw_10k_start_index = doc_start
            raw_10k_end_index = doc_end
            raw_10k_txt = raw_txt[doc_start:doc_end]
            break

    if not overwrite:
        # Writing to file
        with open(output_10k_txt_path, "w") as filing:
            # Writing data to a file
            filing.writelines(raw_10k_txt)
    return raw_10k_start_index, raw_10k_end_index


def Step_03_Parse10kSections_as_txt(SP500_Master_csv, root_target_dir):
    sp500_df = pd.read_csv(filepath_or_buffer=SP500_Master_csv, dtype={'CIK': str})
    sp500_df.set_index('ID')
    for sp500_row in sp500_df.itertuples(index=True, name='Pandas'):
        id = getattr(sp500_row, "ID")
        sub_dir = os.path.join(root_target_dir, "SP500_Data", id)
        csv_path = os.path.join(sub_dir, f"_10k_filings_{id}.csv" )
        df = pd.read_csv(filepath_or_buffer=csv_path, dtype={'CIK': str})
        df.set_index('id')

        for row in df.itertuples(index=True, name='Pandas2'):
            year = getattr(row, "filing_year")
            txt_path = os.path.join(sub_dir, f"10k_filings_{id}_{year}.txt")
            #v
            business_txt_path = utl_Step_3A_ParseBusinessSection(input_10k_filing_txt=txt_path)
            risk_txt_path = utl_Step_3B_ParseRisksSection(input_10k_filing_txt=txt_path)
            mda7_txt_path = utl_Step_3C_ParseMDA7Section(input_10k_filing_txt=txt_path)
            #^ this is where you would put the code to parse the specific sections of the 10k filing using the locations in the locations folder. inputs need to be the path to the 10k filing text file and the output needs to be the path to the parsed section text file.

            df.at[row.Index, 'business_txt_path'] = business_txt_path
            df.at[row.Index, 'risk_txt_path'] = risk_txt_path
            df.at[row.Index, 'mda7_txt_path'] = mda7_txt_path
        df.to_csv(path_or_buf=csv_path, index=False)

def Step_04_Create_Keyword_Matrix_as_csv(sections, SP500_Master_csv, keywords_csv, target_directory):
    print(f"Starting Step_04_Create_Keyword_Matrix_as_csv()")

    kw_df = pd.read_csv(filepath_or_buffer=keywords_csv)
    kw_df.set_index('ID') 
    ## this is the keyword csv file that contains the keywords and their different possible variations to search for. ID is a unique identifier for each keyword set.
    
    ## if you want to add or change keywords to search for, you can do it in the keywords.csv file.
    print(f"Reading keywords.csv with a record count of: {len(kw_df)} ")

    # Step 4A_process_sections
    print("Processing 10k Sections...")
    for section in sections:

        print(f"STEP 4A - Processing 10k Section: {section}")
        sp500_df = pd.read_csv(filepath_or_buffer=SP500_Master_csv, dtype={'10K_Yearly_filings_csv': str})
        sp500_df.set_index('ID')
        print(f"Reading SP500_Master.csv with a record count of: {len(sp500_df)} (number of TIK ids or ticker symbols to process")
        # print statement above is just for debugging purposes to see how many companies are being processed. useful if you plan to add or remove companies from the analysis. 

        # Step 4B_process_ticker
        keyword_matrix_csv_path = "TBD" ##this is defined later
        for sp500_row in sp500_df.itertuples(index=True):
            id = getattr(sp500_row, "ID")
            print(f"\tSTEP 4B - Processing TIK: {id}")
            
            csv_path = os.path.join(target_directory, 'SP500_Data', id, f"_10k_filings_{id}.csv")
            annual_10k_docs_df = pd.read_csv(filepath_or_buffer=csv_path)
            annual_10k_docs_df.set_index('id')
            print(f"\tCount of 10k Docs to process: {len(annual_10k_docs_df)}")

            # Step 4C_process_years_within_ticker
            headers = []
            value_list = []
            headers.append('ID')
            headers.append('YEAR')

            for kw_row in kw_df.itertuples(index=True, name='Pandas'):
                kw_FieldName = getattr(kw_row, "FieldName")
                headers.append(kw_FieldName)
            headers.append("10K_Source_text_path")
            headers.append("Section")
            ''' the fieldname is the keyword csv file is what is used as the column header in the keyword matrix csv file.'''
            for current_10_doc_row in annual_10k_docs_df.itertuples(index=True, name='Pandas'):
                source_txt_path_field_name = f"{section}_txt_path"
                year = getattr(current_10_doc_row, "filing_year")
                print(f"\t\t10k Year: {year}: ")
                txt_path = os.path.join(target_directory, 'SP500_Data', id, f"10k_filings_{id}_{year}.txt")

                # Step4D_process_keywords()
                values = [id, year]
                for kw_row in kw_df.itertuples(index=True, name='Pandas'):
                    kw_KeywordList = getattr(kw_row, "KeywordList").split('|')
                    total_count = 0  #initialize the total count to zero so that it can be incremented in the for loop below and reset for each keyword set.
                    for kw in kw_KeywordList:
                        current_kw_count = utl_Step_4E_CountKeywords(input_kw_str=kw, input_10k_txt=txt_path)
                        total_count = total_count + current_kw_count
                    if total_count > 0:
                        print(f"\t\t\t{total_count} keyword(s) found: {kw_KeywordList}")
                        #if you just want to see the keyword frequencies in the created csv files, comment out the print statement above. this will print the frequency of each keyword set for each company in the console.
                    values.append(total_count)
                values.append(csv_path)
                values.append(section)
                value_list.append(values)
                #value_list is the list of lists that is used to create the keyword matrix csv file. each list in the list is a row in the csv file.
            keyword_matrix_csv_dir = os.path.join(target_directory, f"{section}_keyword_matrix")
            if not os.path.exists(keyword_matrix_csv_dir):
                # if the demo_folder directory is not present
                # then create it.
                os.makedirs(keyword_matrix_csv_dir)
            # lines 317-319 create the directory for the section keyword matrix csv files if it doesn't already exist. It will create a new directory for each section once the sections are able to be parsed.
            keyword_matrix_csv_path =  os.path.join(keyword_matrix_csv_dir, f"{id}.csv",)
            keyword_matrix_df = pd.DataFrame(value_list, columns=headers)
            df_str = f"\t{keyword_matrix_df.to_string()}".replace("\n", "\n\t") #converts dataframe to string and adds a tab to the beginning of each line. this is just for debugging purposes to see the dataframe in the console with print statement below
            print(df_str)
            
            keyword_matrix_df.to_csv(path_or_buf=keyword_matrix_csv_path, index=False)
            #saves the keyword matrix dataframe as a csv file to the path defined above.

        sp500_df.at[sp500_row, f"{section}_keyword_matrix"] = keyword_matrix_csv_path
        ## ^ change indentation forward to add section keyword matrix path to the masterlist csv file
        sp500_df.to_csv(SP500_Master_csv)

def utl_Step_4E_CountKeywords(input_kw_str, input_10k_txt):
    raw_text = Path(input_10k_txt).read_text()
    keyword_with_spaces = f" {input_kw_str} "
    expression = re.compile(r'\s'+ input_kw_str +r'\s|\.\>', re.IGNORECASE) 
    ''' this is the regex expression that is used to find the keywords in the 10k text files. it is looking for the keyword with a space before and after it or a period after it. this is to avoid finding the keyword in other words.'''
    count = expression.findall(raw_text).__len__()
    return count
    #
    # keyword = "companies that"
    # if keyword == "AI" or keyword == "NLP":
    #     keyword_freq = len(re.findall(r'\b' + keyword + r'\b', text))
    #
    # elif keyword == "open AI" or keyword == "generative AI":
    #     keyword1, keyword2 = keyword.split()
    #     keyword1_freq = len(re.findall(keyword1, text, re.IGNORECASE))
    #     keyword2_freq = len(re.findall(r'\b' + keyword2 + r'\b', text))
    #     keyword_freq = min(keyword1_freq, keyword2_freq)
    #
    # elif keyword == "high-intelligence robotics" or keyword == "speech to text" or keyword == "text to speech":
    #     keyword = keyword.replace(" ", "-?")
    #     keyword_freq = len(re.findall(keyword, text, re.IGNORECASE))
    # # keyword_freq = text.count(keyword)
    # # print(f"frequency of '{keyword}' is '{keyword_freq}'")
    # else:
    #     keyword_freq = len(re.findall(keyword, text, re.IGNORECASE))
    #
    # return keyword_freq

def main():
    '''
    target directory needs to contain 30GB of free space as well as the sp500excel.xlsx file and the keywords.csv file. 
    If on mac, file path will have this structure: /Users/username/ST 597 Project, with the forward slashes.
    '''
    target_directory  = r"F:\ST 597 Project"
    source_SP500_xlsx = os.path.join(target_directory, "SP500excel.xlsx")
    SP500_Master_csv  = os.path.join(target_directory, "SP500_Master_List.csv")
    keyword_csv       = os.path.join(target_directory, "Keywords.csv")
    sections_to_process = [
        '10k'  # ,'business' ,'risk' ,'mda7'
    ]       #only uncomment other sections if code is added in steps 3A, 3B and 3C to parse the 10k text files into the specific sections.
    ''' step 1 does the following, 
    - takes in the sp500excel.xlsx file
    - populates the SP500_Master_List.csv file with the sp500 excel file with extra columns for TIK, CIK, ID, and changes the security column to COMPANY also still contains the original columns from the excel file'''
    Step_01_GetSP500(input_sp500_xlsx=source_SP500_xlsx, tgt_csv=SP500_Master_csv, overwrite=True)
    '''
    step 1 should only need to be run once. comment this functinon call out after the masterlist csv file has been populated. 
    '''
    
    '''
    Step 2 takes in the SP500_Master_List.csv file and uses the IDs created there to create a directory path for each company in the SP500_Data folder. 
    
    Within each company folder, a csv file is created with the 10k filing information for each year. The csv file contains the following columns: id, cik, tik, filing_year, filing_date, filing_htm_url, filing_txt_url, start_index, end_index, 10k_txt_path.
        
    The text for just the 10-k (takes out the other document types like images and exhibits) is downloaded and saved to a text file in the company folder. The text file is named 10k_filings_{id}_{year}.txt. 
    
    10k_txt_path is the path to the 10k text file for that year.
    '''
    Step_02_Get10kUrls_as_csv(SP500_Master_csv=SP500_Master_csv, root_target_dir=target_directory, overwrite=False)
    '''
    step 2 throws an warning, but it still works. It is upset about a potential future dtype error but it doesn't affect the code. Tried to fix it but couldn't figure it out.
    '''

    '''
    Step 3 is where the 10k text files are parsed into the specific sections. This uses functions 3A, 3B and 3C to parse the 10k text files into the business, risks and mda7 sections. Not used at this time but could be used in the future.
    '''
    Step_03_Parse10kSections_as_txt(SP500_Master_csv=SP500_Master_csv, root_target_dir=target_directory)
    
    '''
    Step 4 is where the keyword matrix csv files are created. This uses the keyword csv file that is in the target directory and creates folders for each section. 
    
    Within those section folders, there is a csv file for each company that contains the keyword frequencies for each keyword set from the keyword csv file. Each row in the csv file is a year and the frequency of the keywords for that year. 
    '''
    Step_04_Create_Keyword_Matrix_as_csv(sections=sections_to_process, SP500_Master_csv=SP500_Master_csv, keywords_csv=keyword_csv, target_directory=target_directory)


main()
