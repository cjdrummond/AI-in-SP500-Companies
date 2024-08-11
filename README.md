# AI-in-SP500-Companies
Project designed to webscrape financial documents for past 10 years for companies in the S&amp;P 500. Then find the frequency of specific keywords for each of these companies. Each company is assigned its own folder to keep the data organized and then the frequencies are put into csv files for ease of further analysis. 

File Keywords.csv contains the AI keywords of interest the project is finding the frequencies of.
File SP500excel.xlsx is an excel file of the companies that are in the S&amp;P 500. It is used to create a Master List of the companies, their IDs and then combine both of these to make an ID used to create a directory for each company in a SP500_Data folder. Then within each company folder, a csv file is created with the 10k filing information for each year. The text for the 10k alone is downloaded and saved to a text file in the company folder. The text file is named 10k_filings_{id}_{year}.txt
Another folder is then created in the target directory to hold the keyword frequency matricies for each company. There is a csv file for each company that contains the keyword frequencies for each keyword from the Keywords.csv file. Each row is a year and the columns are the frequency of a specific keyword. 

To run this project the target directory needs to contain atleast 30GB of free space as well as the SP500excel.xlsx file and Keywords.csv file. The rest will be created within the project. Set your target directory in the main function. In the class SecAPI call you also need to change the 'User-Agent' to be your email. 
