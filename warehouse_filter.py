#!/usr/bin/env python3
# author: Puru Rastogi

list_to_check = ['warehouse', 'warehousing','pick']

import pandas as pd
import requests
from bs4 import BeautifulSoup

def find_words_in_websites(websites_df, words_to_check, input_csv_file):
    websites_with_words = []
    total_websites = len(websites_df.index)
    print('total websites: ',total_websites)
    # dict to avoid repeating the websites
    web_set = {}
    for index, row in websites_df.iterrows():

        site = row['Website']
        if pd.isna(site) or not site.strip():
            print("missing website for ", row['Company Name'])
            websites_df.at[index, 'checked'] = True
            websites_df.at[index, new_column_name] = 'Invalid Website'
            print("updating csv file of name ",input_csv_file)
            websites_df.to_csv(input_csv_file, index=False)
            print('update done')
            continue
        
        # bool to check if the row is getting repeated
        site_in_set = False
        if site not in web_set:
            web_set[site] = None
        else:
            site_in_set = True

        # Skip websites that have already been checked
        if row.get('checked') == True:
            # if the website was not in set, read the previous value and add to the set.
            if not site_in_set:
                web_set[site] = row.get('new_column_name')

            if index%10 == 0:
                percentage_completion = ((index*100)//total_websites)
                print(".............percent completed:  ", percentage_completion)
            continue

        try:
            if site_in_set:
                websites_df.at[index, 'checked'] = True
                websites_df.at[index, new_column_name] = web_set[site]
                print(f"site got repeated: {site}")
                if index%10 == 0:
                    percentage_completion = ((index*100)//total_websites)
                    print(".............percent completed:  ", percentage_completion)
                    print("updating csv file of name ",input_csv_file)
                    websites_df.to_csv(input_csv_file, index=False)
                    print('update done')
                continue

            response = requests.get(site, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if any of the words in the list are present in the HTML content
            if any(word.lower() in soup.get_text().lower() for word in words_to_check):
                websites_with_words.append(site)
                websites_df.at[index, new_column_name] = 'True'
                web_set[site] = 'True'
                print(f"{site}: ", True)
            else:
                websites_df.at[index, new_column_name] = 'False'
                web_set[site] = 'False'
                print(f"{site}: ", False)
            # Mark the website as checked
            websites_df.at[index, 'checked'] = True

        except requests.exceptions.RequestException as e:
            print(f"{site}: ", "Invalid")
            websites_df.at[index, new_column_name] = 'Invalid Website'
            web_set[site] = 'Invalid Website'
            # Mark the website as checked
            websites_df.at[index, 'checked'] = True
        
        if index%10 == 0:
            percentage_completion = ((index*100)//total_websites)
            print(".............percent completed:  ", percentage_completion)
            print("updating csv file of name ",input_csv_file)
            websites_df.to_csv(input_csv_file, index=False)
            print('update done')

    return websites_with_words


# Read the CSV file
input_csv_file = 'output_Warehouse_US_List_OH_list.csv'
new_column_name = 'contain_keyword'

# Specify the list of words to check
words_to_check = ['warehouse', 'warehousing','pick']
print('the keywords are ')
print(words_to_check)


# read the CSV file
print('reading file: ', input_csv_file)
websites_df = pd.read_csv(input_csv_file)


# Add columns for new_column_name and 'checked' in the DataFrame if not present
if new_column_name not in websites_df.columns:
    websites_df[new_column_name] = None
if 'checked' not in websites_df.columns:
    websites_df['checked'] = False


# Find websites with specified words and update the DataFrame
result = find_words_in_websites(websites_df, words_to_check, input_csv_file)


# Save the updated DataFrame to the same CSV file
websites_df.to_csv(input_csv_file, index=False)

print("Websites mentioning specified words:")
for site in result:
    print(site)