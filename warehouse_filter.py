#!/usr/bin/env python3
# author: Puru Rastogi

list_to_check = ['warehouse', 'warehousing','pick']

import pandas as pd
import requests
from bs4 import BeautifulSoup

def find_words_in_websites(websites_df, words_to_check,output_csv_file):
    websites_with_words = []
    total_websites = len(websites_df.index)
    print('total websites: ',total_websites)
    for index, row in websites_df.iterrows():
        site = row['Website']
        try:
            response = requests.get(site)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if any of the words in the list are present in the HTML content
            if any(word.lower() in soup.get_text().lower() for word in words_to_check):
                websites_with_words.append(site)
                websites_df.at[index, new_column_name] = True
                print(site)
            else:
                websites_df.at[index, new_column_name] = False
        except requests.exceptions.RequestException as e:
            print(f"Error accessing {site}: {e}")
            websites_df.at[index, new_column_name] = False

        if index%10 == 0:
            percentage_completion = ((index*100)//total_websites)
            print(".............percent completed:  ", percentage_completion)
            print("updating csv file of name ",output_csv_file)
            websites_df.to_csv(output_csv_file, index=False)

    return websites_with_words

# Read the CSV file
input_csv_file = 'Warehouse_US_List_PA_list.csv'
output_csv_file = 'output_Warehouse_US_List_PA_list.csv'
new_column_name = 'contain_keyword'
words_to_check = ['warehouse', 'warehousing','pick']
print('the keywords are ')
print(words_to_check)

print('reading file: ', input_csv_file)
websites_df = pd.read_csv(input_csv_file)

# Specify the list of words to check


# Add a column for new_column_name in the DataFrame
websites_df[new_column_name] = None

# Find websites with specified words and update the DataFrame
result = find_words_in_websites(websites_df, words_to_check,output_csv_file)

# Save the updated DataFrame to a new CSV file
websites_df.to_csv(output_csv_file, index=False)

print("Websites mentioning specified words:")
for site in result:
    print(site)