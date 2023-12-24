#!/usr/bin/env python3

list_to_check = ['warehouse', 'warehousing','pick']

import pandas as pd
import requests
from bs4 import BeautifulSoup

def find_words_in_websites(websites_df, words_to_check):
    websites_with_words = []

    for index, row in websites_df.iterrows():
        site = row['Website']
        try:
            response = requests.get(site)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if any of the words in the list are present in the HTML content
            if any(word.lower() in soup.get_text().lower() for word in words_to_check):
                websites_with_words.append(site)
                websites_df.at[index, 'contains_words'] = True
                print(site)
            else:
                websites_df.at[index, 'contains_words'] = False
        except requests.exceptions.RequestException as e:
            print(f"Error accessing {site}: {e}")
            websites_df.at[index, 'contains_words'] = False

    return websites_with_words

# Read the CSV file
input_csv_file = 'Warehouse_US_List_PA_list.csv'
output_csv_file = 'output_websites.csv'
websites_df = pd.read_csv(input_csv_file)

# Specify the list of words to check
words_to_check = ['warehouse', 'warehousing','pick']

# Add a column for 'contains_words' in the DataFrame
websites_df['contains_words'] = None

# Find websites with specified words and update the DataFrame
result = find_words_in_websites(websites_df, words_to_check, websites_df)

# Save the updated DataFrame to a new CSV file
websites_df.to_csv(output_csv_file, index=False)

print("Websites mentioning specified words:")
for site in result:
    print(site)