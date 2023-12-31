#!/usr/bin/env python3
# author: Puru Rastogi

import pandas as pd
import requests
from bs4 import BeautifulSoup

def find_words_in_websites(websites_df, input_csv_file, warehouse_words, fulfilment_words, warehouse_column_name, fulfilment_column_name):
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
            websites_df.at[index, warehouse_column_name] = 'Invalid Website'
            websites_df.at[index, fulfilment_column_name] = 'Invalid Website'
            print("updating csv file of name ",input_csv_file)
            websites_df.to_csv(input_csv_file, index=False)
            print('update done')
            continue
        
        # bool to check if the row is getting repeated
        site_in_set = False
        if site not in web_set:
            web_set[site] = [None, None]
        else:
            site_in_set = True

        # Skip websites that have already been checked
        if row.get('checked') == True:
            # if the website was not in set, read the previous value and add to the set.
            if not site_in_set:
                web_set[site] = [row.get(warehouse_column_name), row.get(fulfilment_column_name)]

            if index%10 == 0:
                percentage_completion = ((index*100)//total_websites)
                print(".............percent completed:  ", percentage_completion)
            continue

        try:
            if site_in_set:
                websites_df.at[index, 'checked'] = True
                websites_df.at[index, warehouse_column_name] = web_set[site][0]
                websites_df.at[index, fulfilment_column_name] = web_set[site][1]
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
            
            # Check if any of the warehouse words in the list are present in the HTML content
            if any(word.lower() in soup.get_text().lower() for word in warehouse_words):
                websites_with_words.append(site)
                websites_df.at[index, warehouse_column_name] = 'True'
                web_set[site][0] = 'True'
                print(f"waerhouse {site}: ", True)
            else:
                websites_df.at[index, warehouse_column_name] = 'False'
                web_set[site][0] = 'False'
                print(f"waerhouse {site}: ", False)
            # Check if any of the fulfilment words in the list are present in the HTML content
            if any(word.lower() in soup.get_text().lower() for word in fulfilment_words):
                websites_with_words.append(site)
                websites_df.at[index, fulfilment_column_name] = 'True'
                web_set[site][1] = 'True'
                print(f"fulfilment {site}: ", True)
            else:
                websites_df.at[index, fulfilment_column_name] = 'False'
                web_set[site][1] = 'False'
                print(f"fulfilment {site}: ", False)
            # Mark the website as checked
            websites_df.at[index, 'checked'] = True

        except requests.exceptions.RequestException as e:
            print(f"{site}: ", "Invalid")
            websites_df.at[index, warehouse_column_name] = 'Invalid Website'
            websites_df.at[index, warehouse_column_name] = 'Invalid Website'
            web_set[site] = ['Invalid Website','Invalid Website']
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
input_csv_file = 'output_3PL_US_List_OH_list.csv'
warehouse_column_name = 'warehouse_words'
fulfilment_column_name = 'fulfilment_words'

# Specify the list of words to check
warehouse_words = ['warehouse', 'warehousing']
fulfilment_words = ['fulfilment', 'fulfillment','ecommerce','e-commerce']
print('the keywords are ')
print(warehouse_words)
print(fulfilment_words)


# read the CSV file
print('reading file: ', input_csv_file)
websites_df = pd.read_csv(input_csv_file)


# Add columns for new_column_name and 'checked' in the DataFrame if not present
if warehouse_column_name not in websites_df.columns:
    websites_df[warehouse_column_name] = None
if fulfilment_column_name not in websites_df.columns:
    websites_df[fulfilment_column_name] = None
if 'checked' not in websites_df.columns:
    websites_df['checked'] = False


# Find websites with specified words and update the DataFrame
result = find_words_in_websites(websites_df, input_csv_file, warehouse_words, fulfilment_words, warehouse_column_name, fulfilment_column_name)


# Save the updated DataFrame to the same CSV file
websites_df.to_csv(input_csv_file, index=False)

print("Websites mentioning specified words:")
for site in result:
    print(site)