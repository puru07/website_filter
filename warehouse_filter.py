#!/usr/bin/env python3
# author: Puru Rastogi

import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

def update_df(index, df, checked, invalid_website, warehouse, fulfilment, warehouse_column_name, fulfilment_column_name):
    df.at[index, 'checked'] = checked
    if invalid_website:
        df.at[index, warehouse_column_name] = 'invalid_website'
        df.at[index, fulfilment_column_name] = 'invalid_website'
        return
    df.at[index, warehouse_column_name] = str(warehouse)
    df.at[index, fulfilment_column_name] = str(fulfilment)


def check_and_save_csv(idx, websites_df, input_csv_file, total_websites):
    if idx%10 == 0:
        percentage_completion = ((idx*100)//total_websites)
        print(".............percent completed:  ", percentage_completion)
        print("updating csv file of name ",input_csv_file)
        websites_df.to_csv(input_csv_file, index=False)
        print('update done')


def find_words_in_websites(websites_df, input_csv_file, warehouse_words, fulfilment_words, warehouse_column_name, fulfilment_column_name):
    websites_with_words = []

    contains_warehouse_words = False  # Flag to track if any subpage contains the words
    contains_fulfilment_words = False  # Flag to track if any subpage contains the words
    
    total_websites = len(websites_df.index)
    print('total websites: ',total_websites)
    # dict to avoid repeating the websites
    web_set = {}
    for index, row in websites_df.iterrows():

        site = row['Website']
        if pd.isna(site) or not site.strip():
            #print("missing website for ", row['Company Name'])
            websites_df.at[index, 'checked'] = False
            websites_df.at[index, warehouse_column_name] = 'Invalid Website'
            websites_df.at[index, fulfilment_column_name] = 'Invalid Website'
            # print("updating csv file of name ",input_csv_file)
            websites_df.to_csv(input_csv_file, index=False)
            #print('update done')
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
            continue

        try:
            if site_in_set:
                update_df(index, websites_df, True, False, web_set[site][0], web_set[site][1], warehouse_column_name, fulfilment_column_name)
                # print(f"site got repeated: {site}")
                check_and_save_csv(index, websites_df, input_csv_file, total_websites)

                continue

            # checking the websites

            response = requests.get(site, timeout=10)
            response.raise_for_status()  # Raise an exception for HTTP errors

            soup = BeautifulSoup(response.text, 'html.parser')
            #check the main site
            if any(word.lower() in soup.get_text().lower() for word in warehouse_words):
                contains_warehouse_words = True
                websites_with_words.append(site)

            if any(word.lower() in soup.get_text().lower() for word in fulfilment_words):
                contains_fulfilment_words = True
                websites_with_words.append(site)

            if contains_warehouse_words and contains_fulfilment_words:
                
                # Mark the website as checked in this run
                update_df(index, websites_df, True, False, contains_warehouse_words, contains_fulfilment_words, warehouse_column_name, fulfilment_column_name)
                web_set[site] = [str(contains_warehouse_words), str(contains_fulfilment_words)]
                check_and_save_csv(index, websites_df, input_csv_file, total_websites)
                continue
                
            # Extract all links from the main page
            links = [a.get('href') for a in soup.find_all('a', href=True)]

            # Iterate through each linked page
            print(f"----------------going inside the subdomains for {site}")
            for link in links:
                absolute_url = urljoin(site, link)
                
                
                # Check if the link is within the same domain
                if urlparse(absolute_url).netloc == urlparse(site).netloc:
                    try:
                        subpage_response = requests.get(absolute_url, timeout=10)
                        subpage_response.raise_for_status()  # Raise an exception for HTTP errors

                        subpage_soup = BeautifulSoup(subpage_response.text, 'html.parser')

                        # Check if any of the words in the first list are present in the HTML content
                        if any(word.lower() in subpage_soup.get_text().lower() for word in warehouse_words):
                            contains_warehouse_words = True
                            websites_with_words.append(absolute_url)

                        # Check if any of the words in the second list are present in the HTML content
                        if any(word.lower() in subpage_soup.get_text().lower() for word in fulfilment_words):
                            contains_fulfilment_words = True
                            websites_with_words.append(absolute_url)

                        # Break if both subpage lists are found
                        if contains_warehouse_words and contains_fulfilment_words:
                            break
                    except requests.exceptions.RequestException as e:
                        continue
\
                        

            # Mark the website as checked in this run
            update_df(index, websites_df, True, False,contains_warehouse_words, contains_fulfilment_words, warehouse_column_name, fulfilment_column_name)
            web_set[site] = [str(contains_warehouse_words), str(contains_fulfilment_words)]
            check_and_save_csv(index, websites_df, input_csv_file, total_websites)
        
        except requests.exceptions.RequestException as e:
            print(f"{site}: invalid")
            # Mark the website as inaccessible
            update_df(index, websites_df, True, True, contains_warehouse_words, contains_fulfilment_words, warehouse_column_name, fulfilment_column_name)
            web_set[site] = ['Invalid Website','Invalid Website']
            check_and_save_csv(index, websites_df, input_csv_file, total_websites)

    return websites_with_words


# Read the CSV file
input_csv_file = 'warehouse_pa_no_warehouse.csv'
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