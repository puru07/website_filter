#!/usr/bin/env python3

import pandas as pd
import json
import os

def read_csv(file_path):
    print('Reading tehe data')
    return pd.read_csv(file_path)

def print_data_for_company(dataframe, company_name):
    # Print data for a specific company name
    company_data = dataframe[dataframe['Company Name'] == company_name]
    print(f'Data for {company_name}:\n{company_data.to_string(index=False)}\n')


def create_company_data_structure(dataframe, group_by_column='Company Name'):
    print('creating the dataframe')
    # Group by the specified column and aggregate information
    grouped_data = dataframe.groupby(group_by_column).agg(lambda x: x.tolist()).reset_index()

    # Convert the grouped DataFrame to a dictionary
    #company_data_structure = grouped_data.set_index(group_by_column).to_dict(orient='index')

    return grouped_data

def save_to_json(data, json_file_path):
    with open(json_file_path, 'w') as jsonfile:
        json.dump(data, jsonfile, indent=2)

def save_to_csv(dataframe, csv_file_path):
    dataframe.to_csv(csv_file_path, index=False)

def scrape_and_save_websites(dataframe, output_directory='websites'):
    # Create the output directory if it doesn't exist
    os.makedirs(output_directory, exist_ok=True)

    for _, row in dataframe.iterrows():
        company_name = row['Company Name']
        website_url = row['Website']
        output_file_path = os.path.join(output_directory, f'{company_name}_website.html')

        # Skip if the website URL is not available
        if pd.notna(website_url):
            try:
                # Fetch the website content
                response = requests.get(website_url)
                response.raise_for_status()

                # Save the website content to a file
                with open(output_file_path, 'w', encoding='utf-8') as file:
                    file.write(response.text)

                print(f'Successfully saved website content for {company_name} to {output_file_path}')
            except requests.exceptions.RequestException as e:
                print(f'Error fetching website content for {company_name}: {e}')

def main():
    csv_file_path = 'Warehouse_US_List.csv'
    output_csv_file_path = 'output_Warehouse_US_List.csv'
    output_website_directory = 'websites'

    # Read CSV into a DataFrame
    company_data = read_csv(csv_file_path)

    # Print the entire DataFrame
    # print('Entire DataFrame:')
    #print(company_data.to_string(index=False))

    # Create a data structure grouped by 'Company Name'
    grouped_data = create_company_data_structure(company_data, group_by_column='Company Name')

    # Save grouped data to CSV file
    save_to_csv(grouped_data, output_csv_file_path)

    # Print data for a specific company name (replace 'Specific Company' with the desired company name)
    specific_company_name = 'Dhl Group'
    print_data_for_company(company_data, specific_company_name)

    # Scrape and save websites
    #scrape_and_save_websites(company_data, output_directory=output_website_directory)

    print(f'Data has been successfully extracted from the CSV file, saved to {output_csv_file_path}, and website content saved to {output_website_directory}.')

if __name__ == "__main__":
    main()
