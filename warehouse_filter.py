#!/usr/bin/env python3

import requests
from bs4 import BeautifulSoup

def find_warehouse_websites(websites):
    warehouse_sites = []
    list_to_check = ['warehouse', 'warehousing','pick']
    for site in websites:
        try:
            response = requests.get(site)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Check if the word 'warehouse' is present in the HTML content
            for item in list_to_check:
                if item in soup.get_text().lower():
                    warehouse_sites.append(site)
                    break
        except requests.exceptions.RequestException as e:
            print(f"Error accessing {site}: {e}")

    return warehouse_sites

# Example usage:
websites_to_check = ["http://www.shbellco.com", "http://www.lanxess.us", "http://www.storexpressselfstorage.com"]
result = find_warehouse_websites(websites_to_check)

print("Websites mentioning 'warehouse':")
for site in result:
    print(site)