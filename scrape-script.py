import pandas as pd
from bs4 import BeautifulSoup
import requests

# Fetch username and password from environment variables
username = "jayshah36262@gmail.com"
password = "Jayshah12"

# Fetch data
session = requests.Session()
login_url = "https://www.screener.in/login/?"
login_page = session.get(login_url)
soup = BeautifulSoup(login_page.content, 'html.parser')

# Extract CSRF token
csrf_token = soup.find('input', {'name': 'csrfmiddlewaretoken'})['value']
login_payload = {
    'username': username,
    'password': password,
    'csrfmiddlewaretoken': csrf_token
}

headers = {
    'Referer': login_url,
    'User-Agent': 'Mozilla/5.0'
}

# Perform login
response = session.post(login_url, data=login_payload, headers=headers)

if response.url == "https://www.screener.in/dash/":
    search_url = "https://www.screener.in/company/RELIANCE/consolidated/"
    search_response = session.get(search_url)
    
    if search_response.status_code == 200:
        print("Reliance data retrieved successfully")
        soup = BeautifulSoup(search_response.content, 'html.parser')
        table1 = soup.find('section', {'id': 'balance-sheet'})
        table = table1.find('table')
        
        if table:
            headers = [th.text.strip() or f'Column_{i}' for i, th in enumerate(table.find_all('th'))]
            rows = table.find_all('tr')
            row_data = []
            
            for row in rows[1:]:
                cols = row.find_all('td')
                cols = [col.text.strip() for col in cols]
                if len(cols) == len(headers):
                    row_data.append(cols)
                else:
                    print(f"Row data length mismatch: {cols}")
            
            # Create a DataFrame with sanitized headers
            df = pd.DataFrame(row_data, columns=headers)

            # Rename the first column to 'Narration'
            if not df.empty:
                df.columns = ['Narration'] + df.columns[1:].tolist()

            # Drop the index column if it exists
            df = df.reset_index(drop=True)

            # Print the DataFrame
            print(df.head(16))
            df.to_csv('reliance_balance_sheet.csv', index=False)
        else:
            print("Failed to find the data table.")
    else:
        print(f"Failed to retrieve Reliance data. Status Code: {search_response.status_code}")
else:
    print("Login failed.")
