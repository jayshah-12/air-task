# Import pandas library
import pandas as pd

# Define the path to your Excel file
# Use a raw string for Windows paths or use double backslashes
excel_file_path = r'glue\reliance_data.xlsx'

# Read the Excel file
# You can specify the sheet name if needed
df = pd.read_excel(excel_file_path, sheet_name='Balance Sheet')  # Replace 'Balance Sheet' with your sheet name if needed

# Show the DataFrame
print(df)
print("Jay")
print("hahahaha")