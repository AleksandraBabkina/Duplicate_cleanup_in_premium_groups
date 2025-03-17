# Duplicate_cleanup_in_premium_groups
## Description
An insurance premium is a monetary amount that the client (policyholder) agrees to pay regularly or in a lump sum to the insurance company in exchange for the company’s commitment to compensate for losses in the event of an insured event. These contributions are the primary source of income for the insurer and serve to form a financial “cushion” for potential insurance payouts.

This program identifies and removes duplicate records in insurance premium groups by consolidating similar rows based on predefined criteria to ensure data consistency. It operates on insurance data, specifically focusing on groups of premiums, such as auto and health insurance, and it organizes data based on different agent categories, regions, and premium types. The program connects to an Oracle database, extracts relevant data, and processes it to remove duplicate or conflicting entries, ensuring only the most accurate and representative data remains for further analysis.

## Functional Description
The program performs the following steps:
1. Retrieves data from the Oracle database.
2. Identifies duplicate records within insurance premium groups based on predefined rules.
3. Consolidates similar entries into a single representative row.
4. Ensures the consistency and accuracy of insurance premium data.
5. Outputs the cleaned data for further inspection or reporting.

## How It Works
1. The program connects to an Oracle database using SQLAlchemy and executes queries to extract necessary data related to insurance premium groups.
2. For each insurance group, it processes the data by analyzing it for duplicate or conflicting entries.
3. It consolidates rows with overlapping or similar values, such as different insurance agent types or regions, into one row for clarity and accuracy.
4. The results are returned in a cleaned format with duplicates removed and data consolidated.

## Input Structure
To run the program, the following parameters need to be provided:
1. Database credentials: Username, Password, Database DSN (Data Source Name)
2. Column categories: String columns to check for duplicates and numerical columns for premium amounts or counts.
The program is designed to work with specific tables within the Oracle database that contain insurance premium group data.

## Technical Requirements
To run the program, the following are required:
1. Python 3.x
2. Installed libraries: sqlalchemy, pandas, numpy, IPython
3. Oracle Database with the relevant insurance premium group data.

## Usage
1. Modify the username, password, and dsn values to connect to your Oracle database.
2. Run the program. It will clean the data by removing duplicate records within insurance premium groups.
3. The script will output the cleaned dataset with duplicates removed and data consolidated.

## Example Output
Cleaned Data: 
  A dataset with insurance premium group records, where similar rows have been merged into one, ensuring consistency and accuracy.

## Conclusion
This tool helps to identify and clean duplicate records within insurance premium groups, enhancing the quality of the dataset for subsequent analysis or reporting. The process improves data consistency, making it more reliable for further insurance premium calculations or policy assessments.
