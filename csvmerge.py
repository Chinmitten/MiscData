#!/usr/bin/env python3
import pandas as pd
import sys

def merge_csv_by_email(main_csv, source_csv, email_col_main='email', email_col_source='email', column_to_add='department', output_csv='merged_output.csv'):
    """
    Merge two CSV files based on email identifier.
    
    Parameters:
    - main_csv: Path to your main CSV file
    - source_csv: Path to CSV with the column you want to add
    - email_col_main: Email column name in main CSV (default: 'email')
    - email_col_source: Email column name in source CSV (default: 'email') 
    - column_to_add: Name of column to add from source CSV (default: 'department')
    - output_csv: Output file name (default: 'merged_output.csv')
    """
    
    try:
        # Read CSV files
        print(f"Reading {main_csv}...")
        df_main = pd.read_csv(main_csv)
        
        print(f"Reading {source_csv}...")
        df_source = pd.read_csv(source_csv)
        
        print(f"Main CSV: {df_main.shape[0]} rows, {df_main.shape[1]} columns")
        print(f"Source CSV: {df_source.shape[0]} rows, {df_source.shape[1]} columns")
        
        # Merge based on email
        print(f"Merging on email columns...")
        df_merged = df_main.merge(
            df_source[[email_col_source, column_to_add]], 
            left_on=email_col_main, 
            right_on=email_col_source, 
            how='left'
        )
        
        # Remove duplicate email column
        if email_col_source != email_col_main:
            df_merged = df_merged.drop(columns=[email_col_source])
        
        # Show results
        matched = df_merged[column_to_add].notna().sum()
        total = len(df_merged)
        
        print(f"\nResults:")
        print(f"Total rows: {total}")
        print(f"Successfully matched: {matched}")
        print(f"Unmatched rows: {total - matched}")
        
        # Save result
        df_merged.to_csv(output_csv, index=False)
        print(f"\nSaved merged data to: {output_csv}")
        
        return df_merged
        
    except Exception as e:
        print(f"Error: {e}")
        return None

# Example usage - modify these paths and column names as needed:
if __name__ == "__main__":
    # CHANGE THESE TO YOUR ACTUAL FILE PATHS AND COLUMN NAMES:
    main_file = "opptracker.csv"           # Your main CSV file
    source_file = "eaejoin.csv"       # CSV with the column to add
    email_col_main = "Email"              # Email column name in main file
    email_col_source = "Email"            # Email column name in source file  
    column_to_add = "EAE"	          # Column to add from source file
    output_file = "merged_result.csv"     # Output file name
    
    # Run the merge
    result = merge_csv_by_email(
        main_file, 
        source_file, 
        email_col_main, 
        email_col_source, 
        column_to_add, 
        output_file
    )
    
    if result is not None:
        print("\nMerge completed successfully!")
    else:
        print("Merge failed!")
