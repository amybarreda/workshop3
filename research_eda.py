import pandas as pd
import numpy as np

years = ['2015', '2016', '2017', '2018', '2019']
with open("eda_research_results.txt", "w") as f:
    for y in years:
        f.write(f"========== YEAR {y} ==========\n")
        df = pd.read_csv(f"data/raw/{y}.csv")
        f.write(f"Shape: {df.shape}\n")
        f.write(f"Duplicates: {df.duplicated().sum()}\n")
        
        f.write("\nMissing values:\n")
        missing = df.isnull().sum()
        f.write(missing[missing > 0].to_string() + "\n")
        
        f.write("\nData Types:\n")
        f.write(df.dtypes.to_string() + "\n")
        
        f.write("\nOutliers (Numeric Describe):\n")
        num_df = df.select_dtypes(include=[np.number])
        f.write(num_df.describe().T[['min', 'max', 'mean', 'std']].to_string() + "\n")
        f.write("\n")
