import ydata_profiling
import pandas as pd
import os

data_dir = r'/home/mcavoyse/coursework/DAMG7370/final_proj/data'
save_dir = r'/home/mcavoyse/coursework/DAMG7370/final_proj/profiles'

os.makedirs(save_dir, exist_ok=True)

for fname in os.listdir(data_dir):
    print(f'Processing {fname}...')
    savefilename = fname.replace('.tsv', '_profile.html')
    df = pd.read_csv(os.path.join(data_dir, fname), sep='\t', nrows=1000000, na_values=['\\N'])
    ydata_profiling.ProfileReport(df).to_file(os.path.join(save_dir, savefilename))