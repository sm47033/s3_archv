import pandas as pd
import os
import logging
from datetime import datetime
import time
import io
 
os.system('cls' if os.name == 'nt' else 'clear')
 
MAX_FILE_SIZE_BYTES = 20*1024*1024*1024 #20GB Hard Limit
 
# ---------- Configuration ----------
local_source_prefix = r'C:\Users\AD46100\Downloads\2024'  # Root folder containing monthly folders
local_prefix_file = r'C:\Users\AD46100\Desktop\prefix_file.xlsx'
local_output_dir = r'C:\Users\AD46100\Desktop\output'
dry_run = False  # Set True to skip actual saving
 
# ---------- Logging ----------
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
print("-------------Program Started-----------------")
 
 
def get_output_filename(prefix):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"consolidated_{prefix}_{timestamp}.csv"
    return os.path.join(local_output_dir, filename)
 
 
def list_monthly_folders(root_folder):
    folders = [os.path.join(root_folder, d) for d in os.listdir(root_folder)
               if os.path.isdir(os.path.join(root_folder, d)) and d.isdigit() and len(d) == 6]
    return sorted(folders)
 
 
def list_files_in_folder(folder):
    files = []
    for root, _, filenames in os.walk(folder):
        for filename in filenames:
            files.append(os.path.join(root, filename))
    return files
 
 
def read_prefix_sheet(file_path):
    df = pd.read_excel(file_path)
    if 'prefix' not in df.columns:
        raise ValueError("Prefix sheet must contain a 'prefix' column.")
    return df['prefix'].astype(str).tolist()
 
 
 
def read_csv_file(file_path):
    try:
        encoding = 'utf-8'
        try:
            if file_path.lower().endswith('.txt'):
                df = pd.read_csv(file_path, sep='\t', encoding=encoding, low_memory=False)
            else:
                df = pd.read_csv(file_path, encoding=encoding, low_memory=False)
        except UnicodeDecodeError:
            # Fallback to a different encoding like cp1252
            fallback_encoding = 'cp1252'
            if file_path.lower().endswith('.txt'):
                df = pd.read_csv(file_path, sep='\t', encoding=fallback_encoding, low_memory=False)
            else:
                df = pd.read_csv(file_path, encoding=fallback_encoding, low_memory=False)
 
        if df.empty:
            raise ValueError("File is empty")
 
        return df
    except Exception as e:
        raise RuntimeError(f"Error reading {file_path}: {e}")
 
 
 
def save_dataframe(df, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    part = 1
    output_file = os.path.join(local_output_dir,f"{file_path}_part{part}.csv")
    mode = 'w'
    header = True
    current_file_size = 0
   
    for chunk_start in range(0, len(df),10000):
        chunk = df.iloc[chunk_start:chunk_start+10000]
       
        # Writing chunk to memory to estimate its size
        buffer = io.StringIO()
        chunk.to_csv(buffer, mode=mode,header=header,sep='\t',index=False)
        chunk_data = buffer.getvalue()
        chunk_size = len(chunk_data.encode('utf-8'))
        # total_written = os.path.getsize(output_file)
       
        if current_file_size + chunk_size > MAX_FILE_SIZE_BYTES:
            part +=1
            output_file = os.path.join(local_output_dir,f"{file_path}_part{part}.csv")
            mode = 'w'
            header = True
            current_file_size = 0
        with open(output_file,mode,encoding='utf-8') as f:
            f.write(chunk_data)
           
        current_file_size += chunk_size    
        mode = 'a'
        header = False
           
 
 
def consolidate_files():
    prefixes = read_prefix_sheet(local_prefix_file)
    monthly_folders = list_monthly_folders(local_source_prefix)
    master_log = []
    for prefix in prefixes:
        logging.info(f"\n--- Consolidating files for prefix: {prefix} ---")
        consolidated_df = pd.DataFrame()
 
        for folder in monthly_folders:
            period = os.path.basename(folder)
            files = list_files_in_folder(folder)
 
            matched_files = [f for f in files if os.path.basename(f).lower().startswith(prefix.lower())]
            if not matched_files:
                logging.info(f"No files for prefix '{prefix}' in folder '{period}'")
                continue
 
            for file_path in matched_files:
                try:
                    df = read_csv_file(file_path)
                    file_stats = os.stat(file_path)
                    file_size = round(file_stats.st_size/1024,2)
                    row_count, col_count = df.shape
                    timestamp = datetime.now()
                   
                    master_log.append({
                        'file_name':os.path.basename(file_path),
                        'file_location':file_path,
                        'month':period,
                        'row_count':row_count,
                        'col_count':col_count,
                        'file_size':file_size,
                        'timestamp':timestamp
                    })
                    df['period'] = period
                    consolidated_df = pd.concat([consolidated_df, df], ignore_index=True, sort=False)
                    logging.info(f"Appended: {file_path}")
                except Exception as e:
                    logging.error(f"Error reading {file_path}: {e}")
 
        if consolidated_df.empty:
            logging.warning(f"No data found for prefix: {prefix}")
        elif dry_run:
            logging.info(f"Dry run: Skipped saving for prefix {prefix}")
        else:
            output_file = get_output_filename(prefix)
            save_dataframe(consolidated_df, output_file)
   
    if master_log:
        master_df = pd.DataFrame(master_log)
        master_file_path = os.path.join(local_output_dir,f"master_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        master_df.to_csv(master_file_path, sep ='\t', index=False)
        logging.info(f"Master log saved to: {master_file_path}")
    else:
        logging.warning("No file information to save in master log")
 
# ---------- Run ----------
if __name__ == '__main__':
    start = datetime.now()
    consolidate_files()
    end = datetime.now()
    print(f"Total execution time: {end - start}")  
    print("-------------Program Completed-----------------")