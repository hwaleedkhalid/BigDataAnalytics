import pandas as pd
import io
from hdfs import InsecureClient

# Connect to HDFS
client = InsecureClient('http://localhost:9870', user='root')

# HDFS file path
hdfs_dir = '/user/test'
hdfs_path = f'{hdfs_dir}/my_data.parquet'

# Create HDFS directory if not exists
if not client.status(hdfs_dir, strict=False):
    client.makedirs(hdfs_dir)

# CREATE: Write DataFrame to HDFS as Parquet
def create_parquet_file():
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Ali', 'Sara', 'John']
    })
    
    buffer = io.BytesIO()
    df.to_parquet(buffer, engine='pyarrow')
    buffer.seek(0)

    with client.write(hdfs_path, overwrite=True) as writer:
        writer.write(buffer.read())
    
    print(f"✅ [CREATE] Wrote Parquet file to: {hdfs_path}")

# READ: Read Parquet file from HDFS
def read_parquet_file():
    try:
        with client.read(hdfs_path) as reader:
            buffer = io.BytesIO(reader.read())
            df = pd.read_parquet(buffer, engine='pyarrow')
        print(f"📖 [READ] Content of {hdfs_path}:\n{df}")
        return df
    except Exception as e:
        print(f"❌ [READ] Failed: {e}")

# UPDATE: Add new rows and overwrite Parquet file
def update_parquet_file():
    try:
        df_existing = read_parquet_file()
        df_new = pd.DataFrame({
            'id': [4, 5],
            'name': ['Aisha', 'Zain']
        })
        df_updated = pd.concat([df_existing, df_new], ignore_index=True)

        buffer = io.BytesIO()
        df_updated.to_parquet(buffer, engine='pyarrow')
        buffer.seek(0)

        with client.write(hdfs_path, overwrite=True) as writer:
            writer.write(buffer.read())

        print(f"🔄 [UPDATE] Appended new data and updated file.")
    except Exception as e:
        print(f"❌ [UPDATE] Failed: {e}")

# DELETE: Delete Parquet file from HDFS
def delete_parquet_file():
    try:
        if client.status(hdfs_path, strict=False):
            client.delete(hdfs_path)
            print(f"🗑️ [DELETE] Deleted: {hdfs_path}")
        else:
            print(f"⚠️ [DELETE] File not found.")
    except Exception as e:
        print(f"❌ [DELETE] Failed: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    print("\n--- HDFS Parquet CRUD Operations ---")

    create_parquet_file()   # Step 1: Create
    read_parquet_file()     # Step 2: Read
    update_parquet_file()   # Step 3: Update (Append + Overwrite)
    read_parquet_file()     # Step 4: Read again to confirm update
    delete_parquet_file()   # Step 5: Delete
