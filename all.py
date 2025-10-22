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






from hdfs import InsecureClient


client = InsecureClient('http://localhost:9870', user='root')


def create_file(hdfs_path, local_content):
    try:
        with client.write(hdfs_path, encoding='utf-8', overwrite=True) as writer:
            writer.write(local_content)
        print(f" [CREATE] Successfully created file: {hdfs_path}")
    except Exception as e:
        print(f" [CREATE] Error creating file: {e}")


def read_file(hdfs_path):
    try:
        with client.read(hdfs_path, encoding='utf-8') as reader:
            content = reader.read()
        print(f" [READ] Content of {hdfs_path}:\n---\n{content}\n---")
        return content
    except Exception as e:
        print(f"  [READ] Error reading file: {e}")
        return None

def append_to_file(hdfs_path, local_content):

    try:
        with client.write(hdfs_path, encoding='utf-8', append=True) as writer:
            writer.write(local_content)
        print(f" [UPDATE] Successfully appended to file: {hdfs_path}")
    except Exception as e:
        print(f"  [UPDATE] Error appending to file: {e}")

def delete_file(hdfs_path):
    try:
        if client.status(hdfs_path, strict=False):
            client.delete(hdfs_path)
            print(f" [DELETE] Successfully deleted file: {hdfs_path}")
        else:
            print(f" [DELETE] File not found, nothing to delete: {hdfs_path}")
    except Exception as e:
        print(f"  [DELETE] Error deleting file: {e}")

def list_files(hdfs_path):
    try:
        files = client.list(hdfs_path)
        print(f" [LIST] Files in '{hdfs_path}': {files}")
        return files
    except Exception as e:
        print(f"  [LIST] Error listing files: {e}")
        return []


if __name__ == "__main__":
    
    hdfs_dir = '/user/test'
    hdfs_filepath = f"{hdfs_dir}/my_test_file.txt"
    
    print("--- Starting HDFS CRUD Operations ---")
    
    
    if client.status(hdfs_dir, strict=False):
       client.delete(hdfs_dir, recursive=True)
       print(f" Cleaned up existing directory: {hdfs_dir}")
    client.makedirs(hdfs_dir)


    list_files(hdfs_dir)


    initial_content = "Hello from Lahore!\nThis is the first line."
    create_file(hdfs_filepath, initial_content)

    read_file(hdfs_filepath)

    content_to_append = "\nThis is a new line, added on a Thursday night."
    append_to_file(hdfs_filepath, content_to_append)

    read_file(hdfs_filepath)
    
    list_files(hdfs_dir)

    delete_file(hdfs_filepath)
    
    list_files(hdfs_dir)

    print("\n--- HDFS CRUD Operations Complete ---")







import os
from dotenv import load_dotenv
from pymongo import MongoClient
from bson.objectid import ObjectId

# Load environment variables
load_dotenv()

class MongoCRUD:
    def __init__(self):
        """Initialize MongoDB connection"""
        mongo_user = os.getenv("MONGO_ROOT_USERNAME")
        mongo_pass = os.getenv("MONGO_ROOT_PASSWORD")
        mongo_host = os.getenv("MONGO_CONTAINER_NAME", "localhost")
        mongo_port = os.getenv("MONGO_PORT", "27017")

        mongo_uri = f"mongodb://{mongo_user}:{mongo_pass}@{mongo_host}:{mongo_port}/"
        self.client = MongoClient(mongo_uri)
        self.db = self.client["user_db"]  # Database name
        self.collection = self.db["login"]  # "login table" (collection)
        print("✅ Connected to MongoDB.")

    def create_user(self, username, password):
        """Insert a new user (signup)"""
        # Check if username exists
        if self.collection.find_one({"username": username}):
            print("⚠️ Username already exists.")
            return None

        user = {"username": username, "password": password}
        result = self.collection.insert_one(user)
        print(f"✅ User '{username}' created with ID: {result.inserted_id}")
        return str(result.inserted_id)

    def read_users(self):
        """Return all users"""
        users = list(self.collection.find({}, {"password": 0}))  # exclude passwords
        print("📋 Users:")
        for user in users:
            print(user)
        return users

    def read_user(self, username):
        """Return a single user by username"""
        user = self.collection.find_one({"username": username}, {"password": 0})
        if user:
            print(f"👤 Found user: {user}")
        else:
            print("⚠️ User not found.")
        return user

    def update_user(self, username, new_data):
        """Update a user's info"""
        result = self.collection.update_one({"username": username}, {"$set": new_data})
        if result.modified_count:
            print(f"🛠️ User '{username}' updated.")
        else:
            print("⚠️ No user updated (maybe not found).")
        return result.modified_count

    def delete_user(self, username):
        """Delete a user"""
        result = self.collection.delete_one({"username": username})
        if result.deleted_count:
            print(f"🗑️ User '{username}' deleted.")
        else:
            print("⚠️ No user found.")
        return result.deleted_count

    def login(self, username, password):
        """Simple login check"""
        user = self.collection.find_one({"username": username, "password": password})
        if user:
            print(f"🔓 Login successful for '{username}'.")
            return True
        else:
            print("❌ Invalid username or password.")
            return False


if __name__ == "__main__":
    crud = MongoCRUD()

    # Create users
    crud.create_user("alice", "password123")
    crud.create_user("bob", "mypassword")

    # Read all users
    crud.read_users()

    # Read a specific user
    crud.read_user("alice")

    # Update user
    crud.update_user("alice", {"password": "newpass123"})

    # Login test
    crud.login("alice", "wrongpass")
    crud.login("alice", "newpass123")

    # Delete a user
    crud.delete_user("bob")

    # Final list
    crud.read_users()