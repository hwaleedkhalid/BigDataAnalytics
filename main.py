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