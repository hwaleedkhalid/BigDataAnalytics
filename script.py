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
