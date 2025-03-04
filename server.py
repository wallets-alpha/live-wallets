import json
import time
import threading
import websocket
from pyee import EventEmitter
import sqlite3
import os
import csv
from datetime import datetime
import subprocess
import hashlib
from auto_git_push import GitManager

class WebSocketService:
    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self.socket = None
        self.transaction_socket = None
        self.reconnect_attempts = 0
        self.reconnect_delay = 2.5
        self.reconnect_delay_max = 4.5
        self.randomization_factor = 0.5
        self.emitter = EventEmitter()
        self.subscribed_rooms = set()
        self.transactions = set()
        self.connect()

    def connect(self):
        try:
            self.socket = websocket.WebSocketApp(
                self.ws_url,
                on_open=lambda ws: self.on_open(ws, "main"),
                on_close=lambda ws: self.on_close(ws, "main"),
                on_message=self.on_message
            )
            self.transaction_socket = websocket.WebSocketApp(
                self.ws_url,
                on_open=lambda ws: self.on_open(ws, "transaction"),
                on_close=lambda ws: self.on_close(ws, "transaction"),
                on_message=self.on_message
            )
            threading.Thread(target=self.socket.run_forever, daemon=True).start()
            threading.Thread(target=self.transaction_socket.run_forever, daemon=True).start()
        except Exception as e:
            print(f"Error connecting to WebSocket: {e}")
            self.reconnect()

    def on_open(self, ws, socket_type):
        self.resubscribe_to_rooms()

    def on_close(self, ws, socket_type):
        if socket_type == "main":
            self.socket = None
        elif socket_type == "transaction":
            self.transaction_socket = None
        self.reconnect()

    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("type") == "message" and data.get("data"):
                self.emitter.emit(data.get("room", "unknown"), data.get("data"))
        except Exception as e:
            pass

    def disconnect(self):
        if self.socket:
            self.socket.close()
            self.socket = None
        if self.transaction_socket:
            self.transaction_socket.close()
            self.transaction_socket = None
        self.subscribed_rooms.clear()
        self.transactions.clear()

    def reconnect(self):
        delay = min(self.reconnect_delay * (2 ** self.reconnect_attempts), self.reconnect_delay_max)
        jitter = delay * self.randomization_factor
        reconnect_delay = delay + (jitter * (2 * time.time() % 1 - 0.5))
        def delayed_reconnect():
            time.sleep(reconnect_delay)
            self.reconnect_attempts += 1
            self.connect()
        threading.Thread(target=delayed_reconnect, daemon=True).start()

    def join_room(self, room: str):
        self.subscribed_rooms.add(room)
        socket = self.transaction_socket if "transaction" in room else self.socket
        if socket and socket.sock and socket.sock.connected:
            socket.send(json.dumps({"type": "join", "room": room}))

    def leave_room(self, room: str):
        self.subscribed_rooms.discard(room)
        socket = self.transaction_socket if "transaction" in room else self.socket
        if socket and socket.sock and socket.sock.connected:
            socket.send(json.dumps({"type": "leave", "room": room}))

    def on(self, room: str, listener):
        self.emitter.on(room, listener)

    def off(self, room: str, listener):
        self.emitter.remove_listener(room, listener)

    def resubscribe_to_rooms(self):
        if (self.socket and self.socket.sock and self.socket.sock.connected and
            self.transaction_socket and self.transaction_socket.sock and self.transaction_socket.sock.connected):
            for room in self.subscribed_rooms:
                socket = self.transaction_socket if "transaction" in room else self.socket
                socket.send(json.dumps({"type": "join", "room": room}))

class WalletDatabase:
    def __init__(self, db_path='wallet_addresses.db'):
        self.db_path = db_path
        self.init_db()
        
    def init_db(self):
        """Initialize the database with the required table if it doesn't exist."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS wallets (
            wallet_address TEXT PRIMARY KEY,
            timestamp INTEGER,
            amount REAL
        )
        ''')
        conn.commit()
        conn.close()
        
    def add_wallet(self, wallet_address, timestamp, amount):
        """Add a wallet address to the database or update if it already exists."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if wallet exists and delete if it does
        cursor.execute("DELETE FROM wallets WHERE wallet_address = ?", (wallet_address,))
        
        # Insert the new wallet record
        cursor.execute(
            "INSERT INTO wallets (wallet_address, timestamp, amount) VALUES (?, ?, ?)",
            (wallet_address, timestamp, amount)
        )
        
        conn.commit()
        conn.close()
        
        # Check if we need to clean up (exceeding 500k addresses)
        self.cleanup_database()
        
    def cleanup_database(self):
        """Ensure the database doesn't exceed 500k addresses."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get the total count
        cursor.execute("SELECT COUNT(*) FROM wallets")
        count = cursor.fetchone()[0]
        
        if count > 500000:
            # Delete oldest entries to get back to 500k
            delete_count = count - 500000
            cursor.execute(
                "DELETE FROM wallets WHERE wallet_address IN "
                "(SELECT wallet_address FROM wallets ORDER BY timestamp ASC LIMIT ?)",
                (delete_count,)
            )
            print(f"Cleaned up database: removed {delete_count} oldest wallet addresses")
            
        conn.commit()
        conn.close()
        
    def get_all_wallets(self, limit=100):
        """Get all wallet addresses, sorted by most recent first."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT wallet_address, timestamp, amount FROM wallets ORDER BY timestamp DESC LIMIT ?", (limit,))
        results = cursor.fetchall()
        
        conn.close()
        return results
    
    def get_latest_wallets(self, count):
        """Get the most recent wallet addresses up to the specified count."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT wallet_address FROM wallets ORDER BY timestamp DESC LIMIT ?", (count,))
        results = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return results
    
    def get_count(self):
        """Get the total count of wallet addresses in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM wallets")
        count = cursor.fetchone()[0]
        
        conn.close()
        return count

# Database instance
wallet_db = None

# Modify the transaction counter and handle wallet storage
def on_token_transaction(data):
    global transaction_count, wallet_db
    transaction_count += 1
    
    try:
        # Handle the case where data is a list
        if isinstance(data, list):
            for item in data:
                process_transaction_item(item)
        else:
            # Handle the case where data is a dictionary
            process_transaction_item(data)
            
        # Print transaction statistics periodically
        if transaction_count % 10 == 0:  # Print stats every 10 transactions to reduce output
            print(f"Total transactions received: {transaction_count}")
            print(f"Total wallets in database: {wallet_db.get_count()}")
            
    except Exception as e:
        print(f"Error processing transaction: {e}")
        print(f"Data type: {type(data)}")
        print(f"Data content: {data}")

def process_transaction_item(item):
    """Process a single transaction item."""
    if not isinstance(item, dict):
        print(f"Skipping non-dictionary item: {type(item)}")
        return
        
    # Check if the transaction meets our criteria
    amount = item.get("amount", 0)
    
    # Using the proper volume field as mentioned in the example
    # Checking "volume" field which appears to represent the USD value
    volume = item.get("volume", 0)
    wallet_address = item.get("wallet")
    timestamp = item.get("time", int(time.time() * 1000))
    
    if wallet_address:  # Only print if we have a valid wallet address
        print(f"Transaction: Wallet={wallet_address}, Amount={amount}, Volume={volume}")
    
    # Store wallet addresses from transactions with volume >= 60
    if volume >= 60 and wallet_address:
        wallet_db.add_wallet(wallet_address, timestamp, volume)
        print(f"Saved wallet {wallet_address} with amount {volume} to database")
        print(f"Current database count: {wallet_db.get_count()}")

# Function to export wallet addresses to CSV files
def export_wallets_to_csv(wallet_db, git_manager=None):
    """Export the most recent wallet addresses to CSV files."""
    # Create output directory if it doesn't exist
    output_dir = "wallet_exports"
    os.makedirs(output_dir, exist_ok=True)
    
    # Define the sizes to export
    sizes = [5000, 10000, 50000, 100000, 200000]
    
    for size in sizes:
        # Get the most recent wallets up to the specified size
        wallets = wallet_db.get_latest_wallets(size)
        
        # Create the CSV file
        filename = os.path.join(output_dir, f"latest_{size}_wallets.csv")
        
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            # Write the header
            writer.writerow(["wallet"])
            # Write each wallet address
            for wallet in wallets:
                writer.writerow([wallet])
        
        print(f"Exported {len(wallets)} wallets to {filename}")
    
    # If we have a git manager, check for changes and push
    if git_manager:
        if git_manager.check_for_changes():
            git_manager.commit_and_push()
            print("Git: Changes have been committed and pushed.")
        else:
            print("Git: No file changes detected.")

# Timer function to export wallets at regular intervals
def start_export_timer(wallet_db, git_manager=None, interval_seconds=3600):
    """Start a timer to export wallet addresses at regular intervals."""
    def export_timer():
        while True:
            try:
                export_wallets_to_csv(wallet_db, git_manager)
                print(f"Scheduled export completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                time.sleep(interval_seconds)
            except Exception as e:
                print(f"Error in export timer: {e}")
                time.sleep(60)  # If there's an error, retry after a minute
    
    # Start the export timer thread
    export_thread = threading.Thread(target=export_timer, daemon=True)
    export_thread.start()
    return export_thread

def main():
    global transaction_count, wallet_db
    transaction_count = 0
    
    # Initialize the wallet database
    wallet_db = WalletDatabase()
    print(f"Initialized wallet database. Current count: {wallet_db.get_count()}")
    
    # Default to the new GitHub repo URL, but allow override via environment variable
    default_github_url = "https://github.com/wallets-alpha/live-wallets.git"
    github_url = os.environ.get('GITHUB_REPO_URL', default_github_url)
    
    git_manager = GitManager(remote_url=github_url)
    git_manager.init_repo()
    print(f"Git repository initialized with remote: {github_url}")
    
    # Start the export timer (60 minutes = 3600 seconds)
    export_thread = start_export_timer(wallet_db, git_manager, 3600)
    print("Started wallet export timer - CSV files will be updated every 60 minutes")
    
    # Do an initial export
    export_wallets_to_csv(wallet_db, git_manager)
    print("Completed initial wallet export")
    
    ws_url = "wss://datastream.solanatracker.io/961ec417-adc7-48ff-bcae-3ea82af9ed93"
    ws_service = WebSocketService(ws_url)

    token_address = "So11111111111111111111111111111111111111112"
    transaction_room = f"transaction:{token_address}"
    ws_service.join_room(transaction_room)
    ws_service.on(transaction_room, on_token_transaction)

    print("Monitoring transactions... Press Ctrl+C to exit.")
    print(f"All data will be automatically pushed to {github_url} every hour")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nFinal transaction count: {transaction_count}")
        print(f"Final wallet database count: {wallet_db.get_count()}")
    finally:
        ws_service.leave_room(transaction_room)
        ws_service.disconnect()

if __name__ == "__main__":
    main()
