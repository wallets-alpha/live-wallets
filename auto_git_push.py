#!/usr/bin/env python3
import os
import subprocess
import time
import hashlib
from datetime import datetime

class GitManager:
    def __init__(self, repo_dir='.', remote_url=None, branch='main'):
        """
        Initialize the Git Manager.
        
        Args:
            repo_dir (str): Directory of the repository
            remote_url (str): GitHub repository URL (e.g., 'https://github.com/username/repo.git')
            branch (str): Branch to push to
        """
        self.repo_dir = repo_dir
        self.remote_url = remote_url
        self.branch = branch
        self.csv_dir = os.path.join(repo_dir, 'wallet_exports')
        self.file_hashes = {}
        
    def run_cmd(self, cmd, cwd=None):
        """Run a shell command and return the output."""
        if cwd is None:
            cwd = self.repo_dir
        try:
            result = subprocess.run(
                cmd, 
                cwd=cwd, 
                shell=True, 
                check=True, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.PIPE,
                encoding='utf-8'
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            print(f"Command failed: {cmd}")
            print(f"Error: {e.stderr}")
            return None
    
    def init_repo(self):
        """Initialize a Git repository if not already done."""
        if not os.path.exists(os.path.join(self.repo_dir, '.git')):
            print("Initializing Git repository...")
            self.run_cmd('git init')
            
            # Create .gitignore to exclude .db files and other unwanted files
            with open(os.path.join(self.repo_dir, '.gitignore'), 'w') as f:
                f.write("*.db\n")
                f.write("venv/\n")
                f.write("__pycache__/\n")
                f.write("*.pyc\n")
                f.write("*.log\n")
                
            # Commit .gitignore
            self.run_cmd('git add .gitignore')
            self.run_cmd('git commit -m "Initial commit: Add .gitignore"')
            
            # Setup remote if provided
            if self.remote_url:
                self.run_cmd(f'git remote add origin {self.remote_url}')
                
            print("Git repository initialized.")
        else:
            print("Git repository already initialized.")
            
            # Ensure remote is set correctly if provided
            if self.remote_url:
                remotes = self.run_cmd('git remote -v')
                if 'origin' not in remotes:
                    self.run_cmd(f'git remote add origin {self.remote_url}')
                elif self.remote_url not in remotes:
                    self.run_cmd(f'git remote set-url origin {self.remote_url}')
    
    def calculate_file_hash(self, filepath):
        """Calculate MD5 hash of a file."""
        md5_hash = hashlib.md5()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                md5_hash.update(byte_block)
        return md5_hash.hexdigest()
    
    def check_for_changes(self):
        """Check if any CSV files have changed."""
        if not os.path.exists(self.csv_dir):
            print(f"CSV directory {self.csv_dir} does not exist.")
            return False
            
        current_hashes = {}
        changed_files = []
        
        # Get all CSV files in the directory
        for filename in os.listdir(self.csv_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(self.csv_dir, filename)
                current_hash = self.calculate_file_hash(filepath)
                current_hashes[filepath] = current_hash
                
                # Check if file is new or modified
                if filepath not in self.file_hashes or self.file_hashes[filepath] != current_hash:
                    changed_files.append(filepath)
        
        # Update stored hashes
        self.file_hashes = current_hashes
        
        return bool(changed_files)
    
    def commit_and_push(self):
        """Commit changes to CSV files and push to remote."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Add all CSV files
        self.run_cmd(f'git add {self.csv_dir}/*.csv')
        
        # Check if there are changes to commit
        status = self.run_cmd('git status --porcelain')
        if not status:
            print("No changes to commit.")
            return False
            
        # Commit with timestamp
        commit_message = f"Update wallet data - {timestamp}"
        self.run_cmd(f'git commit -m "{commit_message}"')
        print(f"Committed changes: {commit_message}")
        
        # Push to remote if URL is provided
        if self.remote_url:
            push_result = self.run_cmd(f'git push origin {self.branch}')
            if push_result is not None:
                print(f"Changes pushed to {self.remote_url} on branch {self.branch}")
                return True
            else:
                print("Failed to push changes.")
                return False
        else:
            print("No remote URL provided. Changes committed but not pushed.")
            return True

def main():
    # Use the new GitHub repository URL as default, but allow override from environment
    default_github_url = "https://github.com/wallets-alpha/live-wallets.git"
    github_url = os.environ.get('GITHUB_REPO_URL', default_github_url)
    
    # Initialize Git manager
    git_manager = GitManager(remote_url=github_url)
    
    # Initialize repository
    git_manager.init_repo()
    
    # Perform initial check and push
    if git_manager.check_for_changes():
        git_manager.commit_and_push()
    
    # Monitor for changes at regular intervals
    check_interval = 60 * 60  # 1 hour, matching the CSV export interval
    print(f"Monitoring for changes every {check_interval} seconds...")
    print(f"Using GitHub repository: {github_url}")
    
    try:
        while True:
            time.sleep(check_interval)
            print(f"\nChecking for changes at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}...")
            if git_manager.check_for_changes():
                git_manager.commit_and_push()
            else:
                print("No changes detected.")
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
    except Exception as e:
        print(f"Error encountered: {e}")
        print("Restarting monitoring...")
        time.sleep(10)  # Wait 10 seconds before restarting
        main()  # Restart the main function

if __name__ == "__main__":
    main() 
