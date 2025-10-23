import re
from collections import Counter

# Function to find identical filenames in the txt file
def find_identical_files(file_path):
    # Regular expression pattern to match filenames with spaces and special characters
    pattern = r'MERRA2_\d{3,4}\.\d{8}\.SUB\.[\w\s\-\']+\.nc'
    
    # Read the content of the file
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # Find all matching filenames in the content using the regex pattern
    filenames = re.findall(pattern, content)
    
    # Count occurrences of each filename using Counter
    filename_counts = Counter(filenames)
    
    # Filter filenames that appear more than once
    duplicates = {filename: count for filename, count in filename_counts.items() if count > 1}
    
    return duplicates

# Example usage: Replace 'your_file.txt' with the path to your actual file
file_path = './NC Files/log.txt'  # Update this with your actual path
duplicates = find_identical_files(file_path)

if duplicates:
    print("Identical filenames found:")
    for filename, count in duplicates.items():
        print(f"{filename} appears {count} times")
else:
    print("No identical filenames found.")
