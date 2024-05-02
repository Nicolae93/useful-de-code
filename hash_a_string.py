import hashlib

def hash_string(input_string):
    """
    Hashes an input string using SHA-256 algorithm and returns the hexadecimal representation of the hash.
    
    Args:
    input_string (str): The string to be hashed.
    
    Returns:
    str: The hexadecimal hash of the input string.
    """
    # Create a sha256 hash object
    hash_object = hashlib.sha256()
    
    # Update the hash object with the bytes of the input string
    hash_object.update(input_string.encode())
    
    # Get the hexadecimal representation of the hash
    hashed_string = hash_object.hexdigest()
    
    return hashed_string

# Example usage:
if __name__ == "__main__":
    input_data = input("Enter a string to hash: ")
    print("SHA-256 Hash:", hash_string(input_data))
