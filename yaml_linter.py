import yaml
import sys
import os

def lint_yaml(file_path):
    """
    This function takes a file path, attempts to load it as a YAML file,
    and reports whether the file is valid or not.
    
    Args:
        file_path (str): The path to the YAML file to lint.
        
    Returns:
        None
    """
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} does not exist.")
        return
    
    with open(file_path, 'r') as yaml_file:
        try:
            yaml.safe_load(yaml_file)
            print(f"{file_path}: YAML is valid.")
        except yaml.YAMLError as e:
            print(f"{file_path}: YAML is invalid.\nError details: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python lint_yaml.py <path_to_yaml_file>")
        return
    
    yaml_file_path = sys.argv[1]
    lint_yaml(yaml_file_path)

if __name__ == "__main__":
    main()
