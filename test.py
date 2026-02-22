import os

def is_docker():
    return os.path.exists('/.dockerenv')

if is_docker():
    print("Running inside Docker 🐳")
else:
    print("Running on Laptop/Host 💻")
