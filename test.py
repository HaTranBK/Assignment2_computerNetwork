import os
def get_list_local_files(directory='.'):
    try:
        return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    except Exception as e:
        return f"Error: Unable to list files - {e}"
    
print(get_list_local_files())