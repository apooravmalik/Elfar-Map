# server/utils/utils.py
import base64
import os

def map_color(status_text):
    """
    Determines the icon color based on the device's status text.
    - Returns 'red' for "Fail" and "axe_ElfarDisconnected".
    - Returns 'blue' for "Normal" and "axe_ElfarConnected".
    """
    if isinstance(status_text, str):
        if 'Fail' in status_text or 'axe_ElfarDisconnected' in status_text:
            return 'red'
        if 'Normal' in status_text or 'axe_ElfarConnected' in status_text:
            return 'blue'
            
    # Default to blue if the status is not a string or doesn't match
    return 'blue'

def convert_blob_to_base64(blob_data):
    """
    Converts binary blob data into a base64 encoded data URI string for embedding in HTML/JS.
    Returns None if the input data is empty.
    """
    if not blob_data:
        return None
    
    # Encode the binary data to base64
    base64_encoded_data = base64.b64encode(blob_data)
    # Decode the base64 bytes to a string
    base64_string = base64_encoded_data.decode('utf-8')
    # Format as a data URI
    return f"data:image/png;base64,{base64_string}"

def save_icon_from_blob(blob_data, filename="fence_icon.png"):
    """
    Saves the binary blob data as a PNG file to a 'static' directory.
    This is the approach you requested for use in the frontend.
    """
    if not blob_data:
        print("Error: BLOB data is empty. Cannot save icon.")
        return None

    try:
        # Define a path to a 'static' folder within the 'server' directory.
        # This is a common practice for serving files like images, CSS, etc.
        static_dir = os.path.join(os.path.dirname(__file__), '..', 'static')
        
        # Create the 'static' directory if it doesn't exist
        if not os.path.exists(static_dir):
            os.makedirs(static_dir)
            
        # Define the full path for the output file
        output_path = os.path.join(static_dir, filename)

        # Write the binary blob data to the file
        with open(output_path, "wb") as file:
            file.write(blob_data)
        
        print(f"Icon successfully saved to {output_path}")
        # Return the path relative to the server for frontend use
        return f"/static/{filename}"

    except Exception as e:
        print(f"An error occurred while saving the icon: {e}")
        return None