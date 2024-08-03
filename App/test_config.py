import cdsapi

# Initialize the CDS API client
try:
    client = cdsapi.Client()
    print("CDS API client initialized successfully.")
except Exception as e:
    print(f"Error initializing CDS API client: {e}")
