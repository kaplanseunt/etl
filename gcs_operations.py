
from google.cloud import storage

def createBucket(bucke_name,location,storage_class,project_id):
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    bucket.location = location
    bucket.storage_class = storage_class
    new_bucket = storage_client.create_bucket(bucket)
    print(f"Bucket {new_bucket.name} created in {location} with storage class {storage_class}.")
    return new_bucket
    

if __name__ == '__main__':
    bucket_name = "tk_project_24"
    location = "EUROPE-WEST1"  
    storage_class = "STANDARD"
    project_id = "oredata-de-cirrus" 
    createBucket(bucket_name, location, storage_class, project_id)
