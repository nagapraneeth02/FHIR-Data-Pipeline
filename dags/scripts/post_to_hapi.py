import json
import requests

def post_to_hapi(fhir_file_path, hapi_url="https://8080-cs-83d908bb-a05b-4b05-b90f-e00ad675e530.cs-us-east1-dogs.cloudshell.dev/fhir"):
    print(f"🔄 Posting FHIR resources to HAPI: {hapi_url}")
    
    try:
        with open(fhir_file_path, 'r') as f:
            resources = json.load(f)
    except Exception as e:
        print(f"❌ Failed to read FHIR file: {e}")
        raise

    success_count = 0
    failure_count = 0

    for i, resource in enumerate(resources):
        print(resources)
        resource_type = resource.get('resourceType')
        if not resource_type:
            print(f"⚠️  Resource {i} missing 'resourceType', skipping.")
            continue

        # Use PUT if an ID is present (to ensure predictable storage and allow updates)
        if 'id' in resource:
            url = f"{hapi_url}/{resource_type}/{resource['id']}"
            method = "PUT"
        else:
            url = f"{hapi_url}/{resource_type}"
            method = "POST"

        headers = {"Content-Type": "application/fhir+json",
                  "Authorization": "Bearer #ya29.a0AW4XtxgAOWipWEUaI8j6JylhKsrk9ZAlbDvzqDtyMmaRkRU2aVkUrMY1rYU5F13mz83eqMAQfDwYxA1QUUp1X51OO169I4IUuIG8Rz2gqFWum4Nz-X6os5xYnlEWEUxfc76QkAunQfZ1FYKhu9YQqLlQtyd3fkwTIUEdMFjNArH4FzOqC6LuymCqlLAdTBhIs7faJrLWV0MkFi6LktLtnD2nInGJEd5SWdMqKrYEP2J4SefAktKlqOJyKOahrskdSWfGztwul4a_eT9-nHGpiCbPGkq2kfX6piGUHGhQ6gsG0BzOYLikRZDVgcZ6QturSFeEVrr4-q3kjY5xHjIP02tX7yo9cnftE47DS7IcgpsLrYCOU93zeCz6zfi7jJfbKBdVgyrqns16dXZwqbDGta19jnM_cnyCtXf8aCgYKAesSARMSFQHGX2MiXNwfH3mPO4RvbvW2i5NRIw0427"}

        try:
            if method == "PUT":
                response = requests.put(url, headers=headers, json=resource, timeout=10)
            else:
                response = requests.post(url, headers=headers, json=resource, timeout=10)

            if response.status_code in [200, 201]:
                print(f"✅ [{i}] {method} {resource_type}/{resource.get('id', '[auto]')} → {response.status_code}")
                success_count += 1
            else:
                print(f"❌ [{i}] {method} {resource_type}/{resource.get('id', '[auto]')} → {response.status_code} {response.reason}")
                print(f"   Details: {response.text}")
                failure_count += 1

        except requests.exceptions.RequestException as e:
            print(f"❌ [{i}] RequestException: {e}")
            failure_count += 1

    print(f"\n✅ Upload Complete: {success_count} succeeded, ❌ {failure_count} failed.")
