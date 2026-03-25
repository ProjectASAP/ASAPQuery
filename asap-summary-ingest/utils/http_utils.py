import requests


def make_api_request(url, method, data=None):
    """
    Make an API request to the Arroyo API.

    Args:
        url (str): The URL to make the request to
        method (str): The HTTP method (get or post)
        data (str): The data to send with the request (for POST)

    Returns:
        dict: The response JSON data

    Raises:
        Exception: If the request fails
    """
    headers = {"Content-Type": "application/json"}

    try:
        if method.lower() == "post":
            response = requests.post(url, headers=headers, data=data)
        elif method.lower() == "get":
            response = requests.get(url, headers=headers)
        elif method.lower() == "delete":
            response = requests.delete(url, headers=headers)
        elif method.lower() == "patch":
            response = requests.patch(url, headers=headers, data=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            print(
                f"HTTP Error {response.status_code}: {response.content.decode('utf-8')}"
            )
            raise e

        response_data = response.content.decode("utf-8")

        return response_data
    except Exception as e:
        error_msg = f"Failed {method} request to URL: {url}"
        print("Error details:", e)
        print(error_msg)
        raise Exception(error_msg)


def create_arroyo_resource(arroyo_url, endpoint, data, resource_type):
    """
    Create a resource using the Arroyo API.

    Args:
        arroyo_url (str): Base URL of the Arroyo API
        endpoint (str): API endpoint (e.g., 'connection_profiles')
        data (str): JSON data for the resource

    Returns:
        dict: The response JSON data
    """
    url = f"{arroyo_url.rstrip('/')}/{endpoint}"
    try:
        # print(f"Creating {resource_type} resource at {url}...\n")
        # print(f"Data: {data}\n")
        # input("Press Enter to continue...")
        response_data = make_api_request(url=url, method="post", data=data)
    except Exception as e:
        error_msg = f"Failed to create {resource_type} resource: {e}"
        print(error_msg)
        raise Exception(error_msg)

    return response_data
