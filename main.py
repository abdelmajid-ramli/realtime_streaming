import requests
import json


api_url="https://randomuser.me/api/" 
#only male https://randomuser.me/api/?gender=male
#only certain fields https://randomuser.me/api/?inc=name,email,phone
#only certien natinality  https://randomuser.me/api/?nat=us,gb,fr
#api_url="https://randomuser.me/api/?results=10"


try:
    response = requests.get(api_url)

    if response.status_code == 200:
        
        data=response.json()
        print(json.dumps(data,indent=2))
    else:
        print(f"Error: API request failed with status code {response.status_code}")
        print(response.text) 


except requests.exceptions.RequestException as e:
    print(f"An error occurred during the API request: {e}")
