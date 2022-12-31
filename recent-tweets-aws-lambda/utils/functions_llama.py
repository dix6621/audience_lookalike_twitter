import requests

def llama_pools_yield():
    response = requests.get('https://yields.llama.fi/pools').json()
    return response
    
def llama_protocols():
    response = requests.get('https://api.llama.fi/protocols').json()
    return response
