import hvac
import os

VAULT_ADDR = os.getenv('VAULT_ADDR', 'http://localhost:8200') 
VAULT_TOKEN = os.getenv('VAULT_TOKEN', 'root')  

client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)



secret_path = 'myapp/creds'  
secret = client.secrets.kv.v2.read_secret_version(path=secret_path)

api_key = secret['data']['data']['api_key']


# print(api_key)
