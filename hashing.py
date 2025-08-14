import hmac
import hashlib

# Your private API token from Placid
api_token = "placid-e5rlpoxpbjwmciue-vrbobvcadehx634j"

# The query string is empty for the polling URL
query_params = ""

# Hash the empty string with the API token
signature = hmac.new(
    api_token.encode('utf-8'),
    query_params.encode('utf-8'),
    hashlib.sha512
).hexdigest()

# The polling URL
polling_url = "https://api.placid.app/api/rest/images/80625886"

final_url = f"{polling_url}?s={signature}"

print(f"Signature: {signature}")
print(f"Final URL: {final_url}")