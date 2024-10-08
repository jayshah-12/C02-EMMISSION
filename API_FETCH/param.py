import secret

base_url = "https://api.eia.gov/v2/"


api1_url = f"{base_url}co2-emissions/co2-emissions-aggregates/data/"
api2_url = f"{base_url}electricity/rto/daily-fuel-type-data/data/"
api3_url = f"{base_url}international/data/"


api3_params = {
    "frequency": "annual",
    "data[0]": "value",
    "facets[productId][]": [116, 33, 37],
    "facets[countryRegionId][]": "USA",
    "api_key":secret.api_key

}
api1_params = {
    "frequency": "annual",
    "data[0]": "value",
    "api_key": secret.api_key
}


api2_params = {
    "frequency": "daily",
    "data[0]": "value",
    "api_key": secret.api_key
}
