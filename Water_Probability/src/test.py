import json
import requests

url = "https://water-test-dz5b.onrender.com/predict"

x_new = dict (
    ph = 10.71608,
    Hardness = 500.89045,
    Solids = 20791.318981,
    Chloramines = 7.300212,
    Sulfate = 368.516441,
    Conductivity = 564.308654,
    Organic_carbon = 10.379783,
    Trihalomethanes = 86.99097,
    Turbidity = 2.963135
    )

x_new_json = json.dumps(x_new)

response = requests.post(url,data = x_new_json)

print("Response Text:",response.text)
print("Status Code:", response.status_code)

