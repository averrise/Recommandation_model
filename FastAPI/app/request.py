import requests

url = "http://127.0.0.1:8000/send_message/"
data = {
    "Movie_Name": "Inception",
    "Similarity_weight": 5,
    "top_n": 10
}

response = requests.post(url, json=data)
print(response.json())