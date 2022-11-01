import requests


def lineMess(Auth, message):
    message = message
    print(message)
    headers = { "Authorization": "Bearer " + Auth }
    data = { 'message': message }

    requests.post("https://notify-api.line.me/api/notify",
        headers = headers, data = data)