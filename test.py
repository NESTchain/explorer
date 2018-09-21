import explorer_api
import requests

def request(url):
    ret = requests.get(url)
    if ret.status_code == 200:
        return ret.json()

def test_header():
    url = '127.0.0.1:5000/header'
    r = request(url)
    print(r)

if __name__ == '__main__':
    test_header()