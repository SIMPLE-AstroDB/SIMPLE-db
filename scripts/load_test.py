import requests
import datetime
from concurrent.futures import ThreadPoolExecutor

# Adapted from https://stackoverflow.com/questions/74045846/simulating-multiple-user-requests-on-webserer-with-python-multiprocessing 

HOST = 'https://simple-bd-archive.org/'
ENDPOINT = [HOST+'/multi_plot',
            HOST+'/search',
            HOST+'/solo_result/TWA%2027',
            HOST+'/solo_result/TWA%2027B',
            HOST+'/solo_result/2MASS%20J02441019-3548036',
            HOST+'/solo_result/HD%2044627B',
            HOST+'/coordinate_query'
            ]

def send_api_request(ENDPOINT):
    r = requests.get(ENDPOINT)
    statuscode = r.status_code
    elapsedtime = r.elapsed
    return ENDPOINT, statuscode, elapsedtime

def main():
    with ThreadPoolExecutor(max_workers=8) as pool: 
        iterator = pool.map(send_api_request, ENDPOINT)

    for result in iterator:
        print(result)

if __name__ == '__main__':
    main()
