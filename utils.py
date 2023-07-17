import csv
import json
import os
import shutil
from pathlib import Path

import requests

BASE_DIR = Path(__file__).parent


def test_function_1(a):
    a = a * 4
    print(a)
    with open('data.csv', mode='w') as f:
        writer = csv.writer(f)
        writer.writerow([a])


def test_function_2():
    with open('data.csv', mode='r') as f:
        reader = csv.reader(f)
        a = next(reader)[0]
    a = int(a) * 6
    print(a)
    with open('data.csv', mode='a') as f:
        writer = csv.writer(f)
        writer.writerow([a])


def test_function_3():
    with open('data.csv', mode='r') as f:
        reader = csv.reader(f)
        lines = list(reader)
        a = lines[-1][0]
    a = int(a) / 6.5
    print(a)
    with open('data.csv', mode='a') as f:
        writer = csv.writer(f)
        writer.writerow([a])
    os.remove('data.csv')


def test_function_6(a):
    a = a * 4
    with open('data.csv', mode='w') as f:
        writer = csv.writer(f)
        writer.writerow([a])


def test_function_7():
    with open('data.csv', mode='r') as f:
        reader = csv.reader(f)
        a = next(reader)[0]
    a = int(a) * 6
    with open('data.csv', mode='a') as f:
        writer = csv.writer(f)
        writer.writerow([a])


def test_function_8():
    with open('data.csv', mode='r') as f:
        reader = csv.reader(f)
        lines = list(reader)
        a = lines[-1][0]
    a = int(a) / 6.5
    with open('data.csv', mode='a') as f:
        writer = csv.writer(f)
        writer.writerow([a])
    os.remove('data.csv')


def test_function_4(value: int):
    return value * 2


def test_function_5():
    raise Exception('Warning!')


def get_list_by_url(url):
    response = requests.get(url)
    data = response.json()
    filename = f'{url.split("/")[-1]}'
    downloads_dir = BASE_DIR / 'output_data'
    downloads_dir.mkdir(exist_ok=True)
    archive_path = downloads_dir / filename
    with open(archive_path, 'w') as file:
        formatted_data = json.dumps(data, indent=2)
        file.write(formatted_data)


def analytics(filepath):
    with open(filepath, 'r') as json_file:
        data = json.load(json_file)
    city_name = data.get('info').get('tzinfo').get('name').split('/')[-1]
    print(city_name)


def delete_directory(directory_name):
    shutil.rmtree(directory_name)
