from concurrent.futures import ThreadPoolExecutor
from deep_translator import GoogleTranslator
from datetime import datetime, timedelta
from scrapy.cmdline import execute
from pandas import DataFrame
from typing import Iterable
from scrapy import Request
import pandas as pd
import numpy as np
import scrapy
import random
import time
import evpn
import json
import os
import re

proxies = [
    "185.188.76.152", "104.249.0.116", "185.207.96.76", "185.205.197.4", "185.199.117.103", "185.193.74.119", "185.188.79.150", "185.195.223.146", "181.177.78.203", "185.207.98.115", "186.179.10.253", "185.196.189.131", "185.205.199.143", "185.195.222.22", "186.179.20.88", "185.188.79.126", "185.195.213.198", "185.207.98.192", "186.179.27.166", "181.177.73.165", "181.177.64.160", "104.233.53.55", "185.205.197.152", "185.207.98.200", "67.227.124.192", "104.249.3.200", "104.239.114.248",
    "181.177.67.28", "185.193.74.7", "216.10.5.35", "104.233.55.126", "185.195.214.89", "216.10.1.63", "104.249.1.161", "186.179.27.91", "185.193.75.26", "185.195.220.100", "185.205.196.226", "185.195.221.9", "199.168.120.156", "181.177.69.174", "185.207.98.8", "185.195.212.240", "186.179.25.90", "199.168.121.162", "185.199.119.243", "181.177.73.168", "199.168.121.239", "185.195.214.176", "181.177.71.233", "104.233.55.230", "104.249.6.234", "104.249.3.87", "67.227.125.5", "104.249.2.53",
    "181.177.64.15", "104.249.7.79", "186.179.4.120", "67.227.120.39", "181.177.68.19", "186.179.12.120", "104.233.52.54", "104.239.117.252", "181.177.77.65", "185.195.223.56", "185.207.99.39", "104.249.7.103", "185.207.99.11", "186.179.3.220", "181.177.72.117", "185.205.196.180", "104.249.2.172", "185.207.98.181", "185.205.196.255", "104.239.113.239", "216.10.1.94", "181.177.77.2", "104.249.6.84", "104.239.115.50", "185.199.118.209", "104.233.55.92", "185.207.99.117", "104.233.54.71",
    "185.199.119.25", "181.177.78.82", "104.239.113.76", "216.10.7.90", "181.177.78.202", "104.239.119.189", "181.177.64.245", "185.199.118.216", "185.199.116.219", "185.188.77.64", "185.199.116.185", "185.188.78.176", "186.179.12.162", "185.205.197.193", "181.177.74.161", "67.227.126.121", "181.177.79.185",
]


def get_org_or_individual(provider_name: str):
    pattern_start = r'^\w{2}[\. ]'  # Regular expression to match two characters followed by a period at the start
    pattern_end = r'\w{2}[\.]$'  # Regular expression to match two characters followed by a period at the start
    org_status = bool(re.match(pattern=pattern_start, string=provider_name))
    return 'Organization' if org_status else 'Individual'


def df_cleaner(data_frame: DataFrame):
    # normalize headers by joining each with '_' instead of blankspace and converting to lowercase
    data_frame.columns = data_frame.columns.str.lower().str.replace(' ', '_')
    # Apply the function to all columns for Cleaning
    for column in data_frame.columns:
        data_frame[column] = data_frame[column].astype(str).apply(replace_with_na)  # Convert to string before applying
        if column not in ['penyedia', 'provider']:  # Apply functions to clean df and remove redundant characters
            data_frame[column] = data_frame[column].apply(remove_extra_spaces)
        elif column in ['penyedia', 'provider']:  # 'name' column name is 'penyedia' in native (Malay) language
            data_frame[column] = data_frame[column].apply(remove_specific_punctuation)  # Remove punctuation
            data_frame[column] = data_frame[column].apply(remove_extra_spaces)


def replace_with_na(text):
    return re.sub(r'^[\s_-]+$', 'N/A', text)  # Replace _, __, -, --, --- with N/A


def if_empty_then_na(_text):
    return _text if _text not in [None, '', {}, ' ', '   '] else 'N/A'


# Function to remove Extra Spaces from Text
def remove_extra_spaces(_text: str):
    return ' '.join(_text.split())  # Remove extra spaces


def translate_text_with_retries(translator, value, max_retries=20):
    """Translate text with retry mechanism in case of errors."""
    retries = 0
    while retries < max_retries:
        try:
            if value == 'N/A':
                return value
            translated_value = translator.translate(text=value)  # Attempt translation
            return translated_value
        except Exception as e:
            retries += 1
            wait_time = random.uniform(a=2, b=5) * retries  # Exponential backoff with some randomness
            print(f"Error translating '{value}': {e}. Retrying in {wait_time:.2f} seconds (Attempt {retries}/{max_retries})")
            time.sleep(wait_time)
    return value  # Return original value if all retries fail


def translate_chunk_rows(chunk, columns):
    """Translate a chunk of rows in the dataframe for specified columns."""
    proxy = f"http://kunal_santani577-9elgt:QyqTV6XOSp@{random.choice(proxies)}:3199"
    # translator = GoogleTranslator(source='ms', target='en', proxies={'http': proxy})
    translator = GoogleTranslator(source='ms', target='en')
    for index, row in chunk.iterrows():
        print('in translator...')
        for col in columns:
            if col in chunk.columns:
                value = row[col]
                try:
                    if isinstance(value, str) and value.strip() == 'N/A':
                        chunk.at[index, col] = 'N/A'
                    elif isinstance(value, str) and value.strip() == '':
                        chunk.at[index, col] = 'N/A'
                    elif isinstance(value, str) and value.strip() != '':
                        # Translate the value with retries
                        translated_value = translate_text_with_retries(translator, value)
                        chunk.at[index, col] = translated_value
                        print(f"Row {index}, Col '{col}': Translated '{value}' -> '{translated_value}'")
                except Exception as e:
                    print(f"Error translating '{value}' in row {index}, column '{col}': {e}")
                    chunk.at[index, col] = value  # Keep original value if error occurs
    return chunk


def translate_dataframe_in_chunks(df, columns, number_of_chunks=8):
    """Helper function to translate specified columns in the dataframe using parallel processing."""
    chunks = np.array_split(df, number_of_chunks)
    print(f"Dataframe split into {number_of_chunks} chunks for parallel processing.")

    with ThreadPoolExecutor(max_workers=number_of_chunks) as executor:
        results = list(executor.map(lambda chunk: translate_chunk_rows(chunk, columns), chunks))
    print("All chunks processed.")
    return pd.concat(results)


def remove_specific_punctuation(_text):
    punctuation_marks = [
        ".", ",", "?", "!", ":", ";", "—", "-", "_", "(", ")", "[", "]", "{", "}", '"', "'", "‘", "’", "“", "”", "«", "»",
        "/", "\\", "|", "@", "#", "$", "%", "^", "&", "*", "+", "=", "~", "`", "<", ">", "…", "©", "®", "™"
    ]
    # Iterate over each punctuation mark and replace it in the original text
    for punc_mark in punctuation_marks:
        _text = _text.replace(punc_mark, f'{punc_mark} ').replace(punc_mark, '')
    return _text


def text_cleaner(text: str) -> str:
    return re.sub(pattern=r"[^\w\s]", repl=" ", string=text).title()


def get_sanctuion_duration(provider_dict: dict):
    duration = provider_dict.get("publishDurationInMinutes", 'N/A')
    sanctuion_duration = f"{round(int(duration) / 525600)} Years" if if_empty_then_na(duration) != 'N/A' else 'N/A'
    return sanctuion_duration


def get_provider_name(provider_dict: dict):
    provider_name = provider_dict.get("provider", {}).get("name", 'N/A')
    return provider_name


def get_document_name(provider_dict: dict):
    document_name = provider_dict.get("document", {}).get("name", {})
    return document_name


def get_package_id(provider_dict: dict):
    tender = provider_dict.get("tender", {})
    _package_id = tender.get("packageId", 'N/A') if tender not in [None, {}] else 'N/A'
    package_id = text_cleaner(_package_id) if tender not in [None, {}, 'NA', 'N/A'] else 'N/A'
    return package_id


def get_tender_name(provider_dict: dict):
    tender = if_empty_then_na(provider_dict.get("tender", {}))
    tender_name = tender.get("name", 'N/A') if tender != 'N/A' else 'N/A'
    return tender_name


def get_effective_date(provider_dict: dict):
    effective_date = provider_dict.get("startDate", 'N/A')
    date_pattern = r'^\d{4}-\d{2}-\d{2}'  # Regular expression to extract the date
    match = re.search(date_pattern, effective_date)  # Search for the pattern in the string
    # If a match is found, extract the date
    if match:
        date_str = match.group()
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")  # Convert the string to a datetime object
        new_date_str = date_obj.strftime("%Y-%m-%d")  # Format the new date as a string
        return new_date_str
    else:
        return 'N/A'
    # return effective_date[:10] if effective_date not in [None, "N/A"] else "N/A"


def get_status_date(provider_dict: dict):
    published_date = provider_dict.get("statusUpdatedAt", 'N/A')
    # effective_date = provider_dict.get("startDate", 'N/A')
    date_pattern = r'^\d{4}-\d{2}-\d{2}'  # Regular expression to extract the date
    match = re.search(date_pattern, published_date)  # Search for the pattern in the string
    # If a match is found, extract the date
    if match:
        date_str = match.group()
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")  # Convert the string to a datetime object
        new_date = date_obj - timedelta(days=1)  # Subtract one day
        new_date_str = new_date.strftime("%Y-%m-%d")  # Format the new date as a string
        return new_date_str
    else:
        return 'N/A'
    # return published_date[:10] if published_date not in [None, "N/A"] else "N/A"


def get_status(provider_dict: dict):
    status = provider_dict.get('status', 'N/A')
    return status if status not in ['N/A', '', None, ' ', '-'] else 'N/A'


def get_next_page_data_url(provider_dict: dict):
    blacklist_id = provider_dict.get('id', 'N/A')
    if blacklist_id != 'N/A':
        next_page_data_url = f'https://daftar-hitam.inaproc.id/blacklist/{blacklist_id}?_rsc=1168r'
        return next_page_data_url
    else:
        return 'N/A'


def get_npwp(extracted_dict):
    return extracted_dict.get('provider', {}).get('npwp', 'N/A')


def get_nib(extracted_dict):
    return extracted_dict.get('provider', {}).get('nib', 'N/A')


def get_address(extracted_dict):
    return extracted_dict.get('provider', {}).get('address', 'N/A')


def get_provice(extracted_dict):
    return extracted_dict.get('provider', {}).get('province', {}).get('name', 'N/A')


def get_regency(extracted_dict):
    return extracted_dict.get('provider', {}).get('regency', {}).get('name', 'N/A')


def get_additional_address(extracted_dict):
    return extracted_dict.get('provider', {}).get('additionalAddress', 'N/A')


def get_sk_number(extracted_dict):
    return extracted_dict.get('defaultValues', {}).get('step1', {}).get('skNumber', 'N/A')


def get_violation_data_dict(extracted_dict) -> dict:
    violation_data_dict = {
        'violation_type': 'N/A', 'violations_description': 'N/A',
        'violation_show_duration': 'N/A', 'end_date': 'N/A'
    }
    violation_id = extracted_dict.get('defaultValues', {}).get('step1', {}).get('violationId', 'N/A')
    if violation_id not in ['N/A', '', None]:
        for violation_dict in extracted_dict.get('violations', []):
            if violation_dict.get('id', 'N/A') == violation_id:
                duration_month = violation_dict.get('month', 'N/A')
                duration_year = violation_dict.get('year', 'N/A')
                show_duration = f'{if_empty_then_na(duration_year)} tahun {if_empty_then_na(duration_month)} bulan'  # 'tahun' = YEAR , 'bulan' = MONTH
                end_date_str = extracted_dict.get("defaultValues", {}).get("step1", {}).get("expiredDate", 'N/A')
                end_date = 'N/A'
                if end_date_str != 'N/A':
                    # effective_date = provider_dict.get("startDate", 'N/A')
                    date_pattern = r'^\d{4}-\d{2}-\d{2}'  # Regular expression to extract the date
                    match = re.search(date_pattern, end_date_str)  # Search for the pattern in the string
                    # If a match is found, extract the date
                    if match:
                        date_str = match.group()
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d")  # Convert the string to a datetime object
                        new_date = date_obj - timedelta(days=1)  # Subtract one day
                        end_date = new_date.strftime("%Y-%m-%d")  # Format the new date as a string
                violation_data_dict['violation_type'] = violation_dict.get('name', 'N/A')
                violation_data_dict['violations_description'] = violation_dict.get('description', 'N/A')
                violation_data_dict['violation_show_duration'] = show_duration
                violation_data_dict['end_date'] = end_date
    return violation_data_dict


def get_package_info_dict(extracted_dict: dict):
    package_info_dict = {
        'tender_id': 'N/A', 'package_name': 'N/A', 'procurement_type': 'N/A', 'government_info': 'N/A',
        'work_unit': 'N/A', 'hps': 'N/A', 'pagu': 'N/A', 'fiscal_year': 'N/A'
    }
    tender_dict = extracted_dict.get('defaultValues', {}).get('step1', {}).get('tender', {})
    if tender_dict not in ["$undefined", None]:
        # Dictionary containing all values with keys
        package_info_dict['tender_id'] = tender_dict.get('id', 'N/A')
        package_info_dict['package_name'] = tender_dict.get('name', 'N/A')
        package_info_dict['procurement_type'] = tender_dict.get('category', 'N/A')
        package_info_dict['government_info'] = tender_dict.get('kldi', {}).get('name', 'N/A')
        package_info_dict['work_unit'] = tender_dict.get('satker', {}).get('name', 'N/A')
        package_info_dict['hps'] = tender_dict.get('hps', 'N/A')
        package_info_dict['pagu'] = tender_dict.get('pagu', 'N/A')
        package_info_dict['fiscal_year'] = tender_dict.get('budgetYear', 'N/A')
    return package_info_dict


class InaprocIdSpider(scrapy.Spider):
    name = "inaproc_id"  # Spider Name

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        print('Connecting to VPN (INDONESIA)')
        self.api = evpn.ExpressVpnApi()  # Connecting to VPN (INDONESIA)
        self.api.connect(country_id='112')  # indonesia country code
        time.sleep(3)  # keep some time delay before starting scraping because connecting
        if self.api.is_connected:
            print('VPN Connected!')
        else:
            print('VPN Not Connected!')

        self.final_data = list()
        self.delivery_date = datetime.now().strftime('%Y%m%d')

        # Path to store the Excel file can be customized by the user
        self.excel_path = r"../Excel_Files"  # Client can customize their Excel file path here (default: govtsites > govtsites > Excel_Files)
        os.makedirs(self.excel_path, exist_ok=True)  # Create Folder if not exists
        self.filename = fr"{self.excel_path}/{self.name}_{self.delivery_date}"  # Filename with Scrape Date

        self.cookies = {
            '_cfuvid': '.tjkLgWtKnz0og_5m5h2ystJ9TEkdsTe0qRqehZeYcI-1727698098418-0.0.1.1-604800000',
            'cf_clearance': '0l.s.glrl5VWf46Aj5A4gN7VGHha6T76e9ijeDOn3L4-1727758384-1.2.1.1-JIDLW.HdVEjZLsJ8AHmsv_TXIxYsduteTS3QLDXS3Qj7yW_9gNoYjZE6AFu.QwYo59gtCJZgv72Den0yq0bdC_XTJUnX7fNXB75WVQF4Ruh6BKiPPLYPJtQTWVx60zHrLljPTT2oUlzv7H.HuRpbFZLyUvpTMotcxeoWHOzPo.aDvc6K.C8JsFAQmb08sE1dwsyG_1znUK3s6gLp5wHVR4biXUz4nI5Ll5Pm2EYWvUveNvqC8LiYXhVafUzKB1qe21AI2jaJ_PCao3FHO.LsrNAU8G7Zhgd5eXe.7b8PQzp8EaQ_h6w0ea47F6VS4IAZSL2HIenu5f7hhuwIB6B8PMw.5T3T6HP5.cwkb2_0M35eYo3vteKuEs_pRQfzTwVK',
        }

        self.headers = {
            'accept': '*/*', 'accept-language': 'en-US,en;q=0.9', 'content-type': 'application/json', 'origin': 'https://daftar-hitam.inaproc.id',
            'priority': 'u=1, i', 'referer': 'https://daftar-hitam.inaproc.id/', 'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site', 'x-gtp-app': 'blacklist',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
        }
        self.json_data = {
            'query': 'query GetBlacklists($input: BlacklistsInput) {\n  blacklists(input: $input) {\n    blacklists {\n      ... on Blacklist {\n        id\n        skNumber\n        skNumberStatusBased\n        status\n        startDate\n        expiredDate\n        publishDate\n        publishDurationInMinutes\n        status\n        statusUpdatedAt\n        tender {\n          id\n          name\n          packageId\n          pagu\n        }\n        provider {\n          id\n          name\n          npwp\n          address\n          additionalAddress\n        }\n        document {\n          id\n          name\n          blacklistId\n          additionalInfo\n        }\n        correspondence {\n          lpse {\n            id\n            name\n          }\n          kldi {\n            name\n            id\n          }\n          satker {\n            id\n            name\n          }\n        }\n        violation {\n          ...ActiveViolation\n        }\n      }\n    }\n    pagination {\n      ... on Pagination {\n        pageNumber\n        totalPage\n        totalData\n      }\n    }\n  }\n}\n\nfragment ActiveViolation on Violation {\n  id\n  name\n  description\n  month\n  year\n}',
            'variables': {
                'input': {
                    'filter': {},
                    'pagination': {
                        'pageNumber': 1,
                        'perPage': 100,
                    },
                },
            },
            'operationName': 'GetBlacklists',
        }
        self.browsers = ["chrome110", "edge99", "safari15_5"]

    def start_requests(self) -> Iterable[Request]:
        yield scrapy.Request(url='https://gw.inaproc.id/api/', cookies=self.cookies, headers=self.headers, body=json.dumps(self.json_data),
                             method='POST', meta={'impersonate': random.choice(self.browsers)}, cb_kwargs={'page': 1}, callback=self.parse)

    def parse(self, response, **kwargs):
        if response.status != 200:
            print('Response status code:', response.status)
        elif response.status == 200:
            providers_blacklist_dict = json.loads(response.text)
            providers_backlists = providers_blacklist_dict["data"]["blacklists"]["blacklists"]
            for provider_dict in providers_backlists:
                next_page_data_url = get_next_page_data_url(provider_dict)  # Next Page Data Url
                provider_name = get_provider_name(provider_dict)
                org_or_individual = get_org_or_individual(provider_name)
                provider_data_dict = {
                    "url": f'https://daftar-hitam.inaproc.id/?limit=100&p={kwargs['page']}',
                    "penyedia": provider_name, "organization/individual": org_or_individual, "dokumen_surat_keputusan": get_document_name(provider_dict),
                    "nomor_paket": get_package_id(provider_dict), "paket": get_tender_name(provider_dict),
                    "tanggal_berlaku": get_effective_date(provider_dict), "tanggal_status": get_status_date(provider_dict), "durasi_sanksipenyedia": get_sanctuion_duration(provider_dict), "status": get_status(provider_dict),
                    "Aski": next_page_data_url  # Next Page Data Url
                }
                cookies_next_page = {'_cfuvid': '3yYd681pIlRUxoMX02eqCa76bQZpGB5FeVZR6fnAKzo-1728885724423-0.0.1.1-604800000',
                                     'cf_clearance': 'i56PjKByEVibP_ZbqPEgM6CkbscdV0AaQyCGqGcAVKU-1728886487-1.2.1.1-1CTqBIDazBZRHZAYXtfStMnCEeg6c7pbuf2FF0E2bXGdltVYdQbVqtAzGeYS32w7unp.lkUJwJjL0kYyIjOX5xuLXq7EK0MqmCFDEKG_8FAmM.YWj2w.OtsKatYhYfSf8GojRtG1Xy_s9NPA2IRHhxe_lhGHhPqUWLTd4.Raa2dyPHDBWgqQNDide0TP6BpKIfG85Ass799OMbDH32WU1S3aZ0BiyQJjqSjv1blm8YB_99dvSD.uKI.dbjlCVPaEvm8FFZ4ujUbWCmptGvN7q74KDxeJc62SseOTsgdgKd4WbDs.M7uDOxt17.obP5L43NfpXAyKlGWXd2PDc8FCMyVTC3T9dk5peKwTFD7c7dDLQ7k8r7lPkbo0ZeOxVq2b'}

                headers_next_page = {
                    'accept': '*/*', 'accept-language': 'en-US,en;q=0.9,id;q=0.8',
                    'next-router-state-tree': '%5B%22%22%2C%7B%22children%22%3A%5B%22(header-layout)%22%2C%7B%22children%22%3A%5B%22(sidebar)%22%2C%7B%22publicDashboard%22%3A%5B%22__PAGE__%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2C%22%2F%22%2C%22refresh%22%5D%7D%2C%22%2F%22%2C%22refresh%22%5D%2C%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2C%22%2F%22%2C%22refresh%22%5D%7D%5D%7D%5D%7D%2Cnull%2Cnull%2Ctrue%5D',
                    'next-url': '/', 'priority': 'u=1, i', 'referer': 'https://daftar-hitam.inaproc.id/',
                    'rsc': '1', 'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                    'sec-ch-ua-arch': '"x86"', 'sec-ch-ua-bitness': '"64"', 'sec-ch-ua-full-version': '"129.0.6668.90"',
                    'sec-ch-ua-full-version-list': '"Google Chrome";v="129.0.6668.90", "Not=A?Brand";v="8.0.0.0", "Chromium";v="129.0.6668.90"',
                    'sec-ch-ua-mobile': '?0', 'sec-ch-ua-model': '""', 'sec-ch-ua-platform': '"Windows"', 'sec-ch-ua-platform-version': '"15.0.0"', 'sec-fetch-dest': 'empty', 'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-origin', 'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                }
                # Send 1st page data to next page data parse through cb_kwargs
                yield scrapy.Request(
                    url=next_page_data_url, cookies=cookies_next_page, headers=headers_next_page, meta={'impersonate': random.choice(self.browsers)},
                    cb_kwargs={'provider_data_dict': provider_data_dict}, callback=self.next_page_data_parse, dont_filter=True)

            print('Data appended', '=' * 50)
            # Look for the "Next" page link
            total_page = providers_blacklist_dict.get('data', {}).get('blacklists', {}).get('pagination', {}).get('totalPage')
            this_page_no = providers_blacklist_dict.get('data', {}).get('blacklists', {}).get('pagination', {}).get('pageNumber')
            if this_page_no != total_page:  # If there is a next page, send a request to the next page
                self.json_data['variables']['input']['pagination']['pageNumber'] += 1  # Adding next page
                print('Next Page found, Sending Request on Page:', kwargs['page'] + 1)
                yield scrapy.Request(
                    url='https://gw.inaproc.id/api/', cookies=self.cookies,
                    headers=self.headers, body=json.dumps(self.json_data),
                    method='POST', meta={'impersonate': random.choice(self.browsers)},
                    cb_kwargs={'page': kwargs['page'] + 1}, callback=self.parse, dont_filter=True)

    def next_page_data_parse(self, response, **kwargs):
        provider_data_dict = kwargs['provider_data_dict']
        pattern = r'({"mode":.*?"role":\s*"\$undefined"})'
        match = re.search(pattern, response.text)  # Extract the part of the string that matches the pattern
        if match:
            json_str = match.group(1)  # Extracted string
            try:  # Try to convert it into a dictionary
                extracted_dict = json.loads(json_str)  # Replacing $undefined for valid JSON
                # Scraping data from next page
                # Provider Information
                provider_data_dict['npwp_penyedia'] = get_npwp(extracted_dict)
                provider_data_dict['nib_penyedia'] = get_nib(extracted_dict)
                provider_data_dict['alamat_penyedia'] = get_address(extracted_dict)
                provider_data_dict['provinci'] = get_provice(extracted_dict)
                provider_data_dict['kota/kabupaten'] = get_regency(extracted_dict)
                provider_data_dict['keterangan_tambahan'] = get_additional_address(extracted_dict)
                # Violation Information
                provider_data_dict['nomor_sk'] = get_sk_number(extracted_dict)
                violation_data_dict = get_violation_data_dict(extracted_dict)
                provider_data_dict['jenis_pelanggaran'] = violation_data_dict['violation_type']
                provider_data_dict['deskripsi_pelanggaran'] = violation_data_dict['violations_description']
                provider_data_dict['durasi_penayangan'] = violation_data_dict['violation_show_duration']
                provider_data_dict['tanggal_selesai'] = violation_data_dict['end_date']
                # Procurement Package Information
                package_info_dict = get_package_info_dict(extracted_dict)
                provider_data_dict['id_rup/tender'] = package_info_dict['tender_id']
                provider_data_dict['nama_paket'] = package_info_dict['package_name']
                provider_data_dict['jenis_pengadaan'] = package_info_dict['procurement_type']
                provider_data_dict['k/l/pd'] = package_info_dict['government_info']
                provider_data_dict['satuan_kerja_(satker)'] = package_info_dict['work_unit']
                provider_data_dict['hps'] = package_info_dict['hps']
                provider_data_dict['pagu'] = package_info_dict['pagu']
                provider_data_dict['tahun_anggaran'] = package_info_dict['fiscal_year']
                for key in provider_data_dict:
                    provider_data_dict[key] = if_empty_then_na(provider_data_dict[key])
                self.final_data.append(provider_data_dict)  # Inserting Next Page data in previous page data of respective dictionary
                print('data appended...')
            except json.JSONDecodeError as e:
                print("Error parsing JSON:", e)
        else:
            print("Pattern not found")

    def close(self, reason):
        print('closing spider...')
        filename_native_uncleaned = f"{self.filename}_native_uncleaned.xlsx"
        filename_native = f"{self.filename}_native.xlsx"
        print("Converting List of Dictionaries into DataFrame then into Excel file...")
        try:
            print("Creating Native sheet...")
            native_df = pd.DataFrame(self.final_data)
            native_df.drop_duplicates(inplace=True)  # Removing Duplicate data from DataFrame
            native_df = native_df.astype(str)
            # with pd.ExcelWriter(path=filename_native_uncleaned, engine='xlsxwriter') as writer:
            #     native_df.to_excel(excel_writer=writer, index=False)
            # print("Native Excel file Successfully created (not-cleaned).")
            df_cleaner(data_frame=native_df)  # Apply the function to all columns for Cleaning
            with pd.ExcelWriter(path=filename_native, engine='xlsxwriter') as writer:
                native_df.to_excel(excel_writer=writer, index=False)
            print("Native Excel file Successfully created.")
            # try:
            #     trans_start_time = time.time()  # Record the start time
            #     filename_english = fr"{self.filename}_english.xlsx"
            #     print("Creating English sheet...")
            #     columns_to_translate = ['dokumen_surat_keputusan', 'paket', 'alamat_penyedia', 'provinci', 'kota/kabupaten', 'keterangan_tambahan', 'nomor_sk', 'jenis_pelanggaran', 'deskripsi_pelanggaran', 'nama_paket', 'jenis_pengadaan', 'k/l/pd', 'satuan_kerja_(satker)']
            #     translated_df = translate_dataframe_in_chunks(native_df, columns_to_translate)  # Perform chunked translation with parallel processing
            #
            #     # Dictionary mapping Indonesian headers to their English equivalents
            #     translated_column_mapping = {
            #         "penyedia": "provider", "paket": "package", "dokumen_surat_keputusan": "decision_document",
            #         "nomor_paket": "package_number", "tanggal_berlaku": "effective_date",
            #         "tanggal_status": "status_date", "durasi_sanksi": "sanction_duration",
            #         #     ALSO Add new columns translation mapping here ----------------------------
            #         "npwp_penyedia": "npwp_provider", "nib_penyedia": "nib_provider",
            #         "alamat_penyedia": "provider_address", "provinci": "province",
            #         "kota/kabupaten": "city/district", "keterangan_tambahan": "additional_information",
            #         "nomor_sk": "sk_number", "jenis_pelanggaran": "types_of_violations",
            #         "deskripsi_pelanggaran": "violation_description", "durasi_penayangan": "show_duration",
            #         "tanggal_selesai": "end_date", "id_rup/tender": "rup_id",
            #         "nama_paket": "package_name", "jenis_pengadaan": "type_of_procurement",
            #         "k/l/pd": "Ministry/Institution/Regional Government", "satuan_kerja_(satker)": "work_unit_(satker)",
            #         "hps": "hps", "pagu": "pag", "tahun_anggaran": "fiscal_year"
            #     }
            #     translated_df = translated_df.astype(str)
            #     # Assuming you have a DataFrame that you want to rename columns
            #     translated_df.rename(columns=translated_column_mapping, inplace=True)
            #     df_cleaner(translated_df)  # Apply the function to all columns for Cleaning
            #
            #     # replacing 'tahun' with 'year' and 'bulan' with 'month' which are their english translation and which is static so no need to send translate request and avoid overload
            #     translated_df['durasi_penayangan'] = translated_df['durasi_penayangan'].apply(lambda text_: text_.replace('tahun', 'year').replace('bulan', 'month'))
            #
            #     with pd.ExcelWriter(path=filename_english, engine='xlsxwriter') as writer:
            #         translated_df.to_excel(excel_writer=writer, index=False)  # Write data to English Excel file
            #         print("English Excel file Successfully created.")
            #     trans_end_time = time.time()  # Record the end time
            #     print(
            #         f"Total Translation time: {trans_end_time - trans_start_time} seconds")  # Calculate the total execution time
            # except Exception as e:
            #     print('Error while Generating English Excel file:', e)
        except Exception as e:
            print('Error while Generating Native Excel file:', e)
        if self.api.is_connected:  # Disconnecting VPN if it's still connected
            self.api.disconnect()


if __name__ == '__main__':
    execute(f'scrapy crawl {InaprocIdSpider.name}'.split())  # Start the scraping process
