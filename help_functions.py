import time
from _datetime import datetime, timedelta
import numpy as np
from zoho_api.api import api_request

def unix_to_date(ts_value):
    try:
        unix_timestamp_sec = (int(ts_value) + 3600000) / 1000
        date = datetime.fromtimestamp(unix_timestamp_sec)
        return date.strftime('%Y-%m-%d')
    except Exception as e:
        print(e)
        print('oshibkus')
        return None

def format_hours(hours):
    h = int(hours)
    m = int(round(round(hours - h, 2) * 60, 0))
    return f"{h}:{m:02}"

def str_to_date(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d").date()


def datetime_str_to_unix(date_string, hours, minutes):
    if not date_string:
        return ""
    date_obj = datetime.strptime(date_string, "%Y-%m-%d")
    date_obj = date_obj.replace(hour=hours, minute=minutes, second=0)
    #gmt_plus_2 = pytz.timezone("Etc/GMT+2")
    #date_obj_gmt_plus_2 = gmt_plus_2.localize(date_obj)
    unix_timestamp = int(date_obj.timestamp()) * 1000
    return unix_timestamp


def str_to_datetime(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d")


def str_to_unix(date_string):
    return int(str_to_datetime(date_string).timestamp() * 1000)


def str_to_str_date(date_string):
    date_obj = str_to_date(date_string)
    return date_obj.strftime("%B %Y")


def get_working_days(start, end):
    return np.busday_count(str_to_date(start), str_to_date(end) + timedelta(days=1))


def batch_generator(data, batch_size=100):
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]



def get_zp_employees():
    s_index = 1
    result = []
    while True:
        try:
            page_employees = api_request(
                "https://people.zoho.com/people/api/forms/employee/getRecords?sIndex=" + str(
                    s_index) + "&searchParams={searchField: 'Employeestatus', searchOperator: 'Is', searchText : 'Active'}",
                "zoho_people",
                "get",
                None
            )['response']['result']
        except KeyError:
            break
        for page_employee in page_employees:
            zp_employee_id = list(page_employee.keys())[0]
            zp_employee_data = page_employee[zp_employee_id][0]
            result.append({"email": zp_employee_data['EmailID'], "department": zp_employee_data['Department'], 'id': zp_employee_data['Zoho_ID'], 'name': f'{zp_employee_data["FirstName"]} {zp_employee_data["LastName"]}', "team": zp_employee_data["Team"], "staff_type": zp_employee_data["Staff_type"], 'short_id': zp_employee_data['EmployeeID']})
        s_index += 200
    return result