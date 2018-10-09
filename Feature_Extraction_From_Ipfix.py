import csv
import ipaddress
from AmazonS3_Downloader import os, sevenZip_Path
import datetime
'''
Features that are currently extracted:
** destination ip - converted to numeric value
** is_new - whether the ip is new compared to the base data sample
** number of packets sent
** number of bytes sent
** source port
** destination port
** flags (aka control bits)
** duration of the session
** part of day (coding: morning(00:00-08:00) = 0, noon(08:00-16:00) = 1, night(16:00-00:00) = 2
'''
'''                 CONSTANTS                   '''
features_names_in_file = {
            'dst_ip': 'ipfix__destinationIPv4Address',
            'num_of_packets_sent': 'ipfix__packetDeltaCount',
            'total_bytes_sent': 'ipfix__octetDeltaCount',
            'flow_start_time': 'ipfix__flowStartMilliseconds',
            'flow_end_time': 'ipfix__flowEndMilliseconds',
            'src_port': 'ipfix__sourceTransportPort',
            'dst_port': 'ipfix__destinationTransportPort',
            'tcp_control_bits': 'ipfix__tcpControlBits'}

features_names_in_csv = list(features_names_in_file.values()) + ['line_index', 'file_name']

dst_ip_index = 0
num_of_packets_sent_index = 1
total_bytes_sent_index = 2
flow_start_time_index = 3
flow_end_time_index = 4
src_port_index = 5
dst_port_index = 6
tcp_control_bits_index = 7

base_week = 'D:/Dojo_data_logs/ipfix-09.2018(filtered)/base_week'
unique_ip_report = 'D:/reports/unique_ips_report.txt'
unique_ip_output_report_path = 'D:/Dojo_data_logs/reports'
unique_percentage = 10
trn_percentage = 70
opt_percentage = 0
tst_percentage = 10
'''                                            '''


def decompress_file(path_to_compressed_file, output_path):
    extract_command = '%s e %s' % (sevenZip_Path, path_to_compressed_file) + " -o" + output_path  # extract file from gz to txt format
    success = os.system(extract_command)
    if success != 0:
        return None
    else:
        return path_to_compressed_file[:-3]


def delete_decompressed_file(path_to_compressed_file):
    delete_command = 'del %s' % path_to_compressed_file  # delete the gz file as it is no longer needed
    os.system(delete_command)


def parse_date(date_str):
    date, time = (date_str[:-1]).split('T')
    year, month, day = [int(i) for i in date.split('-')]
    hours, minutes, sec = time.split(':')
    hours = int(hours)
    minutes = int(minutes)
    seconds, milliseconds = [int(i) for i in sec.split('.')]
    return_date = datetime.datetime(year, month, day, hours, minutes, seconds, milliseconds*(10**3))
    return return_date


def calculate_date_difference(start_datetime, end_datetime):
    parsed_start_datetime = parse_date(start_datetime)
    parsed_end_datetime = parse_date(end_datetime)
    difference = parsed_end_datetime - parsed_start_datetime
    return difference.total_seconds() * (10**3)


def extract_unique_ip_addresses_from_file(path_to_file, unique_ip_addresses):
    with open(path_to_file, 'r') as f:
        field = '"destinationIPv4Address":'
        for row in f:
            index = row.index(field)
            ip = row[index + len(field) + 1: index + len(field) + 1 + row[index + len(field) + 1:].index('"')]
            unique_ip_addresses.add(ip)


def extract_unique_ip_addresses_from_subfolder(path_to_folder, unique_ip_addresses):
    for subfile in os.listdir(path_to_folder):
        os.chdir(path_to_folder)
        if os.path.isdir(subfile):
            extract_unique_ip_addresses_from_subfolder(path_to_folder + '/' + subfile, unique_ip_addresses)
        else:
            extract_unique_ip_addresses_from_file(subfile, unique_ip_addresses)


def extract_unique_ip_addresses_from_folder(path_to_folder, output_folder):
    unique_ip_addresses = set()
    for subfile in os.listdir(path_to_folder):
        os.chdir(path_to_folder)
        if os.path.isdir(subfile):
            extract_unique_ip_addresses_from_subfolder(path_to_folder + '/' + subfile, unique_ip_addresses)
        else:
            extract_unique_ip_addresses_from_file(subfile, unique_ip_addresses)

    if os.path.exists(output_folder):
        os.remove(output_folder)
    with open(output_folder, 'w+') as out:
        for ip in unique_ip_addresses:
            out.write(ip + '\n')


# pre-condition: the function collect_unique_ip_addresses was called and the file of unique ip addresses was created
# at the unique_ip_report path
def is_ip_address_new(ip_addr):
    with open(unique_ip_report, 'r') as unique_ips:
        ip_list = unique_ips.read().splitlines()
        if ip_addr in ip_list:
            return 1
    return 0


# Function receive a path to a CSV Ipfix file and returns a matrix containing the features of that file
def extract_features_from_file(path):
    feature_to_index_dict = {}  # mapping each feature to its column's index in the row
    return_matrix = []
    with open(path, 'r') as csv_file:
        reader = csv.reader(csv_file)
        headers = next(reader)
        # in case the file is empty after filtering
        if not headers:
            return
        for index in range(len(headers)):
            if headers[index] in features_names_in_file.values():
                feature_to_index_dict[headers[index]] = index
        index_lst = list(feature_to_index_dict.values())
        index_lst.sort()
        for row in reader:
            initial_features = []
            for feature in features_names_in_file.values():
                initial_features.append(row[feature_to_index_dict[feature]])
            if 'NA' in initial_features:
                continue
            # at this point to_add contains the features values by the order they appear in features_names_in_file dict
            # next part of code create the final list of the features needed to be derived from the existing ones
            final_feature_list = []
            # dst-ip (converted to numeric value)
            final_feature_list.append(int(ipaddress.IPv4Address(initial_features[dst_ip_index])))
            # is-new (binary)
            final_feature_list.append(is_ip_address_new(initial_features[dst_ip_index]))
            # number of packets sent # CHANGE TO INT
            final_feature_list.append(int(initial_features[num_of_packets_sent_index]))
            # number of bytes sent
            final_feature_list.append(initial_features[total_bytes_sent_index])
            # source port
            final_feature_list.append(initial_features[src_port_index])
            # destination port
            final_feature_list.append(initial_features[dst_port_index])
            # flags(aka control bits)
            final_feature_list.append(initial_features[tcp_control_bits_index])
            # duration of the session (in milliseconds)
            session_duration = calculate_date_difference(initial_features[flow_start_time_index],
                                                         initial_features[flow_end_time_index])
            final_feature_list.append(session_duration)
            #  part of day
            # (coding: morning(00:00 - 0 8: 00) = 0, noon(0 8: 00 - 16:00) = 1, night(16: 00 - 00:00) = 2
            parsed_start_date = parse_date(initial_features[flow_start_time_index])
            hour = parsed_start_date.hour
            if 0 <= hour <= 8:
                part_of_day = 0
            elif 9 <= hour <= 16:
                part_of_day = 1
            else:
                part_of_day = 2
            final_feature_list.append(part_of_day)

            return_matrix.append(final_feature_list)

    return return_matrix


# NOTICE: this function should only be called from features_to_csv() and therefor the output_report_path file is already
# open
def features_to_csv_subfolder(path_to_subfolder, output_report_path, writer):
    for file in os.listdir(path_to_subfolder):
        path_to_file = path_to_subfolder + '/' + file
        if os.path.isdir(path_to_file):
            features_to_csv_subfolder(path_to_file, output_report_path, writer)
        else:
            line_index = 0
            features_matrix = extract_features_from_file(path_to_file)
            if features_matrix is None:
                continue
            for feature_vector in features_matrix:
                csv_feature_vector = feature_vector + [line_index, file]
                line_index += 1
                writer.writerow(csv_feature_vector)


# NOTICE: output_report_path should include the name of the report (example: C:/blah/.../report1.csv
def features_to_csv(path_to_folder, output_report_path, to_create):
    output_csv_file = None
    if to_create:
        output_csv_file = open(output_report_path, 'w+', newline='')
    else:
        output_csv_file = open(output_report_path, 'a')

    writer = csv.writer(output_csv_file)
    writer.writerow(features_names_in_csv)
    features_to_csv_subfolder(path_to_folder, output_report_path, writer)


if __name__ == '__main__':
    features_to_csv('D:/Dojo_data_logs/ipfix-09.2018(filtered)(csv files)', 'D:/Dojo_data_logs/september_dataset.csv', True)
