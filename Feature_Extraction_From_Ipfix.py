import csv
import ipaddress
from AmazonS3_Downloader import os, sevenZip_Path
import datetime
from math import ceil
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

features_names_in_csv = list(features_names_in_file.values()) + ['is_ip_new', 'line_index', 'file_name']

dst_ip_index = 0
num_of_packets_sent_index = 1
total_bytes_sent_index = 2
flow_start_time_index = 3
flow_end_time_index = 4
src_port_index = 5
dst_port_index = 6
tcp_control_bits_index = 7
is_ip_new_index = 8

unique_ip_report = 'C:/Dojo_Project/reports/unique_ips_report.txt'
unique_ip_output_report_path = 'D:/Dojo_data_logs/reports'
unique_percentage = 10
trn_percentage = 70
opt_percentage = 0
tst_percentage = 20

# when analyzing the csv data set we update the lists indexes by the percentage of each data part
unique_ip_lines_range = []
trn_data_lines_range = []
opt_data_lines_range = []
tst_data_lines_range = []
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


# pre-condition: the function collect_unique_ip_addresses was called and the file of unique ip addresses was created
# at the unique_ip_report path
''' NEED TO BE UPDATED   '''


def is_ip_address_new(ip_addr):

    with open(unique_ip_report, 'r') as unique_ips:
        ip_list = unique_ips.read().splitlines()
        if ip_addr in ip_list:
            return 1
    return 0


# Function receive a path to a CSV Ipfix file and returns a matrix containing the features of that file
def extract_features_from_file(path):
    try:
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
            # not all files have all the features we need, in this case we ignore the file
            if len(feature_to_index_dict) != len(features_names_in_file):
                return
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
                #final_feature_list.append(int(ipaddress.IPv4Address(initial_features[dst_ip_index])))
                final_feature_list.append( float(''.join(initial_features[dst_ip_index].split('.'))) )
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
                # is-new (binary) - initially we accumulate all the ips into one csv file and later on perform the
                # check whether the ip is new or not, depending on the unique_ip percentage, so the initial value
                # is set to -1
                final_feature_list.append(-1)

                return_matrix.append(final_feature_list)
        return return_matrix

    except Exception as e:
        print("An error occured when extracting features from: %s \n the error is %s" % (path, e))


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


def update_dataset_indexes_list(path_to_csv_dataset):
    global unique_ip_lines_range
    global trn_data_lines_range
    global opt_data_lines_range
    global tst_data_lines_range
    with open(path_to_csv_dataset, 'r') as csv_dataset:
        reader = csv.reader(csv_dataset)
        row_count = sum(1 for row in reader)
        # row_count = len(list(csv.reader(open(path_to_csv_dataset))))

        unique_ip_lines_range = [0, ceil(unique_percentage / 100 * row_count)]
        trn_data_lines_range = [unique_ip_lines_range[1], unique_ip_lines_range[1] +
                                ceil((trn_percentage / 100) * row_count)]
        opt_data_lines_range = [trn_data_lines_range[1], trn_data_lines_range[1] +
                                ceil((opt_percentage / 100) * row_count)]
        tst_data_lines_range = [opt_data_lines_range[1], opt_data_lines_range[1] +
                                ceil((tst_percentage / 100) * row_count)]

    print(unique_ip_lines_range)
    print(trn_data_lines_range)
    print(opt_data_lines_range)
    print(tst_data_lines_range)


# at this point we assume the file is open and the function was called from the features_to_csv function
def update_is_ip_new_in_csv(path_to_temp_dataset, path_to_csv_dataset):
    unique_ips = set()
    with open(path_to_temp_dataset, 'r') as csv_temp_dataset, open(path_to_csv_dataset, 'w+', newline='') as output_dataset:
        reader = csv.reader(csv_temp_dataset)
        writer = csv.writer(output_dataset)
        headers = next(reader)
        writer.writerow(headers)

        ip_index = headers.index(features_names_in_file['dst_ip'])
        for i in range(unique_ip_lines_range[0], unique_ip_lines_range[1]):
            line = next(reader)
            unique_ips.add(line[ip_index])
            writer.writerow(line)

        # at this point we finished going through the part allocated for finding unique ips
        # and start updating the rest of the file accordingly
        for line in reader:
            if line[ip_index] in unique_ips:
                line[is_ip_new_index] = 0
            else:
                line[is_ip_new_index] = 1
            writer.writerow(line)

    os.remove(path_to_temp_dataset)


# NOTICE: output_report_path should include the name of the report (example: C:/blah/.../report1.csv
def features_to_csv(path_to_folder, output_report_path, to_create):
    temp = '_temp.csv'
    temp_output = output_report_path[:-4] + temp
    if to_create:
        output_csv_file = open(temp_output, 'w+', newline='')
    else:
        output_csv_file = open(temp_output, 'a')

    writer = csv.writer(output_csv_file)
    writer.writerow(features_names_in_csv)
    features_to_csv_subfolder(path_to_folder, temp_output, writer)
    output_csv_file.close()
    update_dataset_indexes_list(temp_output)
    update_is_ip_new_in_csv(temp_output, output_report_path)


def analyze_csv_dataset(path_to_dataset):
    with open(path_to_dataset, 'r') as csv_dataset:
        pass


if __name__ == '__main__':

    features_to_csv('D:/Dojo_data_logs/ipfix-09.2018(filtered)(csv files)',
                    'D:/Dojo_data_logs/september_dataset.csv',
                    True)
    # update_dataset_indexes_list('D:/Dojo_data_logs/september_dataset.csv')
    # with open('D:/Dojo_data_logs/september_dataset.csv', 'r') as csv_file, \
    #         open('D:/Dojo_data_logs/check.txt', 'w+') as output:
    #     reader = csv.reader(csv_file)
    #     for line in reader:
    #         output.write('      '.join(line[8]) + '\n')
