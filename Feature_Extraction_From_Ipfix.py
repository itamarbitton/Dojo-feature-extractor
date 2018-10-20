from Ipfix_Constants import *
import ipaddress
import datetime
from math import ceil
import time
import queue
import threading
import operator
'''                 CONSTANTS                   '''
features_names_in_file = {
            'dst_ip': 'ipfix__destinationIPv4Address',
            'num_of_packets_sent': 'ipfix__packetDeltaCount',
            'total_bytes_sent': 'ipfix__octetDeltaCount',
            'flow_start_time': 'ipfix__flowStartMilliseconds',
            'flow_end_time': 'ipfix__flowEndMilliseconds',
            'src_port': 'ipfix__sourceTransportPort',
            'dst_port': 'ipfix__destinationTransportPort',
            'tcp_control_bits': 'ipfix__tcpControlBits',
            'timestamp': '@timestamp'}

features_names_in_csv = ['total_packets_sent', 'total_bytes_sent', 'src_port', 'dst_port',
                         'tcp_control_bits', 'flow_duration', 'part of day', 'last_minute', 'last_hour', 'last_day',
                         'line_index', 'file_name']

dst_ip_index = 0
num_of_packets_sent_index = 1
total_bytes_sent_index = 2
flow_start_time_index = 3
flow_end_time_index = 4
src_port_index = 5
dst_port_index = 6
tcp_control_bits_index = 7
timestamp_index = 8


# when analyzing the csv data set we update the lists indexes by the percentage of each data part
unique_ip_lines_range = []
trn_data_lines_range = []
opt_data_lines_range = []
tst_data_lines_range = []
row_count = 0

path_to_folder = 'C:/Dojo_Project/Dojo_data_logs/ipfix-09.2018(filtered)(csv files)'
output_report_path = 'C:/Dojo_Project/Dojo_data_logs/september_dataset.csv'
# path_to_folder = 'D:/Dojo_data_logs/ipfix-09.2018(filtered)(csv files)'
# output_report_path = 'D:/Dojo_data_logs/september_dataset.csv'
'''                                            '''


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


def last_minute_hour_day(date_str, ip_past_dates):
    last_minute = 0
    last_hour = 0
    last_day = 0
    for date in ip_past_dates:
        parsed_date_str = parse_date(date_str)
        parsed_date = parse_date(date)
        if parsed_date_str < parsed_date:
            continue
        diff = parsed_date_str - parsed_date
        days, seconds = diff.days, diff.seconds
        if days == 0:
            last_day += 1
            hours = days * 24 + seconds // 3600
            if hours == 0:
                last_hour += 1
                minutes = (seconds % 3600) // 60
                if minutes == 0:
                    last_minute += 1

    return [last_minute, last_hour, last_day]


# Function receive a path to a CSV Ipfix file and returns a matrix containing the features of that file
def extract_initial_features_from_file(path):
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
                final_feature_list.append(int(ipaddress.IPv4Address(initial_features[dst_ip_index])))
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

                # timestamp for past ipfix's calculations
                final_feature_list.append(initial_features[timestamp_index])

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
            features_matrix = extract_initial_features_from_file(path_to_file)
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
    global row_count
    with open(path_to_csv_dataset, 'r') as csv_dataset:
        reader = csv.reader(csv_dataset)
        row_count = sum(1 for row in reader)

        unique_ip_lines_range = [1, ceil(unique_percentage / 100 * row_count)]
        trn_data_lines_range = [unique_ip_lines_range[1], unique_ip_lines_range[1] +
                                ceil((trn_percentage / 100) * row_count)]
        opt_data_lines_range = [trn_data_lines_range[1], trn_data_lines_range[1] +
                                ceil((opt_percentage / 100) * row_count)]
        tst_data_lines_range = [opt_data_lines_range[1], opt_data_lines_range[1] +
                                ceil((tst_percentage / 100) * row_count)]

        with open((reports_folder + '/' +
                   (os.path.basename(path_to_csv_dataset)[:-4])) + '_meta.csv', 'w+', newline='') as meta_csv:
            writer = csv.writer(meta_csv)
            writer.writerow(['path to dataset', path_to_csv_dataset])
            writer.writerow(['unique_ip_batch_size', unique_ip_lines_range[1] - unique_ip_lines_range[0]])
            writer.writerow(['trn_data_batch_size', trn_data_lines_range[1] - trn_data_lines_range[0]])
            writer.writerow(['opt_data_batch_size', opt_data_lines_range[1] - opt_data_lines_range[0]])
            writer.writerow(['tst_data_batch_size', tst_data_lines_range[1] - tst_data_lines_range[0] - 1])


# at this point we assume the file is open and the function was called from the features_to_csv function
def create_final_dataset_with_info(path_to_temp_dataset, path_to_csv_dataset):
    # unique_ips = set()
    ip_to_dates = {}
    progress = 0
    with open(path_to_temp_dataset, 'r') as csv_temp_dataset, open(path_to_csv_dataset, 'w+', newline='') as output_dataset:
        reader = csv.reader(csv_temp_dataset)
        writer = csv.writer(output_dataset)
        writer.writerow(features_names_in_csv)
        ip_index = dst_ip_index
        # for i in range(unique_ip_lines_range[0], unique_ip_lines_range[1]):
        #     line = next(reader)
        #     unique_ips.add(line[ip_index])

        # creating the final feature vector plus two info columns (index in file & file name)
        sortedlist = sorted(reader, key=operator.itemgetter(timestamp_index), reverse=False)
        def writer_task():
            while True:
                item = q.get()
                if item is None:
                    break
                writer.writerow(item)
                q.task_done()

        q = queue.Queue()
        writing_thread = threading.Thread(target=writer_task)
        writing_thread.start()

        for line in sortedlist:
            '''
            the order of the features written in the temp csv file
            0)ip (converted to numeric value)
            1)total number of packets sent
            2)total number of bytes sent
            3)src port
            4)dst port
            5)tcp control bits
            6)session duration
            7)part of day
            8) time stamp
            
            the order we want in the output dataset file:
            0)total_packets_sent
            1)total_bytes_sent
            2)src_port
            3)dst_port
            4)flow_duration
            5)tcp_control_bits
            6)part of day
            7)last minute
            8)last hour
            9)last day
            '''
            out_line = []
            out_line.append(line[1])
            out_line.append(line[2])
            out_line.append(line[3])
            out_line.append(line[4])
            out_line.append(line[5])
            out_line.append(line[6])
            out_line.append(line[7])
            # if line[0] in unique_ips:
            #     out_line.append(0)
            # elif line[0] not in unique_ips:
            #     out_line.append(1)
            # else:
            #     print('error with unique ip')

            if line[ip_index] in ip_to_dates.keys():
                last_minute, last_hour, last_day = last_minute_hour_day(line[timestamp_index],
                                                                        ip_to_dates[line[ip_index]])
                out_line.append(last_minute)
                out_line.append(last_hour)
                out_line.append(last_day)
                ip_to_dates[line[ip_index]].append(line[timestamp_index])
            else:
                ip_to_dates[line[ip_index]] = [line[timestamp_index]]
                out_line.append(0)
                out_line.append(0)
                out_line.append(0)

            out_line += line[-2:]  # adding the row index and the file name

            q.put(out_line)

            # writer.writerow(out_line)
            # just for keeping track - delete later if necessary
            progress += 1
            if (progress % 50000) == 0:
                print('progress is:', progress)


        # block until all tasks are done
        q.join()
        q.put(None)
        writing_thread.join()
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
    features_to_csv_subfolder(path_to_folder, temp_output, writer)
    output_csv_file.close()
    update_dataset_indexes_list(temp_output)
    create_final_dataset_with_info(temp_output, output_report_path)


if __name__ == '__main__':
    start_time = time.time()

    features_to_csv(path_to_folder,
                    output_report_path,
                    True)
    print("--- %s seconds ---" % (time.time() - start_time))

