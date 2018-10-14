import os
import csv
from multiprocessing import Pool
from functools import partial
from Ipfix_Constants import *

''' CONSTANTS '''
are_files_compressed = True
mac_list = []
src_mac_in_json = '"sourceMacAddress":'
num_of_threads = 15
errors_file = 'D:/reports/errors.txt'
'''           '''


def decompress_file(path_to_compressed_file, output_path):
    extract_command = '%s e %s' % (sevenZip_Path, path_to_compressed_file) + " -o" + output_path  # extract file from gz to txt format
    success = os.system(extract_command)
    if success != 0:
        return None
    else:
        return path_to_compressed_file[:-3]


def get_MACs_from_txt(path_to_txt):
    mac_lst = []
    with open(path_to_txt, 'r') as txt_file:
        for line in txt_file.read().splitlines():
            mac_lst.append(line)
    return mac_lst


# function returns true if the expression joined with a mac address appear in the line
def filter_line(expression, line, mac_list):
    for mac in mac_list:
        if expression + '"{}"'.format(mac) in line:
            return True
    return False


# this function decompresses the file, filter it and then delete the decompressed file
def filter_compressed_file(path_to_gz_file, output_path, mac_list):
    file_dir = '/'.join(path_to_gz_file.split('/')[:-1])
    path_to_decompressed = decompress_file(path_to_gz_file, file_dir)
    if path_to_decompressed is not None:
        filter_file(path_to_decompressed, output_path, mac_list)
        os.remove(path_to_decompressed)
    else:
        with open(errors_file, 'a') as error_f:
            error_f.write('could not extract file: %s \n' % path_to_gz_file)


def filter_file(path_to_txt_file, output_path, mac_list):
    output_file = output_path + '/' + os.path.basename(path_to_txt_file)[:-4] + '(filtered).txt'
    with open(path_to_txt_file, 'r') as f, open(output_file, 'w+') as fout:
        for line in f.readlines():
            if filter_line(src_mac_in_json, line, mac_list):
                fout.write(line)


# the mac arg is a list of mac addresses to filter by
# make sure the output_folder_path exists !!
def filter_files_in_folder(num_of_threads, path_to_folder, output_folder_path, mac):
    files_lst = os.listdir(path_to_folder)
    txt_files = []
    pool = Pool(num_of_threads)
    for f in files_lst:
        os.chdir(path_to_folder)
        if os.path.isdir(f):
            new_output = output_folder_path + '/' + f + '(filtered)'
            create_output_directory(new_output)
            filter_files_in_folder(num_of_threads, path_to_folder + '/' + f, new_output, mac)
            pass
        else:
            txt_files.append(path_to_folder + '/' + f)
    if are_files_compressed:
        partial_write = partial(filter_compressed_file, output_path=output_folder_path, mac_list=mac)
    else:
        partial_write = partial(filter_file, output_path=output_folder_path, mac_list=mac)
    pool.map(partial_write, txt_files)
    pool.close()
    pool.join()


def filter_by_tsv_file(path_to_tsv, path_to_filter, output_path):
    with open(path_to_tsv, 'r') as tsv:
        reader = csv.reader(tsv, dialect='excel-tab')
        for row in reader:
            mac_list.append(row[2])

        filter_files_in_folder(num_of_threads, path_to_filter, output_path, mac_list)


def create_output_directory(output):
    path = output.split('/')
    os.chdir(path[0] + '/')
    for direc in path[1:]:
        if not os.path.exists(direc):
            os.mkdir(direc)
        os.chdir(os.getcwd() + '/' + direc)


def main():
    filter_files_in_folder(num_of_threads,
                           'D:/Dojo_data_logs/temp/ipfix-23.09.2018',
                           'D:/Dojo_data_logs/ipfix-09.2018(filtered)/ipfix-23.09.2018(filtered)',
                           get_MACs_from_txt(most_used_macs))


if __name__ == '__main__':
    main()
