# author: itamar bitton

'''
this script takes a path as an argument and convert a txt file containing a list of jsons to a csv file to all folders
and sub-folders in a asynchronous way order
'''

import csv
import os
import json
from MAC_Addr_File_Filter import create_output_directory
from csv import DictWriter
from multiprocessing import Pool
from functools import partial

'''     CONSTANTS         '''
output_path = 'D:/Dojo_data_logs/ipfix-09.2018(filtered)(csv files)'
folder_to_convert = 'D:/Dojo_data_logs/ipfix-09.2018(filtered)'
num_of_threads = 20
'''                       '''


def flattenjson(b, delim):
    val = {}
    for i in b.keys():
        if isinstance(b[i], dict):
            get = flattenjson(b[i], delim)
            for j in get.keys():
                val[i + delim + j] = get[j]
        else:
            val[i] = b[i]

    return val


# function receive as an argument a path to a txt file containing a list of json format rows
# and replace the txt file with a matching csv file
def write_csv(path_to_txt, output_path):
    os.chdir(os.path.dirname(path_to_txt))
    output_file = output_path + '/' + os.path.basename(path_to_txt)[:-4] + '.csv'
    with open(path_to_txt, 'r') as txt_file, open(output_file, 'w+', newline='') as csv_file:
        writer = csv.writer(csv_file)
        lines = txt_file.readlines()
        ''' first iteration on the file is to get all the different json keys due to the fact that not all json
            have the same key set'''
        key_set = set([])
        for line in lines:
            parsed_json = json.loads(line)
            flat_parsed_json = flattenjson(parsed_json, "__")
            for element in flat_parsed_json.keys():
                key_set.add(element)
        writer.writerow(list(key_set))
        ''' second iteration performs the actual writing of the data of each json row to its right columns '''
        dict_writer = DictWriter(csv_file, list(key_set))
        for line in lines:
            row = {}
            keys = list(key_set)
            for i in range(0, len(keys)):
                row[keys[i]] = 'NA'
            parsed_json = json.loads(line)
            flat_parsed_json = flattenjson(parsed_json, "__")

            for key in flat_parsed_json.keys():
                row[key] = flat_parsed_json[key]
            dict_writer.writerow(row)


# function receive a path to a directory and convert all the files in the directory and -directories in an async way
# make sure that the output_path folder exists !
def convert(num_of_threads, path_to_convert, output_path):
    txt_files = []
    files_lst = os.listdir(path_to_convert)
    pool = Pool(num_of_threads)
    for f in files_lst:
        os.chdir(path_to_convert)
        if os.path.isdir(f):
            new_output = output_path + '/' + f + '(csv files)'
            create_output_directory(new_output)
            convert(num_of_threads, path_to_convert + '/' + f, new_output)
        else:
            txt_files.append(path_to_convert + '/' + f)

    partial_write = partial(write_csv, output_path=output_path)
    pool.map(partial_write, txt_files)
    pool.close()
    pool.join()


def main():
    convert(num_of_threads, folder_to_convert, output_path)
    # create_output_directory(output_path)
    # for path in list_to_convert:
    #     convert(3, path, output_path)


if __name__ == '__main__':
    main()
