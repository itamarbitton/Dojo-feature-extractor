# Do NOT remove imports, they are used in other scripts importing this one
import os
import csv

'''             REPORTS CONSTANTS              '''
# path to the tsv file provided by dojo containing mac addresses of amazon ring devices
tsv_file = 'D:/reports/select___d_last_seen__d_box_id__d_device.tsv'
# the path to the september data-set including all the features for the auto-encoder + two info columns
dataset_path = 'D:/Dojo_data_logs/september_dataset.csv'
# the amazon ring macs that were active all last five days of september
most_used_macs = 'D:/reports/most_active_MACs.txt'
# error log when for when extracting a compressed file fails for some reason (belongs to filter_compressed_file
errors_file = 'D:/reports/errors.txt'
# path to reports folder
reports_folder = 'D:/reports'

'''             PROGRAMS PATHS CONSTANTS           '''
sevenZip_Path = '"C:/Program Files/7-Zip/7z.exe"'



