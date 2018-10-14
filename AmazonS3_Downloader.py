# author: itamar bitton
'''
this script downloads compressed files from the dojo AWS server and extract them on the local machine
'''
from Ipfix_Constants import *

'''     CONSTANTS        '''
LOCAL__DOWNLOAD_PATH = "E:/Dojo_data_logs"
AWS_PATH = 's3://dojo-data-logs/'
year = '2018'
inline_path = '/json' + '/' + year
buckets = ('dhcpfp', 'dnsSniffer', 'httpua', 'ipfix', 'mdns', 'mdnsfull', 'nbns', 'upnp')
days_in_months_dict = {'01': 31, '02': 28, '03': 31, '04': 30, '05': 31, '06': 30, '07': 31, '08': 31, '09': 30, '10': 31, '11': 30, '12': 31}
'''                     '''


def download_and_extract_files(srcPath, dstPath):
    os.chdir(dstPath)  # change working directory to the downloaded files folder for simplicity purposes
    os.system('aws s3 sync %s %s' % (srcPath, dstPath))  # downloading the files
    '''
    for f in os.listdir(dstPath):
        extract_command = '%s e %s' % (sevenZip_Path, f) + " -o" + dstPath  # extract file from gz to txt format
        os.system(extract_command)
        delete_command = 'del %s' % f  # delete the gz file as it is no longer needed
        os.system(delete_command)
    '''


def download_and_extract_day(bucket, month, day):
    full_download_path = AWS_PATH + bucket + inline_path + '/' + "%02d/%02d" % (month, day)
    new_folder_name = "%s-%02d.%02d.%s" % (bucket, day, month, year)
    dst_folder = os.getcwd() + '\\' + new_folder_name
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)
        download_and_extract_files(full_download_path, dst_folder)
    else:
        raise Exception('Files under the same date already exist in your machine. \n'
                        'make sure the download path is correct.')


def download_and_extract_in_range(bucket, month, start_day, end_day):
    new_folder_name = "%s-%02d.%02d.%s-%02d.%02d.%s" % (bucket, start_day, month, year, end_day, month, year)
    dst_folder = os.getcwd() + '\\' + new_folder_name
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)
        for day in range(start_day, end_day+1):
            os.chdir(dst_folder)
            download_and_extract_day(bucket, month, day)
    else:
        raise Exception('Files under the same date already exist in your machine. \n'
                        'make sure the download path is correct.')


def download_and_extract_month(bucket, month):
    new_folder_name = "%s-%02d.%s" % (bucket, month, year)
    dst_folder = os.getcwd() + '\\' + new_folder_name
    if not os.path.exists(dst_folder):
        os.makedirs(dst_folder)
        end_day = days_in_months_dict['%02d' % month]
        for day in range(1, end_day + 1):
            os.chdir(dst_folder)
            download_and_extract_day(bucket, month, day)
    else:
        raise Exception('Files under the same date already exist in your machine. \n'
                        'make sure the download path is correct.')


def main():
    os.chdir(LOCAL__DOWNLOAD_PATH)  # change working directory to the downloaded files folder for simplicity purposes
    # download_and_extract_month('ipfix', 9)
    download_and_extract_day('ipfix', 9, 30)


if __name__ == '__main__':
    main()
