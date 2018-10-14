from Ipfix_Constants import *


def get_macs_from_tsv():
    mac_lst = []
    # getting the MACs for the report and arranging the format for the report
    with open(tsv_file, 'r') as tsv:
        reader = csv.reader(tsv, dialect='excel-tab')
        for row in reader:
            mac_lst.append(row[2])
    return mac_lst


# function create a mac report from the tsv file provided by dojo - indicating for each mac and each day
# how many instances it has on that day from the ipfix's files
def create_mac_instances_report(mac_list, path_to_filtered_txt_files_folder, output_report_path):
    output = []
    day_num = 0
    number_of_days = 5
    MAC, Day, instances_num = 'MAC', 'Day', '# of instances'
    # # getting the MACs for the report and arranging the format for the report
    for mac in mac_list:
        for day in range(1, number_of_days+1):
            to_add = {MAC: mac, Day: day, instances_num: 0}
            output.append(to_add)

    field = '"sourceMacAddress":'
    for folder in os.listdir(path_to_filtered_txt_files_folder):
        day_num += 1
        os.chdir(path_to_filtered_txt_files_folder + '/' + folder)
        for file in os.listdir(path_to_filtered_txt_files_folder + '/' + folder):
            with open(os.path.abspath(file), 'r') as txt_file:
                for row in txt_file:
                    index = row.index(field)
                    mac = row[index+len(field)+1:index+len(field)+18]
                    for record in output:
                        if record[MAC] == mac and record[Day] == day_num:
                            record[instances_num] += 1

    with open(output_report_path + '/' + 'MAC days report.csv', 'wb+') as output_file:
        writer = csv.writer(output_file)
        writer.writerow([MAC, Day, instances_num])
        for record in output:
            writer.writerow([record[MAC], record[Day], record[instances_num]])


# function get the most active macs from the report provided by the create_mac_instances_report function
# active = all days that were measured have a number of instances bigger than zero
def get_most_active_macs(path_to_csv_report, output_folder, days_num):
    with open(path_to_csv_report, 'r') as csv_report, open(output_folder + '/' + 'most_active_MACs.txt', 'wb+') as output:
        reader = csv.reader(csv_report)
        next(reader)  # just to ignore the headers of each column
        index = 0
        valid_mac = True
        for row in reader:
            index += 1
            if row[2] == '0':
                valid_mac = False
            if valid_mac and index == days_num:
                output.write(row[0] + '\n')
            if index == 5:
                index = 0
                valid_mac = True


def main():
    pass


if __name__ == '__main__':
    main()
