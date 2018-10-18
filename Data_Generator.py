from Ipfix_Constants import *


class Data_Gen():
    def __init__(self):
        self.reader = None

    def trn_data_gen(self, batch_sz, offset_into_file):
        current_row = 0
        csv_file = open(dataset_path, 'r')
        self.reader = csv.reader(csv_file)
        next(self.reader)  # ignore the headers
        for i in range(0, offset_into_file):
            next(self.reader)
        while True:
            if current_row < batch_sz:
                try:
                    line = next(self.reader)
                    yield [[[float(i) for i in line[:-2]]], []]
                    current_row += 1
                except StopIteration as e:
                    print('current row: ', current_row)
            else:
                current_row = 0
                csv_file = open(dataset_path, 'r')
                self.reader = csv.reader(csv_file)
                next(self.reader)  # ignore the headers
                for i in range(0, offset_into_file):
                    next(self.reader)

    def tst_data_gen(self, trn_and_opt_batch_size, tst_batch_sz):
        current_row = 0
        csv_file = open(dataset_path, 'r')
        self.reader = csv.reader(csv_file)
        next(self.reader)  # ignore the headers
        for i in range(0, trn_and_opt_batch_size):
            next(self.reader)
        while True:
            if current_row < tst_batch_sz:
                try:
                    line = next(self.reader)
                    yield [[[float(i) for i in line[:-2]]], []]
                    current_row += 1
                except StopIteration as e:
                    print('current row: ', current_row)
                    quit()
            else:
                current_row = 0
                csv_file = open(dataset_path, 'r')
                self.reader = csv.reader(csv_file)
                next(self.reader)  # ignore the headers
                for i in range(0, trn_and_opt_batch_size):
                    next(self.reader)


