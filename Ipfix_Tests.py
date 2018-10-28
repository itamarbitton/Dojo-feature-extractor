from temp_AutoEncoder import *

# EXPERIMENT 1.0 - we take all training data and test it
# EXPERIMENT 1.1 - we take 66& training data and test it
# EXPERIMENT 1.2 - we take 33& training data and test it
# compare 1.0, 1.1, 1.2 results

# EXPERIMENT 2.0 - we take 33% of most recent training data and test it
# EXPERIMENT 2.1 - we take 33% of training data between the most recent recent data and oldest data and test it
# EXPERIMENT 2.2 - we take 33% of oldest training data and test it
# EXPERIMENT 2.3 - we take 66% of the most recent training data and test it
# EXPERIMENT 2.4 - we take 66% of oldest training data and test it
# compare 2.0, 2.1, 2.2 results

trn_size = 0
opt_size = 0
tst_size = 0


def begin_experiments():
    global trn_size, opt_size, tst_size
    with open(september_meta_report, 'r') as csv_meta_report:
        reader = csv.reader(csv_meta_report)
        next(reader)  # ignore the path to dataset
        next(reader)  # ignore the unique number of ip samples

        trn_size = int(next(reader)[1])
        opt_size = int(next(reader)[1])
        tst_size = int(next(reader)[1])

        # result1_0 = experiment_1_0()
        # print('result for experiment 1.0 = ', result1_0)
        # result1_1 = experiment_1_1()
        # print('result for experiment 1.1 = ', result1_1)
        # result1_2 = experiment_1_2()
        # print('result for experiment 1.2 = ', result1_2)
        #
        # result2_0 = experiment_2_0()
        # print('result for experiment 2.0 = ', result2_0)
        # result2_1 = experiment_2_1()
        # print('result for experiment 2.1 = ', result2_1)
        # result2_2 = experiment_2_2()
        # print('result for experiment 2.2 = ', result2_2)
        result2_3 = experiment_2_3()
        print('result for experiment 2.3 = ', result2_3)
        result2_4 = experiment_2_4()
        print('result for experiment 2.4 = ', result2_4)


def experiment_1_0():
    result = train_and_test(trn_size, 0)
    return result


def experiment_1_1():
    result = train_and_test(int(0.66 * trn_size), 0)
    return result


def experiment_1_2():
    result = train_and_test(int(0.33 * trn_size), 0)
    return result


def experiment_2_0():
    result = train_and_test(int(0.33 * trn_size), int(0.66 * trn_size))
    return result


def experiment_2_1():
    result = train_and_test(int(0.33 * trn_size), int(0.33 * trn_size))
    return result


def experiment_2_2():
    result = train_and_test(int(0.33 * trn_size), 0)
    return result


def experiment_2_3():
    result = train_and_test(int(0.66 * trn_size), int(0.34 * trn_size))
    return result


def experiment_2_4():
    result = train_and_test(int(0.66 * trn_size), 0)
    return result


if __name__ == '__main__':
    begin_experiments()

