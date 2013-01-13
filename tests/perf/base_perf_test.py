from primitives import *

def to_str(val):
    if type(val) == timedelta:
        return str(val.total_seconds())
    return str(val)

def check_errors(errors_queue):
    time.sleep(1)
    err_msg = ''
    while True:
        try:
            err_msg += '%s\n'%errors_queue.get(False)
        except Empty:
            break

    if err_msg:
        raise Exception(err_msg)

def collect_perf_stat(nodes_list):
    time.sleep(20)
    stat = collect_nodes_stat(nodes_list, reset=True)
    put_data_block_time = put_client_time = get_keys_info_time = get_data_block_time = timedelta()

    for node_stat in stat.values():
        put_data_block_time += to_dt(node_stat['OperationsProcTime']['PutDataBlock'])
        get_data_block_time += to_dt(node_stat['OperationsProcTime']['GetDataBlock'])
        put_client_time += to_dt(node_stat['OperationsProcTime']['ClientPutData'])
        get_keys_info_time += to_dt(node_stat['OperationsProcTime']['GetKeysInfo'])

    return {'put_data_block_time': put_data_block_time/len(stat), \
                'put_client_time': put_client_time/len(stat), \
                'get_keys_info_time': get_keys_info_time/len(stat), \
                'get_data_block_time': get_data_block_time/len(stat)}



def no_parallel_scenario(nodes_list, block_size, blocks_count):
    print '=> sequential scenario with blocksize=%s and blocks_count=%s ...'%(block_size, blocks_count)
    memmon = MemoryMonitor()

    keys_queue = Queue()
    errors_queue = Queue()

    memmon.start(nodes_list)
    try:
        put_dt = put_data(nodes_list, keys_queue, errors_queue, block_size, blocks_count)
        check_errors(errors_queue)
        get_dt = get_data(nodes_list, keys_queue, errors_queue)
        check_errors(errors_queue)
    finally:
        ret = memmon.stop()
        min_mem, max_mem = ret

    stat = collect_perf_stat(nodes_list)

    return put_dt, get_dt, stat['put_data_block_time'], stat['get_data_block_time'], stat['put_client_time'], stat['get_keys_info_time'], min_mem, max_mem


def parallel_scenario(threads_count, nodes_list, block_size, blocks_count):
    print '=> parallel scenario with %s threads (blocksize=%s, blocks_count=%s) ...'%(threads_count, block_size, blocks_count)
    memmon = MemoryMonitor()

    keys_queue = Queue()
    errors_queue = Queue()

    memmon.start(nodes_list)
    try:
        put_dt, th_avg_put_dt = parallel_put_data(threads_count, nodes_list, keys_queue, errors_queue, block_size, blocks_count)
        check_errors(errors_queue)
        get_dt, th_avg_get_dt = parallel_get_data(threads_count, nodes_list, keys_queue, errors_queue)
        check_errors(errors_queue)
    finally:
        min_mem, max_mem = memmon.stop()

    stat = collect_perf_stat(nodes_list)

    return th_avg_put_dt, th_avg_get_dt, stat['put_data_block_time'], stat['get_data_block_time'], stat['put_client_time'], stat['get_keys_info_time'], min_mem, max_mem


def test_scenario(nodes_list):
    stat_data = []
    MB = 1024*1024

    stat_data.append(['nopar_1mb_1000']+ list(no_parallel_scenario(nodes_list, 1*MB, 1000)))
    stat_data.append(['par_2_1mb_1000']+ list(parallel_scenario(2, nodes_list, 1*MB, 1000)))
    stat_data.append(['par_4_1mb_1000']+ list(parallel_scenario(4, nodes_list, 1*MB, 1000)))

    stat_data.append(['nopar_10mb_100']+ list(no_parallel_scenario(nodes_list, 10*MB, 100)))
    stat_data.append(['par_2_10mb_100']+ list(parallel_scenario(2, nodes_list, 10*MB, 100)))
    stat_data.append(['par_4_10mb_100']+ list(parallel_scenario(4, nodes_list, 10*MB, 100)))

    stat_data.append(['nopar_100mb_10']+ list(no_parallel_scenario(nodes_list, 100*MB, 10)))
    stat_data.append(['par_2_100mb_10']+ list(parallel_scenario(2, nodes_list, 100*MB, 10)))
    stat_data.append(['par_4_100mb_10']+ list(parallel_scenario(4, nodes_list, 100*MB, 10)))

    return stat_data

def more_parallel_test(nodes_list):
    stat_data = []
    MB = 1024*1024

    stat_data.append(['nopar_1mb_1000']+ list(no_parallel_scenario(nodes_list, 1*MB, 1000)))
    stat_data.append(['par_2_1mb_1000']+ list(parallel_scenario(2, nodes_list, 1*MB, 1000)))
    stat_data.append(['par_4_1mb_1000']+ list(parallel_scenario(4, nodes_list, 1*MB, 1000)))
    stat_data.append(['par_8_1mb_1000']+ list(parallel_scenario(8, nodes_list, 1*MB, 1000)))
    stat_data.append(['par_16_1mb_1000']+ list(parallel_scenario(16, nodes_list, 1*MB, 1000)))
    stat_data.append(['par_32_1mb_1000']+ list(parallel_scenario(32, nodes_list, 1*MB, 1000)))

    return stat_data


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print 'usage: %s <nodeaddr1>[,<nodeaddr2>...] <path to output file>'%sys.argv[0]
        sys.exit(1)
    t0 = datetime.now()
    fout = None
    TEST = more_parallel_test
    #TEST = test_scenario
    try:
        nodes_list = [n.strip() for n in sys.argv[1].split(',')]
        fout = open(sys.argv[2], 'w')

        stat_data = TEST(nodes_list)

        fout.write('test_name, put_overal_time, get_overal_time, put_data_block_time, get_data_block_time, put_client_time, get_keys_info_time, fabnet_node_mem_min, fabnet_node_mem_max\n')
        for item in stat_data:
            fout.write(', '.join([to_str(i) for i in item])+'\n')

        print 'Done!'
        print 'Results saved to %s'%sys.argv[2]
    except Exception, err:
        print 'ERROR! %s'%err
        sys.exit(1)
    finally:
        if fout:
            fout.close()
        print 'Processing time: %s'%(datetime.now()-t0)

