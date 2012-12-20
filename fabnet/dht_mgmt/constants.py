
MIN_HASH = 0L
MAX_HASH = pow(2, 160) - 1

DS_INITIALIZE = 'init'
DS_NORMALWORK = 'normwork'
DS_DESTROYING = 'destroying'

RC_NEED_UPDATE = 101
RC_NO_DATA = 324
RC_OLD_DATA = 325
RC_INVALID_DATA = 326
RC_NO_FREE_SPACE = 327

MIN_REPLICA_COUNT = 2


DEFAULT_DHT_CONFIG = { 'WAIT_RANGE_TIMEOUT': 120, #if no ranges found for init DHT node, wait this timeout (in seconds)
                        'DHT_CYCLE_TRY_COUNT': 3, #waiting WAIT_RANGE_TIMEOUT*DHT_CYCLE_TRY_COUNT seconds before node crash
                        'INIT_DHT_WAIT_NEIGHBOUR_TIMEOUT': 1,
                        'ALLOW_USED_SIZE_PERCENTS': 70, #
                        'DANGER_USED_SIZE_PERCENTS': 80, #send notification in this case
                        'MAX_USED_SIZE_PERCENTS': 90,
                        'PULL_SUBRANGE_SIZE_PERC': 15,
                        'CRITICAL_FREE_SPACE_PERCENT': 3,
                        'CHECK_HASH_TABLE_TIMEOUT': 60,
                        'MONITOR_DHT_RANGES_TIMEOUT': 30,
                        'WAIT_FILE_MD_TIMEDELTA': 10,
                        'WAIT_DHT_TABLE_UPDATE': 3,
                        'RANGES_TABLE_FLAPPING_TIMEOUT': 3,
                        'DHT_STOP_TIMEOUT': 2} #wait sending messages from agents threads


