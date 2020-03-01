#
# Parse the rsyncd logs, sup up the operations
#
from __future__ import print_function

import datetime
import sys


# :returns: Timestamp in UNIX seconds (a float, as usual in Python)
def log_stamp_to_timestamp(datestr, timestr):
    # 2020/02/14 20:23:06
    y_m_d = datestr.split('/')
    h_m_s = timestr.split(':')
    try:
        year = int(y_m_d[0])
        month = int(y_m_d[1])
        day = int(y_m_d[2])
        hour = int(h_m_s[0])
        minute = int(h_m_s[1])
        second = int(h_m_s[2])
    except ValueError:
        return None

    # Strictly speaking, rsyncd writes timestamps in its local time zone,
    # so using UTC here is incorrect. However, we only use timestamps for
    # finding transfer durations, so let's ingnore this issue.
    dt = datetime.datetime(year, month, day,
                           hour=hour, minute=minute, second=second,
                           tzinfo = datetime.timezone.utc)
    return dt.timestamp()


def main(argv):
    inp = sys.stdin

    # Per one originating host
    xfers = list()

    pids = dict()
    syntax_error_count = 0
    oppdir_error_count = 0

    for line in inp:
        words = line.split()

        timestamp = log_stamp_to_timestamp(words[0], words[1])
        if timestamp is None:
            syntax_error_count += 1
            continue

        pidstr = words[2]
        if not pidstr.startswith('[') or not pidstr.endswith(']'):
            syntax_error_count += 1
            continue
        try:
            pid = int(pidstr.lstrip('[').rstrip(']'))
        except ValueError:
            syntax_error_count += 1
            continue

        if words[3] == 'connect':
            # "connect from zssg-obsst006-st (172.24.65.144)"
            host = words[5]
            pids[pid] = {'start': timestamp, 'from': None, 'remote': host}
        elif words[3] == 'rsync':
            # Checking in case "connect" log entries are lost
            if pid in pids:
                # Swift never uses the other direction, but let's check.
                if words[4] == 'to':
                    pids[pid]['from'] = True
                else:
                    oppdir_error_count += 1
        elif words[3] == 'sent':
            # "sent 259 bytes  received 31241 bytes  total size 18192091"
            if pid in pids:
                if words[6] != 'received':
                    syntax_error_count += 1
                    continue
                xfer = pids[pid]
                if xfer['from'] != True:
                    continue
                bytecnt = int(words[7])
                xfer['length'] = bytecnt
                xfer['end'] = timestamp
                xfers.append(xfer)
                # No point in wasting memory, and get ready for PID reuse.
                del pids[pid]
        else:
            pass

    print("Errors: syntax %d direction %d" %
          (syntax_error_count, oppdir_error_count))

    zero_dur_cnt = 0
    hosts = dict()
    for xfer in xfers:

        hostname = xfer['remote']
        if hostname in hosts:
            hostp = hosts[hostname]
        else:
            hostp = { 'bps_sum_total': 0, 'bps_cnt_total': 0 }
            hosts[hostname] = hostp

        if xfer['end'] <= xfer['start']:
            zero_dur_cnt += 1
        else:
            bps = xfer['length'] / (xfer['end'] - xfer['start'])
            hostp['bps_sum_total'] += bps
            hostp['bps_cnt_total'] += 1

    print("Transfers: total %d incomplete %d notime %d" %
          (len(xfers), len(pids), zero_dur_cnt))

    print("Transfers host avg.bps:")
    for hostname in sorted(list(hosts.keys())):
        hostp = hosts[hostname]
        if hostp['bps_cnt_total'] != 0:
            x = hostp['bps_sum_total']/float(hostp['bps_cnt_total'])
            print(" %s %.2f" % (hostname, x))

    return 0

sys.exit(main(sys.argv))
