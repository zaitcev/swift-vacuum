#
# Parse the log, sum up the PUT operations
#
from __future__ import print_function

import sys

IDLE = 0
BRACKET = 1
QUOTE = 2

def parse(words):
    ret = []
    state = IDLE
    save = None
    for word in words:
        if state == QUOTE:
            if word[-1] == '"':
                word = word[:-1]
                # Ouch. Multiple spaces inside a quote get consolidated.
                save = ' '.join((save, word))
                ret.append(save)
                state = IDLE
                save = None
            else:
                save = ' '.join((save, word))
        elif state == BRACKET:
            if word[-1] == ']':
                word = word[:-1]
                save = ' '.join((save, word))
                ret.append(save)
                state = IDLE
                save = None
            else:
                save = ' '.join((save, word))
        else: # state == IDLE:
            if word[0] == '"':
                word = word[1:]
                if word[-1] == '"':
                    word = word[:-1]
                    ret.append(word)
                else:
                    save = word
                    state = QUOTE
            elif word[0] == '[':
                word = word[1:]
                if word[-1] == ']':
                    word = word[:-1]
                    ret.append(word)
                else:
                    save = word[1:]
                    state = BRACKET
            else:
                ret.append(word)
    if state != IDLE:
        ret.append(save)
    return ret


def main(argv):
    inp = sys.stdin

    all_cnt = 0
    o_cnt = 0
    o_put_cnt = 0
    o_put_cnt_err = 0
    o_put_bytes = 0

    log_buckets = [0 for _ in range(10)]

    def drop_into_bucket(n):
        # My CS background is so lacking that I cannot compute a logarithm.
        log_top = 10
        for i in range(len(log_buckets)):
            if n < log_top:
                log_buckets[i] += 1
                return
            log_top *= 10
        log_buckets[-1] += 1
        return

    for line in inp:
        all_cnt += 1
        words = line.split()
        words = parse(words)
        if words[4].startswith('object-server'):
            o_cnt += 1
            if words[9].startswith('PUT'):
                o_put_cnt += 1
                try:
                    status = int(words[10])
                except ValueError:
                    status = 999
                if status == 200 or status == 201:
                    try:
                        size = int(words[17])
                    except ValueError:
                        size = 0 
                    o_put_bytes += size
                    drop_into_bucket(size)
                else:
                    o_put_cnt_err += 1

    print("Log entries total: %d" % (all_cnt,))
    print("Object server total: %d" % (o_cnt,))
    print("Object server PUT count: %d" % (o_put_cnt,))
    print("Object server PUT errors: %d" % (o_put_cnt_err,))
    print("Object server PUT bytes: %d" % (o_put_bytes,))
    log_top = 10
    for i in range(len(log_buckets)):
        print("Object server PUT bucket %12s: %10d" %
              ("<%d" % log_top, log_buckets[i]))
        log_top *= 10

    return 0


sys.exit(main(sys.argv))
