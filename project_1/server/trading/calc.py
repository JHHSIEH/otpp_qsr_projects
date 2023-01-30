def signal(p, avg, std, row):
    try:
        if p > (avg + std):
            return 1
        elif p < (avg - std):
            return -1
        return 0
    except Exception as e:
        raise Exception(f'Error with row: {row}')


def pnl(pos_1, p, p_1):
    return pos_1 * (p - p_1)

