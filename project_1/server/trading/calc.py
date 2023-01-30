

# For all t
#     If S(t) > (S_avg(t) + Sigma(t))
#         Define the position as Pos(t + 1) = +1
#         i.e., buy 1 stock at time t + 1 min
#     Else if S(t) < (S_avg(t) - Sigma(t))
#         Pos(t + 1) = -1
#         i.e., sell 1 stock at time t + 1 min
#     Else:
#         do nothing
#         Pos(t + 1) = Pos(t)

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


def test(name: str):
    return name.upper()
