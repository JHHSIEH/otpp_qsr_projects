

# def calc(S: Price_Series):
#     For all t
#         If S(t) > (S_avg(t) + Sigma(t)):
#             Define the position as Pos(t + 1) = +1
#             i.e., buy 1 stock at time t + 1 min
#         Else if S(t) < (S_avg(t) - Sigma(t)):
#             Pos(t + 1) = -1
#             i.e., sell 1 stock at time t + 1 min
#         Else:
#             do nothing
#             Pos(t + 1) = Pos(t)


def test(name: str):
    return name.upper()
