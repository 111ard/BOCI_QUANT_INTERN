from rqfactor import *


def main():
    return SUM(PCT_CHANGE(Factor('close'), 1), 20)
