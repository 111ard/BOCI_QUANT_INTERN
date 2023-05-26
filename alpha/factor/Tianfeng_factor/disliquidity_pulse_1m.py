from rqfactor import *
import rqdatac


def main():
    return MA(ABS(PCT_CHANGE(Factor('close'), 1))/Factor('volume'),20)