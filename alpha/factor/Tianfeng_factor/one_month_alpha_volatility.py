from rqfactor import *
import math


def main():
    return STD(PCT_CHANGE(Factor('close'), 1) - FIX(PCT_CHANGE(Factor('close'), 1), '000905.XSHG'), 20)*math.sqrt(252)
