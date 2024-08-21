#Author: BIPIN KHANAL
#In this example w will see the example for Python argument parser

import argparse

def argumentParser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--slowbros', help = 'Names of Slowbros', action = 'store_true')
    arg = parser.parse_args()
    if(arg.slowbros):
        slowBros()
    else:
        print('Dude give some arguments! Type ArgumentParser -h for more details')


def slowBros():
    print('SLOWBROS MEMBERS: \nBipin Khanal\nHimal Giri\nAnkit Poudel\nS S')


if __name__ == '__main__':
    argumentParser()
