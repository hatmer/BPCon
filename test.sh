#!/bin/bash

xterm -hold -e 'python congregate.py 3_config.ini' &
xterm -hold -e 'python congregate.py 2_config.ini' &
#xterm -hold -e 'python congregate.py 1_config.ini' &
