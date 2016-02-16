#!/bin/bash

xterm -hold -e 'python server.py 3_config.ini' &
xterm -hold -e 'python server.py 2_config.ini' &
xterm -hold -e 'python server.py 4_config.ini' &
python client.py 1_config.ini
