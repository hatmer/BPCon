#!/bin/bash

killall xterm
cd ..
rm -rf demoCopy/*
cp -r congregate/* demoCopy/

cd congregate
xterm -fa 'Monospace' -fs 14 -hold -e 'python run.py' &
sleep 1
cd ../demoCopy
xterm -fa 'Monospace' -fs 14 -hold -e 'python run.py demo_config.ini' &
