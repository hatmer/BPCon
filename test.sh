#!/bin/bash

#killall xterm
cd ..
rm -rf demoCopy/*
cp -r Congregate/* demoCopy/

cd demoCopy
python run.py demo_config.ini
#sleep 1
#cd ../Congregate
#xterm -hold -e 'python run.py' &
