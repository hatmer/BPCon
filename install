#!/bin/bash

# 1. install python 3.4 (from https://www.python.org/downloads/release/python-344/)

# linux tarball

wget https://www.python.org/ftp/python/3.4.4/Python-3.4.4.tgz
fname="Python-3.4.4.tgz"
md5expected="e80a0c1c71763ff6b5a81f8cc9bb3d50"
md5=($(md5sum $fname))

if [ $md5=$md5expected ]; then
  echo "md5sum verified!"
  echo "installing..."
  # TODO for more than just ubuntu
  sudo apt-get install libssl-dev
  tar xzvf Python-3.4.4.tgz
  cd Python-3.4.4
  ./configure
  make

  # TODO possibly unneccessary
  sudo Python-3.4.4/python -m ensurepip

  # create virtual environment
  cd ..
  Python-3.4.4/python -m venv virtualenv
  source virtualenv/bin/activate

  # TODO fetch Congregate via git or pip first
  pip install pycrypto asyncio websockets sortedcontainers

  echo "done!"

  #TODO daemonize

else
  echo "md5sum verification failed"
fi

# TODO windows, mac, etc.

