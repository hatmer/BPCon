# Python-Congregate

Scalable, consistent, and churn-adaptive datastore build according to the architecture specified in [*Scalable Consistency in Scatter*](http://homes.cs.washington.edu/~arvind/papers/scatter.pdf). 

Replication using a byzantined Paxos variant, [Python-BPCon](https://github.com/hatmer/python-bpcon) 

## Requirements
* python3.4
* twisted
* asyncio
* websockets
* pycrypto
* sortedcontainers

make sure to add these to your PYTHONPATH
