# Evolution

A project for Distributed Systems course at NTUA, ECE, academic year 2020-2021.

## Installation Process

### Clone repo

```git clone git@github.com:FayStatha/distributedSystems-Project-2021.git```

### Install python3.8

```sudo apt install python3```

### Install pip3

```sudo apt-get install python3-pip```

### Install python3-venv

```sudo apt-get install python3-venv```

### Create virtual environment

```python3 -m venv env```

### Activate virtual environment

```source env/bin/activate```

### Install packages

```pip install -r requirements.txt```

### Fire up Bootstrap Node

```python3 Bootstrap_node.py bootstrap_ip bootstrap_ip number_of_replicas type_of_consistency```

### Fire up Nodes

```python3 Bootstrap_node.py bootstrap_ip node_ip```
