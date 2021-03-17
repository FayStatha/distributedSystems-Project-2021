# ToyChord

A project for Distributed Systems course at NTUA, ECE, academic year 2020-2021.

We implemented ToyChord, a simple version of Chord, as discribed in [Stoica, Ion, et al. "Chord: A scalable peer-to-peer lookup service for internet applications." ACM
SIGCOMM Computer Communication Review 31.4 (2001): 149-160).](https://dl.acm.org/doi/10.1145/964723.383071).

Our application is a file sharing application with a lot distributed nodes DHT. Every node can interact with Chord either as a server, or as a client. The nodes keep information in key-value pairs, where the key is the title of each song saved in the Chord and the value is the node that has this song.

Project's assignement and report are written in greek, because this is the main language of this course.

## Team Members

| Full Name - Github Account                                     | Email                   |
|----------------------------------------------------------------|-------------------------|
| [Dimitris Tolias](https://github.com/ToliasDimitris)           | email                   |
| [Efstathia Statha](https://github.com/FayStatha)               | fay.statha@gmail.com    |
| [Eleni Oikonomou](https://github.com/EleniOik)                 | elecon16@gmail.com      |


## Installation Process

### Clone repo

```git clone git@github.com:FayStatha/distributedSystems-Project-2021.git```

### Install python3.8

```sudo apt install python3.8```

### Install pip3

```sudo apt-get install python3-pip```

### Install python3-venv

```sudo apt-get install python3-venv```

### Create virtual environment

```python3 -m venv env```

### Activate virtual environment

```source env/bin/activate```

### Install packages

```pip3 install -r requirements.txt```


## Usage

### Fire up Bootstrap Node

```python3 Bootstrap_node.py bootstrap_ip bootstrap_ip number_of_replicas type_of_consistency```

For example:

```python 3 Bootstrap_node.py server1:5000 server1:5000 3 eventual```

Will fire up bootstrap node in host server1 and port 5000. Chord will keep 3 replicas and the consistency type will be eventual.

### Fire up Nodes

```python3 Normal_node.py node_ip bootstrap_ip```

For example:

```python3 Normal_node.py server2:5000 server1:5000```

Will fire up a normal node in host server2 port 5000, that communicates with boostrap in host server1 port 5000.

### Cli usage

```python3 cli.py --help```

This will provide a description of all supported commands by our cli.

Note: It is important for all nodes to work with ip names such as serverX, so that random selecting of nodes when no node given can work right!

MHPWS NA FTIAKSW ENA ARXEIAKI POU NA EKSHGEI TH LEITOURGIA TOU CLI??

----------------------------------------------------------------------------------

isws ta vgalw teleiws auta an se ola ta ubuntu 20.04 paizoun ta panw

### Alternative

If you are getting those errors:

E: Unable to locate package Python-3.8.0
E: Couldn't find any package by glob 'Python-3.8.0'
E: Couldn't find any package by regex 'Python-3.8.0'

Then maybe the following commands will help:

```sudo add-apt-repository ppa:deadsnakes/ppa```   

```sudo apt-get update```   

```sudo apt install python3.8```

```sudo apt install python3.8-distutils```

```wget https://bootstrap.pypa.io/get-pip.py```

```sudo python3.8 get-pip.py```

```pip3 install -r requirements.txt```