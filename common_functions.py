import requests, random

# random select, selects randomly an online node to send the request
# set the ip to the Bootstrap's ip

def random_select():
    r = requests.post('http://127.0.0.1:5000/overlay')
    nodes_list = r.json()
    ip_list = []

    for node in nodes_list:
        temp_ip = (node['node_ip_port'])
        ip_list.append(temp_ip)

    return random.choice(ip_list)

def insert(key, value, node = None):
    if node != None:
    	ip = node
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/insert', data = { 'key':key, 'value':value })
    print(r.text)	

def query(key, node = None):
    if node != None:
    	ip = node
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/query', data = { 'key':key })

    if (key == '*'):
        print("Those are all key-value pairs in Chord:\n")
        for node in r.json():
            print(node, "\n")
    else:
        print(r.text)

def checknodes():
    r = requests.post('http://127.0.0.1:5000/checknodes')
    return r.text