import requests, random

# random select, selects randomly an online node to send the request
# set the ip to the Bootstrap's ip
boot_ip='127.0.0.1:5000'

def exec_requests(requests):
    for r in requests:
        if r[0]=='insert':
            insert(r[1],r[2],r[3])
        elif r[0]=='query':
           query(r[1],r[2])
    return

def random_select():
    r = requests.post('http://'+boot_ip+'/overlay')
    nodes_list = r.json()['topology']
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
        for node in r.json()['result']:
            print(node, "\n")
    else:
        print(r.json()['result'])

