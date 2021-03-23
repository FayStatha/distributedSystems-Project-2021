import requests, random, sys, click, time
import common_functions
from threading import Thread

boot_ip='127.0.0.1:5000'

@click.group()
def main():
    """A CLI for Chord users!"""
    pass

@main.command()
@click.option('--key', required = True, help = "The name of the song to insert")
@click.option('--value', required = True, help = "Node that has this song")
@click.argument('node', required = False)
def insert(**kwargs):
    """Insert given key-value pair in Chord!"""

    key = kwargs['key']
    value = kwargs['value']
    node = kwargs['node']
    common_functions.insert(key, value, node)
    pass

@main.command()
@click.option('--key', required = True, help = "The name of the song to delete")
@click.argument('node', required = False)
def delete(**kwargs):
    """Deletes key-value pair for given key"""

    key = kwargs['key']
    if kwargs['node'] != None:
        ip = kwargs['node']
    else:
        ip = common_functions.random_select()
    r = requests.post('http://'+ip+'/delete', data = { 'key':key })
    print(r.text)

    pass

@main.command()
@click.option('--key', required = True, help = "The name of the song to find, if special character '*' is given it returns all key-value pairs in Chord")
@click.argument('node', required = False)
def query(**kwargs):
    """Find the key-value pair for given key"""

    key = kwargs['key']
    node = kwargs['node']
    common_functions.query(key, node)

    pass

@main.command()
@click.argument('node', required = True)
def depart(**kwargs):
    """Departs node with given ip from Chord"""

    ip = kwargs['node']
    r = requests.post('http://'+ip+"/depart")
    print(r.text)
    pass

@main.command()
@click.argument('node', required = False)
def overlay(**kwargs):
    """Returns Chord topology"""

    if kwargs['node'] != None:
    	ip = kwargs['node']
    else:
    	ip = common_functions.random_select()

    r = requests.post('http://'+ip+'/overlay')
    print("This is the Chord topology: \n"+r.text)

    pass

@main.command()
@click.argument('node', required = True)
def join(**kwargs):
    """Join node with given ip to Chord"""
    ip = kwargs['node']
    r = requests.post('http://'+ip+"/join")
    print(r.text)
    pass

@main.command()
@click.argument('file_path', required = True)
@click.option('--request_type', type = click.Choice(['insert', 'query', 'mix'], case_sensitive = False))
def file(**kwargs):
    """Send requests with input from a file"""

    count = 0

    file = kwargs['file_path']
    file1 = open(file, 'r')
    Lines = file1.readlines()

    if kwargs['request_type'] == 'insert':

        start = time.time()

        for line in Lines:
            count += 1
            line_list = line.strip().split(',')
            key = line_list[0]
            value = line_list[1]
            common_functions.insert(key, value)

        end = time.time()

    elif kwargs['request_type'] == 'query':

        start = time.time()

        for line in Lines:
            count += 1
            line_list = line.strip().split(',')
            key = line_list[0]
            common_functions.query(key)

        end = time.time()

    elif kwargs['request_type'] == 'mix':

        start = time.time()

        for line in Lines:
            count += 1
            line_list = line.strip().split(',')
            req_type = line_list[0]
            key = line_list[1]

            if req_type == 'insert':

                value = line_list[2]
                common_functions.insert(key, value)

            elif req_type == 'query':

                common_functions.query(key)
        end = time.time()

    throughput = count/(end-start)

    print("Throuput of Chord = %.4f requests/second"%throughput, "%.4f seconds per query"%(1/throughput))



@main.command()
@click.argument('file_path', required = True)
@click.option('--request_type', type = click.Choice(['insert', 'query', 'mix'], case_sensitive = False))
def fileparallel(**kwargs):
    """Send requests with input from a file using one thread for each node"""

    #get topology ip's
    r = requests.post('http://' + boot_ip + '/overlay')
    nodes_list = r.json()['topology']
    ip_list = []
    for node in nodes_list:
        temp_ip = (node['node_ip_port'])
        ip_list.append(temp_ip)
    #create requests dicts
    requests_dicts={}
    threads_list=[]
    for ip in ip_list:
        requests_dicts[ip]=[]

    count = 0
    file = kwargs['file_path']
    file1 = open(file, 'r')
    Lines = file1.readlines()

    if kwargs['request_type'] == 'insert':
        for line in Lines:
            count += 1
            line_list = line.strip().split(',')
            key = line_list[0]
            value = line_list[1]
            random_ip=random.choice(ip_list)
            requests_dicts[random_ip].append( ('insert',key,value,random_ip) )
    elif kwargs['request_type'] == 'query':
        for line in Lines:
            count += 1
            line_list = line.strip().split(',')
            key = line_list[0]
            random_ip = random.choice(ip_list)
            requests_dicts[random_ip].append(('query',key, random_ip))
    elif kwargs['request_type'] == 'mix':
        for line in Lines:
            count += 1
            line_list = line.strip().split(',')
            req_type = line_list[0]
            key = line_list[1]
            random_ip = random.choice(ip_list)
            if req_type == 'insert':
                value = line_list[2]
                requests_dicts[random_ip].append(('insert',key,value, random_ip))
            elif req_type == 'query':
                requests_dicts[random_ip].append(('query', key, random_ip))

    start = time.time()
    for ip in ip_list:
        thread = Thread(target=common_functions.exec_requests, kwargs={'requests': requests_dicts[ip]})
        threads_list.append(thread)
        thread.start()
    for t in threads_list:
        t.join()
    end = time.time()
    throughput = count/(end-start)
    print("Throuput of Chord = %.4f requests/second"%throughput, "%.4f seconds per query"%(1/throughput))



if __name__ == '__main__':
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("You need to provide a command!")
    main()

