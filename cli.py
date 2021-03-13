import requests, random, re, sys, click


def random_select():
	r = requests.post('http://127.0.0.1:5000/overlay')
	data = '['+r.text.split('[')[1]
	my_list = re.findall("127.{11}", data)

	return random.choice(my_list)

def my_insert(key, value, node = None):
    if node != None:
    	ip = node
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/insert', data = { 'key':key, 'value':value })
    print(r.text)	

def my_query(key, node = None):
    if node != None:
    	ip = node
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/query', data = { 'key':key })
    print(r.text)		

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

    my_insert(key, value, node)

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
    	ip = random_select()
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

    my_query(key, node)
    '''
    if kwargs['node'] != None:
    	ip = kwargs['node']
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/query', data = { 'key':key })
    print(r.text)
	'''
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
@click.argument('node', required = True)
def join(**kwargs):
    """Join node with given ip to Chord"""
    ip = kwargs['node']
    r = requests.post('http://'+ip+"/join")
    print(r.text)
    pass

@main.command()
@click.argument('node', required = False)
def overlay(**kwargs):
    """Returns Chord topology"""

    if kwargs['node'] != None:
    	ip = kwargs['node']
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/overlay')
    print(r.text)

    pass

@main.command()
@click.argument('file_path', required = True)
@click.option('--request_type',
              type=click.Choice(['insert', 'query', 'mix'], case_sensitive=False))
def file(**kwargs):
	"""Send requests with input from a file"""

	# Using readlines()
	file = kwargs['file_path']
	file1 = open(file, 'r')
	Lines = file1.readlines()

	if kwargs['request_type'] == 'insert':
		
		# Strips the newline character
		for line in Lines:
		    line_list = line.strip().split(',')
		    key = line_list[0]
		    value = line_list[1]
		    my_insert(key, value)

	elif kwargs['request_type'] == 'query':
		
		# Strips the newline character
		for line in Lines:
		    line_list = line.strip().split(',')
		    key = line_list[0]
		    my_query(key)

	elif kwargs['request_type'] == 'mix':

		for line in Lines:
		    line_list = line.strip().split(',')
		    req_type = line_list[0]
		    key = line_list[1]

		    if req_type == 'insert':

		    	value = line_list[2]
		    	my_insert(key, value)

		    elif req_type == 'query':

		    	my_query(key)		



if __name__ == '__main__':
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("You need to provide a command!")
    main()