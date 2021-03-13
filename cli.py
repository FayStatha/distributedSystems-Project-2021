import requests, random, re, sys, click


def random_select():
	r = requests.post('http://127.0.0.1:5000/overlay')
	data = '['+r.text.split('[')[1]
	my_list = re.findall("127.{11}", data)

	return random.choice(my_list)

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

    if kwargs['node'] != None:
    	ip = kwargs['node']
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/insert', data = { 'key':key, 'value':value })
    print(r.text)
    
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

    if kwargs['node'] != None:
    	ip = kwargs['node']
    else:
    	ip = random_select()
    r = requests.post('http://'+ip+'/query', data = { 'key':key })
    print(r.text)

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

if __name__ == '__main__':
    args = sys.argv
    if "--help" in args or len(args) == 1:
        print("You need to provide a command!")
    main()