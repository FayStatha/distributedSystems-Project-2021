import time

import flask
from flask import Flask, render_template, request, redirect, url_for
import sys
import requests
import json
from node import node
from threading import Thread
import hashlib

app = Flask(__name__)
ip_port=sys.argv[1]
boot_ip_port=sys.argv[2]

#THIS IS HOST AND PORT FOR A NORMAL NODE
host_port=ip_port.split(":")
host=host_port[0]
port=host_port[1]
# construct node
node=node(ip_port,boot_ip_port)

responses_dict={}
#istoriko twn request kai responses gia ta antistoixa seqn
requests_list=[]
responses_list=[]
seqn=0


def make_req(req_type,data,req_code):
    req= { 'source':node.ip_port , 'type':req_type,  'data':data , 'seqn':(str(req_code)) }
    return req

def make_resp(receiver,resp_type,data,req_code):
    resp={'receiver':receiver,  'type':resp_type, 'data':data, 'seqn':(str(req_code))}
    return resp

def make_same_req(source, req_type,data,req_code):
    req= { 'source':source , 'type':req_type,  'data':data , 'seqn':(str(req_code)) }
    return req

def post_req_to(ip_port, req):
    url="http://"+ip_port+"/ntwreq"
    #debugging
    #debug("I LL POST THIS NTW REQUEST:"+json.dumps(req))
    requests.post(url,json=req)

def post_req_thread(ip_port,req):
    thread = Thread(target=post_req_to, kwargs={'ip_port': ip_port, 'req': req})
    thread.start()

def post_resp_to(ip_port, resp):
    url="http://"+ip_port+"/ntwresp"
    # debugging
    #debug("I LL POST THIS NTW  RESPONSE:" + json.dumps(resp))
    requests.post(url,json=resp)

def post_resp_thread(ip_port,resp):
    thread = Thread(target=post_resp_to, kwargs={'ip_port': ip_port, 'resp': resp})
    thread.start()

def hash(text):
    hash_object = hashlib.sha1(text.encode())
    hash_code = hash_object.hexdigest()
    return hash_code

def handle_response(resp, **kwargs):
    receiver=resp['receiver']
    resp_type=resp['type']
    data=resp['data']
    seqn=resp['seqn']
    # check if i am the correct receiver
    if(node.ip_port== receiver):
        if resp_type=='insert':
            msg=data['resp_text']
            c = kwargs.get('unhashed_key', None)
            msg = 'Key:' + c + ' ' + msg
        elif resp_type== 'query':
            msg=data['resp_text']
            c = kwargs.get('unhashed_key', None)
            msg = 'Key:' + c + ' ' + msg
        elif resp_type== 'delete':
            msg=data['resp_text']
            c = kwargs.get('unhashed_key', None)
            msg = 'Key:' + c + ' ' + msg
        elif resp_type== 'depart':
            msg=data['resp_text']
        elif resp_type == 'overlay':
            topology = data['topology']
            msg = topology
        elif resp_type == 'query_all':
            pairs = data['key-value pairs']
            msg =pairs
    return msg

@app.route('/', methods=['POST', 'GET'])
def func1():
    if request.method == 'GET':

        return "Hello Normal node guest\n"
    if request.method == 'POST':
        req_dict = request.form
        resp_text = f"Key={req_dict['key']}, Value={req_dict['value']}\n"
        #print(resp_text)
        return resp_text

@app.route('/insert', methods=['POST'])
def insert():
    if request.method == 'POST':
        global seqn
        seqn = seqn + 1
        req_code = str(seqn)

        request_dict = request.form

        # hash the key before doing anything with it
        key = hash(request_dict['key'])
        value = request_dict['value']
        #insert data
        data = {'key': key, 'value': value, "resp_ip_port":"None" , 'index': 0}
        req = make_req('insert', data, req_code)

        # η post req thread ανοιγει ενα thread
        post_req_thread(node.ip_port, req)
        while responses_dict.get(req_code, "None") == "None":
            {}
        #       pop response from dict and handle it
        resp = responses_dict.pop(req_code)

        return handle_response(resp, unhashed_key=request_dict['key'])

@app.route('/query', methods=['POST'])
def query():
    if request.method == 'POST':
        global seqn
        seqn = seqn + 1
        req_code = str(seqn)
        request_dict = request.form


        if request_dict['key'] != '*':

            key = hash(request_dict['key'])
            data = {'key': key, 'value': "None", "resp_ip_port": "None", 'index': 0,'failed_to_find':[False]}
            req = make_req('query', data, req_code)
            post_req_thread(node.ip_port, req)
        else:
            data = {'key-value pairs': []}
            req = make_req('query_all', data, req_code)
            post_req_thread(node.succ_ip_port, req)

        while responses_dict.get(req_code, "None") == "None":
            {}
        #       pop response from dict and handle it
        resp = responses_dict.pop(req_code)
        msg=handle_response(resp,unhashed_key=request_dict['key'])
        response = flask.jsonify(result=msg)
        return response

@app.route('/delete', methods=['POST'])
def delete():
    if request.method == 'POST':
        global seqn
        seqn = seqn + 1
        req_code = str(seqn)

        request_dict = request.form

        key = hash(request_dict['key'])
        data = {'key': key, 'resp_ip_port': "None", 'index': 0}
        req = make_req('delete', data, req_code)
        post_req_thread(node.ip_port, req)
        while responses_dict.get(req_code, "None") == "None":
            {}
        #       pop response from dict and handle it
        resp = responses_dict.pop(req_code)
        return handle_response(resp,unhashed_key=request_dict['key'])

@app.route('/depart', methods=['POST'])
def depart():
    if request.method == 'POST':

        if not node.get_isInChord():
            return 'This node does not participate in Chord!\n'
        else:
            # Στις επόμενες 4 γραμμές, μαζεύω τα data που θα στείλω στον successor
            global seqn
            seqn = seqn + 1
            req_code = str(seqn)
            new_keys=node.keys_vals[0]
            last_keys=node.keys_vals[node.get_replicas()-1]
            #send message to boot to find out K_nodes value

            boot_resp=requests.post("http://"+node.boot_ip_port+"/ntwresp",json={'type':'K_nodes'})
            K_nodes=json.loads(boot_resp.text)['K_nodes']
            data={'index':0, 'new_keys':new_keys, 'last_keys':last_keys, 'K_nodes':K_nodes}

            if node.is_alone():
                node.init_state()
                return f"Node {node.ip_port} departed from the Chord!\n"
            else:
                #make depart request and post to next node--> it will update all the rm chain
                req = make_req('depart', data, req_code)
                post_req_thread(node.succ_ip_port, req)
                while responses_dict.get(req_code, "None") == "None":
                    {}
                responses_dict.pop(req_code)
                # then take care of the neighbour pointers
                # data = {'prev': node.prev_ip_port, 'succ': node.succ_ip_port, 'type':'set_neighboors'}
                prev=node.prev_ip_port
                succ=node.succ_ip_port
                if  node.is_duo():
                    # an exoume mono 2 nodes tote prepei na steiloume na enimerwsei kai ton next kai ton prev o enas mas geitonas
                    #to parakatw tha perimenei mexri na lavoume apantisi
                    post_resp_to(node.prev_ip_port, {'prev':prev, 'succ':succ, 'type':'set_neighboors'})
                else:
                    #an uparxoun toul 3 komvoi stelnoume kai ston prev kai ston next na enimerwsoun mono ton enan geitona tous ekastos
                    post_resp_to(node.prev_ip_port,{'prev':"None", 'succ':succ, 'type':'set_neighboors'})
                    post_resp_to(node.succ_ip_port, {'prev': prev, 'succ': "None", 'type': 'set_neighboors'})
                # Then return the node to dafault state
                node.init_state()
                post_resp_to(node.boot_ip_port, {'type': 'dec_number'})
            return f"Node {node.ip_port} successfully departed from Chord!\n"


@app.route('/overlay', methods=['POST'])
def overlay():
    if request.method == 'POST':
        global seqn
        seqn = seqn + 1
        req_code = str(seqn)

        data = {'topology': []}
        req = make_req('overlay', data, req_code)
        post_req_thread(node.succ_ip_port, req)
        while responses_dict.get(req_code, "None") == "None":
            {}
        #       pop response from dict and handle it
        resp = responses_dict.pop(req_code)
        msg = (handle_response(resp))
        response = flask.jsonify(topology=msg)
        response.headers.add('Access-Control-Allow-Origin', '*')

        return response

@app.route('/join',methods=['POST'])
def call_join():
    if node.get_isInChord():
        msg = 'This node is already in Chord!\n'
    else:

        msg=join()
    return msg

@app.route('/show_info', methods=['POST'])
def show_info():
    x = "\n" + node.return_node_stats()
    response = flask.jsonify(x)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

def join():
    # its important to stall join request so that our server has started properly and then we can get the boot server response
    global seqn
    seqn = seqn + 1
    req_code = str(seqn)
    # 1) kane join to node sto chord kai ftiakse tous pointers tou kai twn geitonwn tou kai dwstou kai ta kleidia tou epomenou same keys  new_keys
    # 2) gia tous epomenous k nodes ektelese new ntw_req --> ( source, type="join_upd_chain" , req_code, data { index:0 , same_keys, new_keys }    )
    # 3) pare ta keys_vals tou prevs stis theseis 0 ews k-1 kai valta stis dikes sou 1 ews k-1
    # 4) respond to user
    data = {'key': node.id}
    req = make_req('join', data, req_code)


    post_req_thread(node.boot_ip_port, req)
    while responses_dict.get(req_code, "None") == "None":
        {}
    # pop response from dict and handle it
    resp = responses_dict.pop(req_code)
    # apo to response pairnw ta same_keys, new_keys, kai ftiaxnw kai tous geitones mou
    same_keys = resp['data']['same_keys']
    new_keys = resp['data']['new_keys']
    node.prev_ip_port = resp['data']['prev']
    node.succ_ip_port = resp['data']['succ']
    # ta new_keys einai ta keys gia ta opoia twra tha eisai responsible
    node.keys_vals[0] = new_keys
    data = {'index': 0, 'same_keys': same_keys, 'new_keys': new_keys}
    req = make_req('join_upd_chain', data, req_code)
    # post request to next node so that all chain of replicas updates
    post_req_thread(node.succ_ip_port, req)
    while responses_dict.get(req_code, "None") == "None":
        {}
    responses_dict.pop(req_code)
    # get_keys request from prev
    req = make_req('get_keys', {}, req_code)
    post_req_thread(node.prev_ip_port, req)
    while responses_dict.get(req_code, "None") == "None":
        {}
    resp = responses_dict.pop(req_code)
    prev_keys = resp['data']['keys']

    post_resp_to(node.boot_ip_port, {'type': 'inc_number'})

    # vale ta keys 0--k-2 tou prev sta dika sou 1--k-1
    # if nodes_in_chord < k then go till nodes_in_chord -1 element
    nodes_in_chord= requests.post("http://" + node.boot_ip_port + "/ntwresp", json={'type':'nodes_in_chord'})
    nodes_in_chord = json.loads(nodes_in_chord.text)['nodes_in_chord']
    limit=min(nodes_in_chord,node.get_replicas())
    for i in range(limit - 1):
        node.keys_vals[i + 1] = prev_keys[i]

    msg = ("Node joined succesfully")

    return msg

@app.route('/ntwreq',methods=['POST'])
def ntwreq():
    req_dict = json.loads(request.data)
    source = req_dict['source']
    req_type = req_dict['type']
    # to data apotelei dict keys:values opou keys oi metavlites kai values oi times tous
    data = req_dict['data']
    req_code = req_dict['seqn']

    if (req_type == 'join'):
        dispatch_join(source, req_code, req_type, data)
    elif (req_type == 'depart'):
        dispatch_depart(source, req_code, req_type, data)
    elif (req_type == 'query'):
        dispatch_query(source, req_code, req_type, data)
    elif (req_type == 'insert'):
        dispatch_insert(source, req_code, req_type, data)
    elif (req_type == 'delete'):
        dispatch_delete(source, req_code, req_type, data)
    elif (req_type == 'query_all'):
        dispatch_query_all(source, req_code, req_type, data)
    elif (req_type == 'overlay'):
        dispatch_overlay(source, req_code, req_type, data)
    elif (req_type == 'get_keys'):
        dispatch_get_keys(source, req_code, req_type, data)
    elif (req_type == 'join_upd_chain'):
        dispatch_join_upd_chain(source, req_code, req_type, data)

    return {}

@app.route('/ntwresp',methods=['POST'])
def ntwresp():
    resp_dict = json.loads(request.data)
    if resp_dict['type'] == 'set_neighboors':
        prev_ip_port = resp_dict['prev']
        succ_ip_port = resp_dict['succ']

        node.set_neighboors(prev_ip_port, succ_ip_port)
    elif resp_dict['type']=='join_vars':
        repn = resp_dict['data']['repn']
        rep_type = resp_dict['data']['rep_type']
        node.join_set_vars(repn, rep_type)
    else:
        seqn = resp_dict['seqn']
        responses_dict[seqn] = resp_dict
    return {}


def dispatch_join(source, req_code, req_type, data):
    #if this node is prev ( this.succ== is_responsible ) update its next to source  put its ip to prev datum and forward request to next
    #if this node is_resp === is next   update its prev to source put its ip to succ datum and get same_keys , new_keys and respond to source
    #if nothing of the above just forward to next node

    if (is_responsible(data['key'])):


        data['succ']=node.ip_port
        same_new = node.get_same_new_keys(data['key'])
        data['same_keys'] = same_new[0]
        data['new_keys'] = same_new[1]
        if(node.is_alone()):
            # special case --> here we are previous also so we must fix those values too
            node.succ_ip_port=source
            node.prev_ip_port = source
            data['prev']=node.ip_port
        #respond to source
        node.prev_ip_port = source
        resp=make_resp(source,req_type,data,req_code)
        post_resp_thread(source,resp)

    elif (succ_is_responsible(data['key'])):
        temp=node.succ_ip_port
        node.succ_ip_port=source
        data['prev']=node.ip_port
        req=make_same_req(source,req_type,data,req_code)
        post_req_thread(temp,req)
    else:
        req=make_same_req(source,req_type,data,req_code)
        post_req_thread(node.succ_ip_port,req)
    return

def dispatch_depart(source, req_code, req_type, data):
    #update data  --> first node has to add his keys[0] to new_keys  , every node has to put his last keys to last_keys
    #if node isnt last forward next
    #else respond to source
    index=data['index']
    new_keys=data['new_keys']
    last_keys=data['last_keys']
    K_nodes=data['K_nodes']
    if (index==0):
        new_keys.update(node.keys_vals[index])
    node.keys_vals[index]=new_keys
    if (index+1<node.get_replicas()-1):
        node.pushup(index+1)
    if (index+1<=node.get_replicas()-1):
        #if there are  K nodes then we should put {} to last keys else do what we do
        if (K_nodes=='True'):
            node.keys_vals[node.get_replicas()-1]={}
        else:
            node.keys_vals[node.get_replicas()-1]=last_keys
    #check if last_node
    if (index==node.get_replicas()-1 or node.succ_ip_port==source):
        resp=make_resp(source,req_type,{},req_code)
        post_resp_thread(source,resp)
    else:
        new_data={'index':index+1, 'new_keys':new_keys, 'last_keys':node.keys_vals[node.get_replicas()-2], 'K_nodes':K_nodes}
        req=make_same_req(source,req_type,new_data,req_code)
        post_req_thread(node.succ_ip_port,req)
    return

def dispatch_query(source,req_code,req_type,data):
    #data = {'key': key, 'value': "None", "resp_ip_port": "None", 'index': 0}
    key=data['key']
    value=data['value']
    resp_ip_port=data['resp_ip_port']
    index=data['index']
    # for eventual consistency, if not found once we dont search for key again until it reaches resp, there is no point
    #make this an array of one element so i can change it later
    failed_to_find=data['failed_to_find']
    k=node.get_replicas()
    if is_responsible(key):
        resp_ip_port=node.ip_port
        value=node.query(key)
        if node.get_rep_type()=="eventual":
            if value!="None":
                resp_text = "has value:" + value + " found at node:" + node.ip_port
                resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                post_resp_thread(source, resp)
                return
        #if node isnt last forward request
        if index!=k-1 and node.succ_ip_port!=resp_ip_port:
            req=make_same_req(source,req_type,{'key':key, 'value':value, 'resp_ip_port':resp_ip_port, 'index':index+1, 'failed_to_find':failed_to_find},req_code)
            post_req_thread(node.succ_ip_port,req)
        else:
        #is last node respond to source
            if value=="None":
                resp_text = "not found at node " + node.ip_port
            else:
                resp_text = "has value:" + value + " found at node:" + node.ip_port
            resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
            post_resp_thread(source, resp)
    else:
        #isnt responsible
        if index==0:
            #hasnt reached resposible yet
            if node.get_rep_type()=="eventual":
                if not failed_to_find[0]:
                    #this query should be executed only once!
                    value=node.query(key)
                    if (value!="None"):
                        resp_text = "has value:" + value + " found at node:" + node.ip_port
                        resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                        post_resp_thread(source, resp)
                        return
                    else:
                        failed_to_find[0]=True
            req = make_same_req(source, req_type,{'key': key, 'value': value, 'resp_ip_port': resp_ip_port, 'index': index, 'failed_to_find':failed_to_find }, req_code)
            post_req_thread(node.succ_ip_port, req)
        else:
            #it has reached responsible
            new_val=node.query(key)
            if new_val!="None" :
                value=new_val
                if node.get_rep_type()=="eventual":
                    resp_text = "has value:" + value + " found at node:" + node.ip_port
                    resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                    post_resp_thread(source, resp)
                    return
            if index!=k-1 and node.succ_ip_port!=resp_ip_port:
                #its not last node-->forward request
                req = make_same_req(source, req_type,{'key': key, 'value': value, 'resp_ip_port': resp_ip_port, 'index': index+1, 'failed_to_find':failed_to_find},req_code)
                post_req_thread(node.succ_ip_port, req)
            else:
                #its last node respond to source
                if value == "None":
                    resp_text="not found at node "+node.ip_port
                else:
                    resp_text = "has value:" + value + " found at node:" + node.ip_port
                resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                post_resp_thread(source, resp)
    return

def dispatch_insert(source,req_code,req_type,data):
    #data = {'key': key, 'value': value, resp_ip_port:"None" , 'index': 0}
    key=data['key']
    value=data['value']
    index=data['index']
    resp_ip_port=data['resp_ip_port']
    if is_responsible(key):
        resp_ip_port=node.ip_port
        text = node.insert(key, value, index)
        #check if last --> if so respond to source and return
        if index==node.get_replicas()-1 or node.succ_ip_port==resp_ip_port:
            resp_text = text + " at node:" + node.ip_port
            resp=make_resp(source,req_type,{'resp_text':resp_text},req_code)
            post_resp_thread(source,resp)
            #its important to return here
            return
        else:
            #if type == eventual respond to source
            if node.get_rep_type() == "eventual":
                #debug("INSERT---> EVENTUAL IF")
                resp_text = text + " at node:" + node.ip_port
                resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                post_resp_thread(source, resp)
            #now forward request to next node
            new_req=make_same_req(source,req_type,{'key':key, 'value':value, 'resp_ip_port':resp_ip_port, 'index':index+1},req_code)
            post_req_thread(node.succ_ip_port,new_req)
    elif index==0:
        #request hasnt reached responsible yet--> just forward it to next
        new_req=make_same_req(source,req_type,data,req_code)
        post_req_thread(node.succ_ip_port,new_req)
    else:
        text=node.insert(key,value,index)
        # if last_node dont forward , also if linear.. respond to source
        if index==node.get_replicas()-1 or node.succ_ip_port==resp_ip_port:
            if node.get_rep_type()=="linearizability":
                resp_text = text + " at node:" + node.ip_port
                resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                post_resp_thread(source, resp)
        else:
            #if not last node --> forward changed req to succ
            new_req = make_same_req(source, req_type,{'key': key, 'value': value, 'resp_ip_port': resp_ip_port, 'index': index + 1},req_code)
            post_req_thread(node.succ_ip_port, new_req)

    return

def dispatch_delete(source,req_code,req_type,data):
    # data = {'key': key,  resp_ip_port: "None", 'index': 0}
    key = data['key']
    index = data['index']
    resp_ip_port = data['resp_ip_port']
    if is_responsible(key):
        resp_ip_port = node.ip_port
        text=node.delete(key)
        # check if last --> if so respond to source and return
        if index == node.get_replicas() - 1 or node.succ_ip_port == resp_ip_port:
            resp_text = text+ " at node:" + node.ip_port
            resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
            post_resp_thread(source, resp)
            # its important to return here
            return
        else:
            # if type == eventual respond to source
            if node.get_rep_type() == "eventual":
                resp_text = text+ " at node:" + node.ip_port
                resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                post_resp_thread(source, resp)
            # now forward request to next node
            new_req = make_same_req(source, req_type, {'key': key, 'resp_ip_port': resp_ip_port, 'index': index + 1},req_code)
            post_req_thread(node.succ_ip_port, new_req)
    elif index == 0:
        # request hasnt reached responsible yet--> just forward it to next
        new_req = make_same_req(source, req_type, data, req_code)
        post_req_thread(node.succ_ip_port, new_req)
    else:
        text=node.delete(key)
        # if last_node dont forward , also if linear.. respond to source
        if index == node.get_replicas() - 1 or node.succ_ip_port == resp_ip_port:
            if node.get_rep_type() == "linearizability":
                resp_text = text+ " at node:" + node.ip_port
                resp = make_resp(source, req_type, {'resp_text': resp_text}, req_code)
                post_resp_thread(source, resp)
        else:
            # if not last node --> forward changed req to succ
            new_req = make_same_req(source, req_type, {'key': key, 'resp_ip_port': resp_ip_port, 'index': index + 1},req_code)
            post_req_thread(node.succ_ip_port, new_req)
    return

def dispatch_query_all(source,req_code,req_type,data):
    pairs = data['key-value pairs']
    element = {node.ip_port: node.keys_vals}
    pairs.append(element)
    new_data = {'key-value pairs': pairs}
    if node.ip_port == source:
        # post response to this node's /ntwresp
        resp = make_resp(source, req_type, new_data, req_code)
        post_resp_thread(source, resp)
    else:
        # post request to succ node
        req=make_same_req(source,req_type,new_data,req_code)
        post_req_thread(node.succ_ip_port, req)
    return

def dispatch_overlay(source,req_code,req_type,data):
    topology = data['topology']
    element = {'node_id': node.id, 'node_ip_port': node.ip_port}
    topology.append(element)
    new_data = {'topology': topology}
    if node.ip_port == source:
        # post response to this node's /ntwresp
        resp = make_resp(source, req_type, new_data, req_code)
        post_resp_thread(source, resp)
    else:
        # post request to succ node
        req=make_same_req(source,req_type,new_data,req_code)
        post_req_thread(node.succ_ip_port, req)
    return

def dispatch_get_keys(source,req_code,req_type,data):
    #vazei ola ta  keys sta data sto 'keys'
    resp=make_resp(source,req_type,{'keys':node.keys_vals},req_code)
    post_resp_thread(source,resp)
    return

def dispatch_join_upd_chain(source,req_code,req_type,data):
    #exei ginei idi to join tou komvou kai exoun enimerwthei oloi oi pointers
    same_keys=data['same_keys']
    new_keys=data['new_keys']
    index=data['index']
    # update data
    node.keys_vals[index]=same_keys
    if (index+2<node.get_replicas()):
        node.pushdown(index+2)
    if (index+1<node.get_replicas()):
        node.keys_vals[index+1]=new_keys
    # if not last node forward next
    if (index!=node.get_replicas()-1 and node.succ_ip_port!=source):
        req=make_same_req(source,req_type,{'index':index+1, 'same_keys':same_keys, 'new_keys':new_keys},req_code)
        post_req_thread(node.succ_ip_port,req)
    else:
        #make response forward source
        resp=make_resp(source,req_type,{},req_code)
        post_resp_thread(source, resp)
    return


def is_responsible(key):
    curr = node.id
    prev = hash(node.prev_ip_port)
    if node.is_alone():
        # ara uparxei mono enas komvos sto chord
        return True
    elif prev > curr:
        if(key > prev or key <= curr):
            return True
        else:
            return False
    else:
        if(key > prev and key <= curr):
            return True
        else:
            return False

def succ_is_responsible(key):
    curr = hash(node.succ_ip_port)
    prev = node.id
    if prev > curr:
        if (key > prev or key <= curr):
            return True
        else:
            return False
    else:
        if (key > prev and key <= curr):
            return True
        else:
            return False

def debug(string):
    print("####################################################")
    print(string)
    print("####################################################\n")

def delayed_join():
    #delay it so that the server has started first before we call join
    time.sleep(20)
    requests.post("http://" + node.ip_port + "/join", json={})
    return

if __name__ == '__main__':
    # run join func via a thread so that the server will have begun when we get the response!!!!
    # if we dont use thread the response comes but the server hasnt started yet so it doesnt accept it
    # IT WORKED!!!
    thread = Thread(target=delayed_join)
    thread.start()
    app.run(host=host, port=port, debug=True)
