import time

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
    print(f"JUST BEFORE POSTING REQ IS:{req}")
    requests.post(url,json=req)


def post_req_thread(ip_port,req):
    thread = Thread(target=post_req_to, kwargs={'ip_port': ip_port, 'req': req})
    thread.start()

def post_resp_to(ip_port, resp):
    url="http://"+ip_port+"/ntwresp"
    print(f"JUST BEFORE POSTING REQ ISSS:{resp}")
    requests.post(url,json=resp)

def post_resp_thread(ip_port,resp):
    thread = Thread(target=post_resp_to, kwargs={'ip_port': ip_port, 'resp': resp})
    thread.start()

def hash(text):
    hash_object = hashlib.sha1(text.encode())
    hash_code = hash_object.hexdigest()
    return hash_code

def make_new_data(req_dict):
    new_data = {}
    req_type = req_dict['type']
    data = req_dict['data']

    if req_type == 'insert':
        value = data['value']
        unhashed_key = data['unhashed_key']
        key = data['key']
        rep_number = int(data['repn']) - 1
        new_data = {'key': key, 'value': value, 'unhashed_key': unhashed_key, 'repn': rep_number}

    elif req_type == 'query':
        unhashed_key = data['unhashed_key']
        key = data['key']
        rep_number = int(data['repn']) - 1
        new_data = {'key': key, 'unhashed_key': unhashed_key, 'repn': rep_number}

    elif req_type == 'delete':
        unhashed_key = data['unhashed_key']
        key = data['key']
        rep_number = int(data['repn']) - 1
        new_data = {'key': key, 'unhashed_key': unhashed_key, 'repn': rep_number}

    elif req_type == 'insert_rep':
        rep_number = int(data['repn']) - 1
        new_data = {'keys_vals':data['keys_vals'], 'repn':rep_number, 'rep_code': data['rep_code'], 'first':False}

    return new_data


def take_action(req_dict):
    print('take_action req_dict is:', req_dict)
    new_data={}
    req_type=req_dict['type']
    data=req_dict['data']
    key = data['key']

    if req_type == 'insert':
        unhashed_key = data['unhashed_key']
        value = data['value']
        repn = int(node.get_replicas()) - int(data['repn'])
        msg = node.insert(key, value, unhashed_key, repn)
        print('take_action_insert: ', node.ip_port, ' action with ', key, value, repn)
        new_data = {'key': key, 'value': value, 'repn': data['repn'], 'resp_text': msg}

    elif req_type == 'query':
        unhashed_key = data['unhashed_key']
        msg = node.query(key, unhashed_key)
        new_data = {'key': key, 'resp_text': msg}

    elif req_type == 'delete':
        unhashed_key = data['unhashed_key']
        msg = node.delete(key, unhashed_key)
        print('take_action_delete: ', node.ip_port, ' action with', key)
        new_data = {'key': key, 'repn': data['repn'], 'resp_text': msg}

    elif req_type=='join':
        #     1) update this nodes prev to source
        #     2) update prev node's next to source-> post to /ntw_resp me type set_neighboors
        #     3) remove keys needed from this node and hand to source
        #     4)give sources' prev and next to node.prev and node.ip_port
        id1=hash(node.prev_ip_port)
        source=req_dict['source']
        id2=hash(source)
        join_keys_vals=node.rem_ret_betw_keys(id1, id2)
        join_prev=node.prev_ip_port
        join_succ=node.ip_port
        # post prev to update neighboors and w8 for response
        post_resp_to(node.prev_ip_port,{'type':'set_neighboors' , 'prev':'None', 'succ':source})
        node.prev_ip_port=source
        new_data={'prev':join_prev, 'succ':join_succ, 'keys_vals':join_keys_vals}
    return new_data


def handle_response(resp):
    receiver=resp['receiver']
    resp_type=resp['type']
    data=resp['data']
    seqn=resp['seqn']
    # check if i am the correct receiver
    if(node.ip_port== receiver):
        if resp_type=='insert':
            msg=data['resp_text']
        elif resp_type== 'query':
            msg=data['resp_text']
        elif resp_type== 'delete':
            msg=data['resp_text']
        elif resp_type== 'depart':
            msg=data['resp_text']
        elif resp_type=='insert_rep':
            msg==data['resp_text']
        elif resp_type == 'overlay':
            topology = data['topology']
            msg = "This is the Chord topology:\n" + str(topology)+"\n"
        elif resp_type == 'query_all':
            pairs = data['key-value pairs']
            msg = "Those are all key-value pairs in Chord:\n" + str(pairs)+"\n"
        elif resp_type== 'join':
            #update prev next and keys
            node.set_neighboors(data['prev'],data['succ'])
            node.keys_vals[0]=data['keys_vals']
            repn = node.get_replicas()
            rep_type = node.get_rep_type()
            msg=f"Node {node.id}:[{node.ip_port}] joined the Chord with replication type {rep_type} and {repn} replicas!\n"
    return msg

@app.route('/', methods=['POST', 'GET'])
def func1():
    if request.method == 'GET':

        return "Hello Normal node guest\n"
    if request.method == 'POST':
        req_dict = request.form
        resp_text = f"Key={req_dict['key']}, Value={req_dict['value']}\n"
        print(resp_text)
        return resp_text

@app.route('/insert', methods=['POST'])
def insert():
    if request.method == 'POST':

        global seqn
        seqn = seqn + 1
        req_code = str(seqn)

        request_dict = request.form

        print(f"/insert request dict:{request_dict}\n")
        # hash the key before doing anything with it
        key = hash(request_dict['key'])
        value = request_dict['value']
        rep_number = node.get_replicas()
        data = {'key': key, 'value': value, 'unhashed_key': request_dict['key'], 'repn': rep_number}

        req = make_req('insert', data, req_code)
        print(f"/insert REQUEST IS:{req}\n")

        post_req_thread(node.ip_port, req)
        while responses_dict.get(req_code, "None") == "None":
            {}
        #       pop response from dict and handle it
        resp = responses_dict.pop(req_code)
        return handle_response(resp)

@app.route('/query', methods=['POST'])
def query():
    if request.method == 'POST':
        global seqn
        seqn=seqn+1
        req_code=str(seqn)

        request_dict=request.form

        if request_dict['key'] != '*':
            
            key=hash(request_dict['key'])
            rep_number = node.get_replicas()
            data={'key':key, 'unhashed_key': request_dict['key'],'repn': rep_number}

            req=make_req('query',data,req_code)
            post_req_thread(node.ip_port, req)
        else:
            data={'key-value pairs':[]}
            req = make_req('query_all', data, req_code)
            post_req_thread(node.succ_ip_port, req)
            
        while responses_dict.get(req_code,"None")=="None":
            {}
#       pop response from dict and handle it
        resp=responses_dict.pop(req_code)
        return handle_response(resp)

@app.route('/delete', methods=['POST'])
def delete():
    if request.method == 'POST':
        global seqn
        seqn = seqn + 1
        req_code = str(seqn)

        request_dict = request.form
        key = hash(request_dict['key'])
        rep_number = node.get_replicas()
        data = {'key': key, 'unhashed_key': request_dict['key'],'repn': rep_number}

        req = make_req('delete', data, req_code)
        post_req_thread(node.ip_port, req)
        while responses_dict.get(req_code, "None") == "None":
            {}
        #       pop response from dict and handle it
        resp = responses_dict.pop(req_code)
        return handle_response(resp)

@app.route('/depart', methods=['POST'])
def depart():

    if request.method == 'POST':

        if not node.get_isInChord():
            msg = 'This node does not participate in Chord!\n'
        else:
            # Στις επόμενες 4 γραμμές, μαζεύω τα data που θα στείλω στον successor
            global seqn
            seqn=seqn+1
            req_code=str(seqn)

            rep_number = node.get_replicas()
            curr_repn = int(rep_number) + 1

            if node.is_alone():
                node.init_state()
                return f"Node {node.ip_port} departed from the Chord!\n"
            else:
                #first take care of the data
                for data_dict in node.keys_vals:

                    curr_repn -= 1
                    rep_code = req_code+"repn"+str(curr_repn)

                    data = {'keys_vals': data_dict, 'repn':str(curr_repn), 'rep_code': rep_code, 'first':True}
                    req = make_req('insert_rep', data, req_code)
                    post_req_thread(node.succ_ip_port, req)
     
                    while responses_dict.get(rep_code,"None")=="None":
                        {}

                #then take care of the neighbour pointers

                data = {'prev':node.prev_ip_port, 'succ':node.succ_ip_port}
                req=make_req('depart', data, req_code)
                if not node.is_duo():
                    # an exoume >=2 nodes sto chord mono tote stelnoume 2 post , alliws stelnoume mono1 kai pernoyme pali 2 responses
                    post_req_thread(node.prev_ip_port, req)
                post_req_thread(node.succ_ip_port, req)
                 # w8 till we have prev and succ responses (codes-> req_code+_prev/_succ) then pop and handle both
                prev_code=req_code+'_prev'
                succ_code=req_code+'_succ'
                while responses_dict.get(prev_code,"None")=="None":
                    {}
                while responses_dict.get(succ_code, "None")=="None":
                    {}
                 # pop response from dict and handle it
                resp_prev=responses_dict.pop(prev_code)
                resp_succ=responses_dict.pop(succ_code)
                handle_response(resp_prev)
                handle_response(resp_succ)
                
                # Then return the node to dafault state
                node.init_state()

        return f"Node {node.ip_port} successfully departed from Chord!\n"


@app.route('/overlay', methods=['POST'])
def overlay():
    if request.method == 'POST':
        global seqn
        seqn=seqn+1
        req_code=str(seqn)

        data={'topology':[]}
        req=make_req('overlay',data,req_code)
        post_req_thread(node.succ_ip_port, req)
        while responses_dict.get(req_code,"None")=="None":
            {}
#       pop response from dict and handle it
        resp=responses_dict.pop(req_code)
        return handle_response(resp)



@app.route('/join',methods=['POST'])
def call_join():
    if node.get_isInChord():
        msg = 'This node is already in Chord!\n'
    else:
        msg=join()
    return msg

@app.route('/show_info', methods=['POST'])
def show_info():
    return node.return_node_stats() 

def join():
    # its important to stall join request so that our server has started properly and then we can get the boot server response

    global seqn
    seqn=seqn+1
    req_code=str(seqn)

    data={'key':node.id}
    req=make_req('join',data,req_code)
    print(f"THIS IS THE JOIN REQUEST IM ABOUT TO SEND:{req}\n")
    print(f"AND THIS IS THE ADDRESS I AM SENDINT IT TO:{node.boot_ip_port}\n")
    post_req_thread(node.boot_ip_port, req)
    while responses_dict.get(req_code,"None")=="None":
        {}
    # pop response from dict and handle it
    resp=responses_dict.pop(req_code)
    msg=(handle_response(resp))
    return msg


@app.route('/ntwreq',methods=['POST'])
def ntwreq():
    req_dict=json.loads(request.data)

    print('ntwreq data is:',req_dict)

    source=req_dict['source']
    req_type=req_dict['type']
    data=req_dict['data']
    req_code=req_dict['seqn']

    if node.get_rep_type() == "eventual":

        if req_type == 'insert_rep':

            if int(data.get('repn')) == int(node.get_replicas()) or data.get('first'):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                index = int(node.get_replicas()) - int(data.get('repn'))

                node.keys_vals[index].update(data['keys_vals'])

                if index+1 < int(node.get_replicas()):
                    node.delete_same_keys(index+1, data['keys_vals'])

                resp_code = data.get('rep_code')
                resp_text = "Data for "+resp_code+" updated!\n"
                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': resp_text}
                resp = make_resp(source, req_type, resp_data, resp_code)
                print(f"I AM POSTING THE RESPONSE:{resp}")
                post_resp_thread(source, resp)

                if int(node.get_replicas()) != 1:
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)

            elif 0 < int(data.get('repn')) < int(node.get_replicas()):
                # at least the responsible server has taken care of the request
                # replica managers don't send responses

                index = int(node.get_replicas()) - int(data.get('repn'))

                node.keys_vals[index].update(data['keys_vals'])

                if index+1 < int(node.get_replicas()):
                    node.delete_same_keys(index+1, data['keys_vals'])

                new_data = make_new_data(req_dict)
                if int(data.get('repn')) != 1:
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)    

        elif req_type== 'depart':
 
            #is next or is prev?
            #is_next-> update prev_ip , update keys_vals with departing nodes'
            #is_prev -> update next_ip
            #data={'prev':node.prev_ip_port, 'succ':node.succ_ip_port}

            if node.is_next(source):
                node.set_neighboors(data['prev'],"None")

                resp_data={'sender_ip_port':node.ip_port, 'resp_text':"Next node updated...\n"}
                resp_code=str(req_code)+"_succ"
                resp=make_resp(source, req_type, resp_data, resp_code)
                post_resp_thread(source,resp)
            #  in special cases 1 or 2 nodes on Chord a node could be both succ and prev so we dont use elif
            if node.is_prev(source):
                node.set_neighboors("None",data['succ'])

                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': "prev node updated...\n"}
                resp_code = str(req_code) + "_prev"
                resp = make_resp(source, req_type, resp_data, resp_code)
                post_resp_thread(source, resp)

        elif req_type== 'overlay':

            topology = data['topology']
            element = {'node_id': node.id, 'node_ip_port': node.ip_port}
            topology.append(element)
            new_data = {'topology': topology}
            if node.ip_port==source:
                # post response to this node's /ntwresp
                resp=make_resp(source, req_type, new_data, req_code)
                post_resp_thread(source, resp)
            else:
                # post request to succ node
                req_dict['data']=new_data
                post_req_thread(node.succ_ip_port, req_dict)

        elif req_type == 'query_all':

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
                req_dict['data'] = new_data
                post_req_thread(node.succ_ip_port, req_dict)

        elif req_type == 'query':

            if is_responsible(data.get('key')) or node.has_key(data.get('key')):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                new_data = take_action(req_dict)
                resp = make_resp(source, req_type, new_data, req_code)
                print(f"I AM POSTING THE QUERY RESPONSE:{resp}\n")
                post_resp_thread(source, resp)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)

        elif req_type == 'insert' or req_type == 'delete':

            # insert and delete implementation here
            if is_responsible(data.get('key')):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                new_data = take_action(req_dict)
                resp = make_resp(source, req_type, new_data, req_code)
                print(f"I AM POSTING THE RESPONSE:{resp}")
                post_resp_thread(source, resp)
                if int(node.get_replicas()) != 1:
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
            elif 0 < int(data.get('repn')) < int(node.get_replicas()):
                # at least the responsible server has taken care of the request
                # replica managers don't send responses
                take_action(req_dict)
                new_data = make_new_data(req_dict)
                if int(data.get('repn')) != 1:
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)

        else:

            if is_responsible(data.get('key')):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                new_data=take_action(req_dict)
                resp=make_resp(source,req_type,new_data,req_code)
                print(f"I AM POSTING THE JOIN RESPONSE:{resp}\n")
                post_resp_thread(source,resp)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)

    if node.get_rep_type() == "linearizability":

        if req_type == 'insert_rep':

            if int(data.get('repn')) == int(node.get_replicas()) or data.get('first'):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                index = int(node.get_replicas()) - int(data.get('repn'))

                node.keys_vals[index].update(data['keys_vals'])

                if index+1 < int(node.get_replicas()):
                    node.delete_same_keys(index+1, data['keys_vals'])

                if int(node.get_replicas()) != 1 and int(data.get('repn')) != 1:
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
                else:
                    resp_code = data.get('rep_code')
                    resp_text = "Data for "+resp_code+" updated!\n"
                    resp_data = {'sender_ip_port': node.ip_port, 'resp_text': resp_text}
                    resp = make_resp(source, req_type, resp_data, resp_code)
                    print(f"I AM POSTING THE RESPONSE:{resp}")
                    post_resp_thread(source, resp)

            elif 0 < int(data.get('repn')) < int(node.get_replicas()):
                # at least the responsible server has taken care of the request
                # replica managers don't send responses

                index = int(node.get_replicas()) - int(data.get('repn'))

                node.keys_vals[index].update(data['keys_vals'])

                if index+1 < int(node.get_replicas()):
                    node.delete_same_keys(index+1, data['keys_vals'])

                new_data = make_new_data(req_dict)
                if int(data.get('repn')) != 1:
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req) 
                else:
                    resp_code = data.get('rep_code')
                    resp_text = "Data for "+resp_code+" updated!\n"
                    resp_data = {'sender_ip_port': node.ip_port, 'resp_text': resp_text}
                    resp = make_resp(source, req_type, resp_data, resp_code)
                    print(f"I AM POSTING THE RESPONSE:{resp}")
                    post_resp_thread(source, resp)   

        elif req_type== 'depart':
 
            #is next or is prev?
            #is_next-> update prev_ip , update keys_vals with departing nodes'
            #is_prev -> update next_ip
            #data={'prev':node.prev_ip_port, 'succ':node.succ_ip_port}

            if node.is_next(source):
                node.set_neighboors(data['prev'],"None")

                resp_data={'sender_ip_port':node.ip_port, 'resp_text':"Next node updated...\n"}
                resp_code=str(req_code)+"_succ"
                resp=make_resp(source, req_type, resp_data, resp_code)
                post_resp_thread(source,resp)
            #  in special cases 1 or 2 nodes on Chord a node could be both succ and prev so we dont use elif
            if node.is_prev(source):
                node.set_neighboors("None",data['succ'])

                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': "prev node updated...\n"}
                resp_code = str(req_code) + "_prev"
                resp = make_resp(source, req_type, resp_data, resp_code)
                post_resp_thread(source, resp)

        elif req_type== 'overlay':

            topology = data['topology']
            element = {'node_id': node.id, 'node_ip_port': node.ip_port}
            topology.append(element)
            new_data = {'topology': topology}
            if node.ip_port==source:
                # post response to this node's /ntwresp
                resp=make_resp(source,'overlay',new_data,req_code)
                post_resp_thread(source,resp)
            else:
                # post request to succ node
                req_dict['data']=new_data
                post_req_thread(node.succ_ip_port, req_dict)

        elif req_type == 'query_all':

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
                req_dict['data'] = new_data
                post_req_thread(node.succ_ip_port, req_dict)

        elif req_type == 'query':

            if is_responsible(data.get('key')):
                if int(node.get_replicas()) != 1:
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
                else:
                    new_data = take_action(req_dict)
                    resp = make_resp(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE QUERY RESPONSE:{resp}\n")
                    post_resp_thread(source, resp)
            elif 0 < int(data.get('repn')) < int(node.get_replicas()):
                if int(data.get('repn')) == 1:
                    new_data = take_action(req_dict)
                    resp = make_resp(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE QUERY RESPONSE:{resp}\n")
                    post_resp_thread(source, resp)
                else:
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)

        elif req_type == 'insert' or req_type == 'delete':

            # insert and delete implementation here
            if is_responsible(data.get('key')):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                if int(node.get_replicas()) != 1:
                    take_action(req_dict)
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
                else:
                    new_data = take_action(req_dict)
                    resp = make_resp(source, req_type, new_data, req_code)
                    print(f"I WITH", node.ip_port, " AM RESPONSIBLE AND I AM POSTING THE RESPONSE:{resp}")
                    post_resp_thread(source, resp)
            elif 0 < int(data.get('repn')) < int(node.get_replicas()):
                # at least the responsible server has taken care of the request
                # replica managers don't send responses
                if int(data.get('repn')) == 1:
                    new_data = take_action(req_dict)
                    resp = make_resp(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE RESPONSE:{resp}\n")
                    post_resp_thread(source, resp)
                else:
                    take_action(req_dict)
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)

        else:
            # only join function here, not set already
            if is_responsible(data.get('key')):
                # do actions, make response , post it to source /ntwresp
                # take_action(req_dict) -> do actions and return new_data
                new_data=take_action(req_dict)
                resp=make_resp(source,req_type,new_data,req_code)
                print(f"I AM POSTING THE JOIN RESPONSE:{resp}\n")
                post_resp_thread(source,resp)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)
    return {}

@app.route('/ntwresp',methods=['POST'])
def ntwresp():
    resp_dict=json.loads(request.data)
    if resp_dict['type']=='set_neighboors':
        prev_ip_port=resp_dict['prev']
        succ_ip_port=resp_dict['succ']
        print('normal:', node.ip_port, 'with:', prev_ip_port, succ_ip_port)
        node.set_neighboors(prev_ip_port,succ_ip_port)
    elif resp_dict['type']=='join_vars':
        repn = resp_dict['repn']
        rep_type = resp_dict['rep_type']
        node.join_set_vars(repn, rep_type)
    else:
        seqn=resp_dict['seqn']
        responses_dict[seqn]=resp_dict
    return {}

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


if __name__ == '__main__':
    # run join func via a thread so that the server will have begun when we get the response!!!!
    # if we dont use thread the response comes but the server hasnt started yet so it doesnt accept it
    # IT WORKED!!!
    # thread = Thread(target=join)
    # thread.start()
    app.run(host=host, port=port, debug=True)