import time

from flask import Flask, render_template, request, redirect, url_for
import sys
import requests
import json
from node import node
from threading import Thread
import hashlib
from colorama import Fore, Style

# TODO
# 2)make query(*)  case
# 3)make request/response logs
# 6) !!! debugging !!!

app = Flask(__name__)
ip_port = sys.argv[1]
replicas = sys.argv[3] # pairnei to plithos twn replicas
rep_type = sys.argv[4] # pairnei ton typo rep pou tha xrisimopoihthei (dekta mono ta : "linearizability", "eventual")
boot_ip_port = ip_port
# THIS IS HOST AND PORT FOR A NORMAL NODE
host_port = ip_port.split(":")
host = host_port[0]
port = host_port[1]
# construct node
node = node(ip_port, boot_ip_port, replicas, rep_type, True)

responses_dict = {}
# istoriko twn request kai responses gia ta antistoixa seqn
requests_list = []
responses_list = []
seqn = 0

bg_colors = {'yellow': 43, 'blue': 44, 'red': 41, 'green': 42, 'purple': 45}
font_colors = {'black': 30, 'red': 31, 'green': 32, 'yellow': 33, 'blue': 34}


def style_str(string, font_color, bg_color):
    f_col = str(font_colors[font_color])
    bg_col = str(bg_colors[bg_color])
    style = f'2;{f_col};{bg_col}'
    new_s = '\x1b[%sm %s \x1b[0m' % (style, string)
    return new_s


def make_req(req_type, data, req_code):
    req = {'source': node.ip_port, 'type': req_type, 'data': data, 'seqn': (str(req_code))}
    return req


def make_resp(receiver, resp_type, data, req_code):
    resp = {'receiver': receiver, 'type': resp_type, 'data': data, 'seqn': (str(req_code))}
    return resp

def make_same_req(source, req_type, data, req_code):
    req = {'source': source, 'type': req_type, 'data': data, 'seqn': (str(req_code))}
    return req

def post_req_to(ip_port, req):
    url = "http://" + ip_port + "/ntwreq"
    print(f"JUST BEFORE POSTING REQ IS:{req}")
    requests.post(url, json=req)


def post_req_thread(ip_port, req):
    thread = Thread(target=post_req_to, kwargs={'ip_port': ip_port, 'req': req})
    thread.start()


def post_resp_to(ip_port, resp):
    url = "http://" + ip_port + "/ntwresp"
    requests.post(url, json=resp)


def post_resp_thread(ip_port, resp):
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
    key = data['key']
    if req_type == 'insert':
        value = data['value']
        rep_number = int(data['repn']) - 1
        new_data = {'key': key, 'value': value, 'repn': rep_number}

    elif req_type == 'query':
        rep_number = int(data['repn']) - 1
        new_data = {'key': key, 'repn': rep_number}

    elif req_type == 'delete':
        rep_number = int(data['repn']) - 1
        new_data = {'key': key, 'repn': rep_number}

    return new_data


def take_action(req_dict):
    new_data = {}
    req_type = req_dict['type']
    data = req_dict['data']
    key = data['key']
    if req_type == 'insert':
        repn = int(node.get_replicas()) - int(data['repn'])
        value = data['value']
        msg = node.insert(key, value, repn)
        new_data = {'key': key, 'value': value, 'repn': data['repn'], 'resp_text': msg}

    elif req_type == 'query':
        msg = node.query(key)
        new_data = {'key': key, 'resp_text': msg}

    elif req_type == 'delete':
        msg = node.delete(key)
        new_data = {'key': key, 'repn': data['repn'], 'resp_text': msg}

    elif req_type == 'join':
        #     1) update this nodes prev to source
        #     2) update prev node's next to source-> post to /ntw_resp me type set_neighboors
        #     3) remove keys needed from this node and hand to source
        #     4)give sources' prev and next to node.prev and node.ip_port
        id1 = hash(node.prev_ip_port)
        source = req_dict['source']
        id2 = hash(source)
        join_keys_vals = node.rem_ret_betw_keys(id1, id2)
        join_prev = node.prev_ip_port
        join_succ = node.ip_port
        # post prev to update neighboors and w8 for response
        post_resp_to(node.prev_ip_port, {'type': 'set_neighboors', 'prev': 'None', 'succ': source})
        node.prev_ip_port = source
        new_data = {'prev': join_prev, 'succ': join_succ, 'keys_vals': join_keys_vals}
    return new_data


def handle_response(resp):
    receiver = resp['receiver']
    resp_type = resp['type']
    data = resp['data']
    seqn = resp['seqn']
    # check if i am the correct receiver
    if (node.ip_port == receiver):
        if resp_type == 'insert':
            msg = data['resp_text']
        elif resp_type == 'query':
            msg = data['resp_text']
        elif resp_type == 'delete':
            msg = data['resp_text']
        elif resp_type == 'overlay':
            topology = data['topology']
            msg = "This is the Chord topology:\n" + str(topology)+"\n"
    return msg


@app.route('/', methods=['POST', 'GET'])
def func1():
    if request.method == 'GET':
        return "Hello Normal node guest"
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
        data = {'key': key, 'value': value, 'repn': rep_number}

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
        key=hash(request_dict['key'])
        rep_number = node.get_replicas()
        data={'key':key, 'repn': rep_number}

        req=make_req('query',data,req_code)
        post_req_thread(node.ip_port, req)
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
        data = {'key': key, 'repn': rep_number}

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
       return "Bootstrap node never departs from Chord..\n"


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
        return handle_response(resp)


@app.route('/join', methods=['POST'])
def call_join():
    return "Bootstrap node is always in Chord!\n"

@app.route('/show_info', methods=['POST'])
def show_info():
    return node.return_node_stats()

def join():

    global seqn
    seqn = seqn + 1
    req_code = str(seqn)

    data = {'key': node.id}
    req = make_req('join', data, req_code)

    print ("The Chord now starts...\n")


@app.route('/ntwreq', methods=['POST'])
def ntwreq():
    req_dict = json.loads(request.data)

    print(req_dict)

    source = req_dict['source']
    req_type = req_dict['type']
    # to data apotelei dict keys:values opou keys oi metavlites kai values oi times tous
    data = req_dict['data']
    req_code = req_dict['seqn']

    if node.get_rep_type() == "eventual":
        if req_type == 'depart':
            #     is next or is prev?
            # is_next-> update prev_ip , update keys_vals with departing nodes'
            # is_prev -> update next_ip
            # data={'keys_vals':node.keys_vals, 'prev':node.prev_ip_port, 'succ':node.succ_ip_port}
            if node.is_next(source):
                node.set_neighboors(data['prev'], "None")
                # updates keys of this node from the ones from the departing node
                node.keys_vals.update(data['keys_vals'])

                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': "next node updated...\n"}
                resp_code = str(req_code) + "_succ"
                resp = make_resp(source, 'depart', resp_data, resp_code)
                post_resp_thread(source, resp)
            #     in special cases 1 or 2 nodes on Chord a node could be both succ and prev so we dont use elif
            if node.is_prev(source):
                node.set_neighboors("None", data['succ'])

                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': "prev node updated...\n"}
                resp_code = str(req_code) + "_prev"
                resp = make_resp(source, 'depart', resp_data, resp_code)
                post_resp_thread(source, resp)

        elif req_type == 'overlay':
            topology = data['topology']
            element = {'node_id': node.id, 'node_ip_port': node.ip_port}
            topology.append(element)
            new_data = {'topology': topology}
            if node.ip_port == source:
                # post response to this node's /ntwresp
                resp = make_resp(source, 'overlay', new_data, req_code)
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
                    #only if you need to make replicas you send a request
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
            # only join function here, not set already
            post_resp_to(source, {'type': 'join_vars', 'repn': node.get_replicas(), 'rep_type': node.get_rep_type()})
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

    elif node.get_rep_type() == "linearizability":

        if req_type == 'depart':
            #     is next or is prev?
            # is_next-> update prev_ip , update keys_vals with departing nodes'
            # is_prev -> update next_ip
            # data={'keys_vals':node.keys_vals, 'prev':node.prev_ip_port, 'succ':node.succ_ip_port}
            if node.is_next(source):
                node.set_neighboors(data['prev'], "None")
                # updates keys of this node from the ones from the departing node
                node.keys_vals.update(data['keys_vals'])

                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': "next node updated...\n"}
                resp_code = str(req_code) + "_succ"
                resp = make_resp(source, 'depart', resp_data, resp_code)
                post_resp_thread(source, resp)
            #     in special cases 1 or 2 nodes on Chord a node could be both succ and prev so we dont use elif
            if node.is_prev(source):
                node.set_neighboors("None", data['succ'])

                resp_data = {'sender_ip_port': node.ip_port, 'resp_text': "prev node updated...\n"}
                resp_code = str(req_code) + "_prev"
                resp = make_resp(source, 'depart', resp_data, resp_code)
                post_resp_thread(source, resp)

        elif req_type == 'overlay':
            topology = data['topology']
            element = {'node_id': node.id, 'node_ip_port': node.ip_port}
            topology.append(element)
            new_data = {'topology': topology}
            if node.ip_port == source:
                # post response to this node's /ntwresp
                resp = make_resp(source, 'overlay', new_data, req_code)
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
                    print(f"I AM POSTING THE RESPONSE:{resp}\n")
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
                    new_data = make_new_data(req_dict)
                    new_req = make_same_req(source, req_type, new_data, req_code)
                    print(f"I AM POSTING THE SAME REQ:{new_req} TO NEXT REPLICA MANAGER\n")
                    post_req_thread(node.succ_ip_port, new_req)
            else:
                # post same request to succ /ntwreq
                print(f"I AM POSTING THE SAME REQ:{req_dict} TO NEXT NODE\n")
                post_req_thread(node.succ_ip_port, req_dict)
        else:
            post_resp_to(source, {'type': 'join_vars', 'repn': node.get_replicas(), 'rep_type': node.get_rep_type()})
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


@app.route('/ntwresp', methods=['POST'])
def ntwresp():
    resp_dict = json.loads(request.data)

    if resp_dict['type'] == 'set_neighboors':
        prev_ip_port = resp_dict['prev']
        succ_ip_port = resp_dict['succ']
        node.set_neighboors(prev_ip_port, succ_ip_port)
    else:
        seqn = resp_dict['seqn']
        responses_dict[seqn] = resp_dict
    return {}


def is_responsible(key):
    curr = node.id
    prev = hash(node.prev_ip_port)
    if node.is_alone():
        # ara uparxei mono enas komvos sto chord
        return True
    elif prev > curr:
        if (key > prev or key <= curr):
            return True
        else:
            return False
    else:
        if (key > prev and key <= curr):
            return True
        else:
            return False

if __name__ == '__main__':
    # run join func via a thread so that the server will have begun when we get the response!!!!
    # if we dont use thread the response comes but the server hasnt started yet so it doesnt accept it
    # IT WORKED!!!
    # thread = Thread(target=join)
    # thread.start()
    join()
    app.run(host=host, port=port, debug=True)

