import hashlib
import json


class node():

    # init method or constructor
    def __init__(self, ip_port, boot_ip, k=1, reptype="None", isBootstrap=False):
        self.ip_port:str=ip_port
        self.boot_ip_port:str=boot_ip
        self.prev_ip_port:str=ip_port
        self.succ_ip_port:str=ip_port
        self.id:str=self.make_id()
        self.keys_vals=[] # a list of dict
        self.replicas = k
        self.rep_type = reptype
        self.isInChord = False
        if isBootstrap:
            for i in range(int(k)):
                self.keys_vals.append({})

    def get_rep_type(self):
        # Getter for replication type
        return self.rep_type

    def get_replicas(self):
        # Getter for number of replicas
        return self.replicas

    def get_isInChord(self):
        #Getter for isInChord variable
        return self.isInChord

    def hash(self,text):
        hash_object = hashlib.sha1(text.encode())
        hash_code = hash_object.hexdigest()
        return hash_code

    def make_id(self):
        id=self.hash(self.ip_port)
        return id

    def has_key(self, key):
        for data_dict in self.keys_vals:
            if data_dict.get(key, "None") == "None":
                continue
            else:
                return True
        return False

    def insert(self, key, val, repn):
        # returns pair inserted or updated if it already existed
        msg=""
        if self.keys_vals[int(repn)].get(key, "None")=="None":
             msg+=f"pair ({key},{val}) inserted\n"
        else:
             msg+=f"pair ({key},{val}) updated\n"
        self.keys_vals[int(repn)].update({key: val})
        return msg

    def query(self, key):
        for data_dict in self.keys_vals:
            x = data_dict.get(key, "None")
            if x == "None":
                continue
            else:
                msg = f"The {key} corresponds to the value:{x}\n"
                return msg
        msg = f"The key:{key} was not found\n"
        return msg

    def delete(self,key):
        for data_dict in self.keys_vals:
            x= data_dict.pop(key, "None")
            if x == "None":
                continue
            else:
                msg=f"Pair {key}:{x} deleted succesfully!\n"
                return msg
        msg=f"Key:{key} doesn't exist, hence cant be deleted\n"
        return msg

    def is_next(self,source_ip_port):
        if self.prev_ip_port == source_ip_port:
            return True
        else:
            return False

    def is_prev(self,source_ip_port):
        if self.succ_ip_port == source_ip_port:
            return True
        else:
            return False

    def set_neighboors(self, prev_ip_port, succ_ip_port):
        if prev_ip_port != "None":
            self.prev_ip_port=prev_ip_port
        if succ_ip_port != "None":
            self.succ_ip_port= succ_ip_port

    def join_set_vars(self, repn, rep_type):
        self.replicas = repn
        self.rep_type = rep_type
        self.isInChord = True
        for i in range(int(repn)):
            self.keys_vals.append({})

    def rem_ret_betw_keys(self,id1,id2):

        # need to change

        betw_keys={}
        new_keys_vals={}
        # special case which needs or
        if id1>id2:
            for k,v in self.keys_vals[0].items():
                if (k>id1 or k<=id2):
                    betw_keys[k]=v
                else:
                    new_keys_vals[k]=[v]
        #in this case we need and
        else:
            for k,v in self.keys_vals[0].items():
                if (k>id1 and k<=id2):
                    betw_keys[k]=v
                else:
                    new_keys_vals[k]=[v]

        self.keys_vals[0]=new_keys_vals
        return betw_keys

    def is_alone(self):
        if self.prev_ip_port==self.succ_ip_port==self.ip_port:
            return True
        else:
            return False

    def is_duo(self):
        if ((not self.is_alone()) and self.succ_ip_port == self.prev_ip_port):
            return True
        else:
            return False

    def init_state(self):
        self.prev_ip_port: str =self.ip_port
        self.succ_ip_port: str = self.ip_port
        self.keys_vals = []
        self.replicas = 1
        self.rep_type = "None"
        self.isInChord = False

    def return_node_stats(self):

        #SOS TO HASHTABLE EDW DEN KSERW PWS NA TO FTIAKSW

        msg=f"IP:{self.ip_port}\n ID:{self.id}\n Prev_IP:{ self.prev_ip_port}\n Next_IP:{ self.succ_ip_port}\n Boot_IP:{self.boot_ip_port}\n Hashtable:{json.dumps(self.keys_vals)}\n"
        return msg