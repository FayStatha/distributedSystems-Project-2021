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
        return int(self.replicas)

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
             msg+="inserted"
        else:
             msg+="updated"
        self.keys_vals[int(repn)].update({key: val})
        return msg

    def query(self, key):
        #epistrefei to value an vrethei alliws "None"
        x="None"
        for data_dict in self.keys_vals:
            x = data_dict.get(key, "None")
            if x == "None":
                continue
            else:
                return x
        return x

    def delete(self,key):
        for data_dict in self.keys_vals:
            x= data_dict.pop(key, "None")
            if x == "None":
                continue
            else:
                msg="deleted"
                return msg
        msg="doesn't exist hence cant be deleted"
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

    def get_same_new_keys(self,key):
        #computes and returns [same_keys,new_keys]
        same_keys={}
        new_keys={}
        id1=key
        id2=self.id
        # special case which needs or
        if id1 > id2:
            for k, v in self.keys_vals[0].items():
                if (k > id1 or k <= id2):
                    same_keys[k] = v
                else:
                    new_keys[k] = v
        # in this case we need and
        else:
            for k, v in self.keys_vals[0].items():
                if (k > id1 and k <= id2):
                    same_keys[k] = v
                else:
                    new_keys[k] = v
        return same_keys,new_keys

    def pushdown(self,index):
        #pushdown all dicts of keys   index-1-->index , index-->index+1 ,...  , k-2-->k-1
        # index values >0  and <k
        index=int(index)
        k=self.get_replicas()
        if (index<=0 or index>=k):
            return
        else:
            temp=self.keys_vals[index-1]
            for i in range(index,k):
                temp2=self.keys_vals[i]
                self.keys_vals[i]=temp
                temp=temp2
            return

    def pushup(self,index):
        index=int(index)
        k = self.get_replicas()
        if index>=k-1 or index<0:
            return
        for i in range(index+1,k):
            self.keys_vals[i-1]=self.keys_vals[i]
        return

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
        hashtable=""
        for i in range(len(self.keys_vals)):
            dict=self.keys_vals[i]
            l=[]
            for k,v in dict.items():
                l.append(v)
            hashtable+=str(l)+"\n"

        msg=f"\nIP:{self.ip_port}\n  Prev_IP:{ self.prev_ip_port}\n Next_IP:{ self.succ_ip_port}\n Boot_IP:{self.boot_ip_port}\n Hashtable:\n"+hashtable+"\n"
        return msg