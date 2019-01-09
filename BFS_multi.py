__author__ = 'Administrator'
from mpi4py import MPI
import graph_tool.all as gt
import numpy as np
import Queue as queue
import globalvar as gl
import numpy as np
import loadgraph as lg
import time
import random
import bfs_serial as bfss
# mpiexec -np 3 python BFS_multi.py
class BFS:
    result_node=set()
    result_dist={}
    out_messages =[]
    in_messages =[]
    con_run =0
    def __init__(self):
        self.result_node=set()
        self.result_dist={}
        self.out_messages =[]
        self.in_messages =[]
        self.con_run =0
        for i in xrange(0,gl.worker_num):
            self.out_messages.append([])
            self.in_messages.append([])

    def clear_messages(self):
        self.out_messages =[]
        self.in_messages =[]
        for i in xrange(0,gl.worker_num):
            self.out_messages.append([])
            self.in_messages.append([])

    def send_messages(self):
        for i in xrange(0,gl.worker_num):
            partner = (i - gl.worker_id +gl.worker_num) % gl.worker_num
            if partner != gl.worker_id:
                if gl.worker_id < partner :
                    gl.comm.send(self.out_messages[partner], dest=partner, tag=0)
                    self.in_messages[partner] = gl.comm.recv(source=partner, tag=0)
                else :
                    self.in_messages[partner] = gl.comm.recv(source=partner, tag=0)
                    gl.comm.send(self.out_messages[partner], dest=partner, tag=0)

    def is_continue(self):
        for i in xrange(0,gl.worker_num):
            if self.in_messages[i]:
                    self.con_run =1
                    break
        buf = np.array([self.con_run])
        gl.comm.Barrier()
        gl.comm.Allreduce(MPI.IN_PLACE,buf,MPI.SUM)

        if buf[0]>0:
            self.con_run = 0
            return True
        else:
            return False

    def pEval(self,Dgraph,root_set,bound):
        que = queue.Queue()
        for v in root_set:
            if v in gl.inner_node:
                que.put((gl.global2local[v],bound))
        while not que.empty():
            node,distance = que.get()
            if node in self.result_node:
                continue
            self.result_node.add(node)
            self.result_dist[node]=bound-distance
            if gl.local2global[node]  in gl.border_node:
                for d in gl.msg_through_node_distance[gl.local2global[node] ]:
                    if d!=gl.worker_id:
                        self.out_messages[d].append((gl.local2global[node],distance))
            if distance>0:
                for v in Dgraph.vertex(node).out_neighbors():
                    que.put((int(v),distance-1))
                for v in Dgraph.vertex(node).in_neighbors():
                    que.put((int(v),distance-1))
        # if root in gl.inner_node:
        #     print "first",gl.worker_id,self.result_node
        self.send_messages()

    def incEval(self,dgraph,root,bound):
        que = queue.Queue()
        for i in xrange(0,gl.worker_num):
            for triple in self.in_messages[i]:
                t=(gl.global2local[triple[0]],triple[1])
                que.put(t)
        self.clear_messages()
        while not que.empty():
            node,distance =que.get()
            # if node in self.result_node:
            #     continue
            if node in self.result_node and bound-distance>self.result_dist[node]:
                continue
            self.result_node.add(node)
            self.result_dist[node]=bound-distance
            if gl.local2global[node] in gl.border_node:
                for d in gl.msg_through_node_distance[gl.local2global[node]]:
                    if d!=gl.worker_id:
                        self.out_messages[d].append((gl.local2global[node],distance))
            if distance>0:
                for v in dgraph.vertex(node).out_neighbors():
                    que.put((int(v),distance-1))
                for v in dgraph.vertex(node).in_neighbors():
                    que.put((int(v),distance-1))
        self.send_messages()

    def set_is_same_set(self,set1,set2):
        if (len(set1)!=len(set2)):
            return False
        for v in set1:
            if v not in set2:
                return False
        return True

    def get_all_result(self):
        global_all_node_set=set()
        self.result_node=set(gl.local2global[v] for v in self.result_node)
        all_result = None
        if gl.worker_id == 0:
            all_result=gl.comm.gather(self.result_node, root=0)
            for s in all_result:
                global_all_node_set|=s
        else:
            gl.comm.gather(self.result_node, root=0)
        return global_all_node_set

    def bfs_paraller(self,Dgraph,root,bound):
        # paraller_result =set()
        self.pEval(Dgraph,root,bound)
        gl.comm.Barrier()
        while(self.is_continue()):
                self.incEval(Dgraph,root,bound)
                gl.comm.Barrier()
        gl.comm.Barrier()

    def run(self,Dgraph,root_set,bound):
        self.bfs_paraller(Dgraph,root_set,bound)
        r =self.get_all_result()
        return r



if __name__ == "__main__":
    gl.comm = MPI.COMM_WORLD
    gl.worker_id = gl.comm.Get_rank()
    gl.worker_num = gl.comm.Get_size()
    Dgraph = gt.load_graph("Dgraph.xml.gz")
    if gl.worker_id ==0:
        gl.partion_dgraph("Dgraph.xml.gz")
    gl.id2block = gl.comm.bcast(gl.id2block if gl.worker_id == 0 else None, root=0)


    gl.comm.Barrier()
    gl.LocalGraph = gl.load_local_graph(Dgraph)
    # # print type(gl.LocalGraph)
    print gl.LocalGraph.num_vertices(),len(gl.inner_node)
    index = 1
    while index<=300:
        root=0
        bound=0
        if gl.worker_id==0:
            bound=random.randint(1,4)
            root =random.randint(0,Dgraph.num_vertices()-1-bound)

        root = gl.comm.bcast(root if gl.worker_id == 0 else None, root=0)
        bound = gl.comm.bcast(bound if gl.worker_id == 0 else None, root=0)
        # print "info",gl.worker_id,root,bound
        # root,bound=(1,6)
        root_set=set(i for i in xrange(root,root+bound))
        dwork=BFS()
        paraller_result =dwork.run(gl.LocalGraph,root_set,bound)
        if gl.worker_id ==0:
            direct_result =bfss.find_area_by_node_set(Dgraph,root_set,bound)
            # print paraller_result,direct_result
            # print direct_result.difference(paraller_result)
            print index,root,bound,dwork.set_is_same_set(paraller_result,direct_result)
        index+=1