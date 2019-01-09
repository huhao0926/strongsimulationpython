__author__ = 'Administrator'
from mpi4py import MPI
import graph_tool.all as gt
import numpy as np
import Queue as queue
import globalvar as gl
import numpy as np
import loadgraph as lg
import time
 # mpiexec -np 3 python BFS.py
class BfsWorker:
    result_node =set()
    out_messages =[]
    in_messages =[]
    con_run =0
    def __init__(self):
        self.result_node=set()
        self.out_messages =[]
        self.in_messages =[]
        con_run =0
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

    def pEval(self,dgraph,root,bound):
        que = queue.Queue()
        if root in gl.inner_node:
            que.put((bound,root))
        while not que.empty():
            tiple =que.get()
            distance = tiple[0]
            node = tiple[1]
            if node in self.result_node:
                continue
            if node in gl.border_node:
                for d in gl.msg_through_node_distance[node]:
                    if d!=gl.worker_id:
                        self.out_messages[d].append((distance,node))
            if distance>0:
                for v in dgraph.vertex(node).out_neighbors():
                    que.put((distance-1,int(v)))
                for v in dgraph.vertex(node).in_neighbors():
                    que.put((distance-1,int(v)))
        self.send_messages()

    def incEval(self,dgraph,root,bound):
        que = queue.Queue()
        d ={}
        for i in xrange(0,gl.worker_num):
            for triple in self.in_messages[i]:
                que.put(triple)
        #         if d.has_key(triple[1]):
        #             if triple[0]>d[triple[1]]:
        #                 d[triple[1]] =triple[0]
        #         else:
        #             d[triple[1]] = triple[0]
        # for key in d.keys():
        #     que.put((d[key],key))
        self.clear_messages()
        while not que.empty():
            triple =que.get()
            distance = triple[0]
            node = triple[1]
            # if node in self.result_node:
            #     continue
            self.result_node.add(node)
            if node in gl.border_node:
                for d in gl.msg_through_node_distance[node]:
                    self.out_messages[d].append((distance,node))
            if distance>0:
                for v in dgraph.vertex(node).out_neighbors():
                    que.put((distance-1,int(v)))
                for v in dgraph.vertex(node).in_neighbors():
                    que.put((distance-1,int(v)))
        self.send_messages()

    def set_is_same_set(self,set1,set2):
        if (len(set1)!=len(set2)):
            return False
        for v in set1:
            if v not in set2:
                return False
        return True

    def bfs_paraller(self,Dgraph,root,bound):
        # paraller_result =set()
        self.pEval(Dgraph,root,bound)
        gl.comm.Barrier()
        while(self.is_continue()):
                self.incEval(Dgraph,root,bound)
                gl.comm.Barrier()
        gl.comm.Barrier()


if __name__ == "__main__":
    gl.comm = MPI.COMM_WORLD
    gl.worker_id = gl.comm.Get_rank()
    gl.worker_num = gl.comm.Get_size()
    Dgraph = gt.load_graph("Dgraph.xml.gz")
    if gl.worker_id ==0:
        gl.partion_dgraph("Dgraph.xml.gz")
    gl.id2block = gl.comm.bcast(gl.id2block if gl.worker_id == 0 else None, root=0)

    # mpiexec -np 11 python BFS.py
    # Dgraph = lg.load_graph_from_grapeformat("twitter/twitter.v","twitter/twitter.e")
    # if gl.worker_id ==0:
    #     gl.initial_id2block("twitter/twitter.r")
    # gl.id2block = gl.comm.bcast(gl.id2block if gl.worker_id == 0 else None, root=0)
    gl.comm.Barrier()
    gl.LocalGraph = gl.load_local_graph(Dgraph)
    # # print type(gl.LocalGraph)
    # test_set =[(1,1),(2,1),(10,1),(4,2),(5,3),(6,3),(10,2),(100,3)]
    test_set =[(5,3)]
    s=time.time()
    for index in xrange(0,500):
        root =index
        bound =3
        dwork=BfsWorker()
        paraller_result =dwork.run(gl.LocalGraph,root,bound)
        if gl.worker_id ==0:
            direct_result =set()
            dist = gt.shortest_distance(Dgraph, root, None, None, None,None,False)
            for i in xrange(0, len(dist.a)):
                if dist.a[i] <= bound:
                    direct_result.add(i)
            # print direct_result.difference(paraller_result)
            # print index,dwork.set_is_same_set(paraller_result,direct_result)
    e =time.time()
    print e-s