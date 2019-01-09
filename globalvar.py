__author__ = 'Administrator'
import graph_tool.all as gt
from mpi4py import MPI
import networkx as nx
import metis
comm = None
worker_id = 0
worker_num = 0
outer_node = set()
border_node=set()
inner_node=set()
id2block ={}
msg_through_node_distance={}
LocalGraph=gt.Graph()
global2local ={}
local2global ={}
def load_local_graph(Dgraph):
    vmatch_set = set()
    ematch_set = set()
    for v in Dgraph.vertices():
        if id2block[int(v)] == worker_id :
            inner_node.add(int(v))
            vmatch_set.add(int(v))
    for e in Dgraph.edges():
        if id2block[int(e.source())] == worker_id and id2block[int(e.target())] == worker_id:
            ematch_set.add(e)
        elif id2block[int(e.source())] == worker_id:
            vmatch_set.add(int(e.target()))
            border_node.add(int(e.source()))
            outer_node.add(int(e.target()))
            ematch_set.add(e)
            id_to = id2block[int(e.target())]
            if msg_through_node_distance.has_key(int(e.source())):
                msg_through_node_distance[int(e.source())].add(id_to)
            else:
                msg_through_node_distance[int(e.source())]= set([id_to])
        elif id2block[int(e.target())] == worker_id:
            vmatch_set.add(int(e.source()))
            outer_node.add(int(e.source()))
            border_node.add(int(e.target()))
            ematch_set.add(e)
            id_to = id2block[int(e.source())]
            if msg_through_node_distance.has_key(int(e.target())):
                msg_through_node_distance[int(e.target())].add(id_to)
            else:
                msg_through_node_distance[int(e.target())] = set([id_to])
    LocalGraph.add_vertex(len(vmatch_set))
    LocalGraph.vertex_properties["label"] = LocalGraph.new_vertex_property("string")
    for idv,v in enumerate(vmatch_set):
        local2global[idv] = int(v)
        global2local[int(v)] = idv
        LocalGraph.vertex_properties["label"][idv] = Dgraph.vertex_properties["label"][v]
    for e in ematch_set:
        LocalGraph.add_edge(global2local[int(e.source())],global2local[int(e.target())])
    return LocalGraph

def partion_dgraph(dgraph_filename):
        partition_graph = nx.read_graphml(dgraph_filename)
        if worker_num != 1:
            (edgecuts, parts) = metis.part_graph(partition_graph, worker_num)
            for i in xrange(0,partition_graph.number_of_nodes()):
                id2block[i] = parts[i]
        else:
            for i in xrange(0,partition_graph.number_of_nodes()):
                id2block[i] = 0

def initial_id2block(part_file):
        try:
            vf = open(part_file, 'r')
        except IOError:
            print "Error: file %s not exists" % part_file
        v_lines=vf.readlines()
        try:
            for lines in v_lines:
                lines = lines.strip('\n')
                line=lines.split('\t')
                if(len(line)!=2):
                    break
                node_id=int(line[0])-1
                block_id = int(line[1])
                id2block[node_id] = block_id
        finally:
            vf.close()
