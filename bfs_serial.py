__author__ = 'Administrator'
import graph_tool.all as gt
import time
def find_area_by_node_set(Dgraph,node_set,d_Q):
    affected_center_nodes=set()
    affected_ball_nodes=set()
    d_tmp=0
    last_floor =set()
    for v in node_set:
        last_floor.add(Dgraph.vertex(int(v)))
    while len(last_floor)!=0 and d_tmp <=d_Q:
        affected_ball_nodes|=last_floor
        s1=time.time()
        next_floor=set()
        for node in last_floor:
            for out_node in set(node.out_neighbors()):
                if out_node not in affected_ball_nodes:
                    affected_ball_nodes.add(out_node)
                    next_floor.add(out_node)
            for in_node in set(node.in_neighbors()):
                if in_node not in affected_ball_nodes:
                    affected_ball_nodes.add(in_node)
                    next_floor.add(in_node)
        e1=time.time()
        if d_tmp<=d_Q:
            affected_center_nodes|=last_floor
        # print "calculate one floor time",e1-s1
        last_floor=set(v for v in next_floor)
        d_tmp+=1
    return affected_center_nodes