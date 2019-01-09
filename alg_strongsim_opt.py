#!/usr/bin/python
# -*- coding: utf-8 -*-

import time
import dualsimulation as ds
import graph_tool.all as gt
import loadgraph as lg
#Qgraph = gt.load_graph("Qgraph.xml.gz")
#Dgraph = gt.load_graph("Dgraph.xml.gz")

def cal_diameter_qgraph(qgraph):
    '''
    Calculate the diameter of qgraph
    '''
    #ug=gt.Graph(qgraph)
    #ug.set_directed(False)
    temp_dia = 0
    max_dia = qgraph.num_vertices()-1
    for u in qgraph.vertices():
        dist = gt.shortest_distance(qgraph, u, None, None, None,None,False)
        for i in xrange(0, len(dist.a)):
            if dist.a[i] <= max_dia and temp_dia < dist.a[i]:
                temp_dia = dist.a[i]
    return temp_dia

def create_ball_view(w, d_Q,Dgraph):
    '''
    Create a ball [w, d_Q] view on top of data graph
    '''
    #global Dgraph
    dist = gt.shortest_distance(Dgraph, w, None, None, None,None,False)
    ball_view = gt.GraphView(Dgraph, vfilt = lambda v: dist.a[int(v)] <= d_Q)
    # print "ball------------------------->"
    # for e in ball_view.edges():
    #     print ball_view.vertex_properties["label"][e.source()],"-->",ball_view.vertex_properties["label"][e.target()]

    return ball_view

def remove_equiv_nodes(qgraph, S_Q):
    '''
    Remove equivalent nodes from qgraph.
    u is equivalent to v iff v\in S_Q[u] and u\in S_Q[v]
    '''
    mark = [0 for col in xrange(0, qgraph.num_vertices())]
    for u in qgraph.vertices():
        for v in qgraph.vertices():
            if mark[int(u)] == 0 and mark[int(v)] == 0 and int(u) != int(v) and u in S_Q[v] and v in S_Q[u]:
                for v_p in v.in_neighbors():
                    eg_in = qgraph.edge(v_p,u)
                    if not eg_in:
                        qgraph.add_edge(v_p, u)
                for v_s in v.out_neighbors():
                    eg_out = qgraph.edge(u,v_s)
                    if not eg_out:
                        qgraph.add_edge(u, v_s)
                mark[int(v)] = 1
    qgraph_view = gt.GraphView(qgraph, vfilt = lambda v: mark[int(v)] == 0)
    gt.remove_parallel_edges(qgraph_view)
    qgraph_view.purge_vertices()
    return qgraph_view

def query_minimization(qgraph):
    '''
    Minimize Qgraph
    '''
    S_Q = {}
    ds.dual_simulation(qgraph, qgraph, S_Q, False)
    return remove_equiv_nodes(qgraph, S_Q)

def valid_sim_w(sim, w, qgraph):
    '''
    Check whether the matching relation sim contains w or not
    '''
    if sim == None:
        return False
    uid = -1
    for u in qgraph.vertices():
        if w in sim[u]:
            uid = int(u)
            break
    if uid == -1:
        return False

    return True

def extract_max_pg(ball_view, qgraph, w, S_w, d_Q):
    '''
    Extract the maximum perfect graph of qgraph in the ball.
    '''
    if valid_sim_w(S_w, w, qgraph) == False:
        return None
    vertex_matchset = set(v for u in qgraph.vertices() for v in S_w[u])
    # edge_matchset = set(e for e in ball_view.edges() for (u, v) in qgraph.edges() if e.source() in S_w[u] and e.target() in S_w[v])
    edge_matchset = set()
    for e in qgraph.edges():
        source=e.source()
        target=e.target()
        for sim_v1 in S_w[source]:
            for sim_v2 in S_w[target]:
                eg=ball_view.edge(sim_v1,sim_v2)
                if eg:
                    edge_matchset.add(eg)
    pg_view = gt.GraphView(ball_view, vfilt = lambda v: v in vertex_matchset, efilt = lambda e: e in edge_matchset)
    dist = gt.shortest_distance(pg_view, w,  None, None, None,None,False)
    maxPGC = gt.GraphView(pg_view, vfilt = lambda v: dist.a[int(v)] <= d_Q)
    for u in qgraph.vertices():
        S_w[u]=set(v for v in maxPGC.vertices() if v in S_w[u])
        if len(S_w[u])==0:
            maxPGC=None
    return maxPGC

def output_max_pgs(max_perfect_subgraphs):
    '''
    Out put maximum perfect subgraphs.
    '''
    if len(max_perfect_subgraphs) == 0:
        print 'There is no perfect subgraph!'
    for maxPG in max_perfect_subgraphs:
        #print '>>>>>One PG is %s' % maxPG
        for e in maxPG.edges():
            print e,
        print "\n"

    return max_perfect_subgraphs

def is_the_same_ball(g1, g2):
    '''
    Check whether g1 and g2 are the "same".
    '''
    #g1 is contained in g2 return 1
    #g2 is contained in g1 return 2
    #g1,g2 can not contain each other return 0
    g1_vertex_num=len(g1.get_vertices())
    g2_vertex_num=len(g2.get_vertices())

    # if (g1_vertex_num ==g2_vertex_num):
    #     print "======"
    #     for e in  g1.edges():
    #         print e
    #     print "---"
    #     for e in g2.edges():
    #         print e
    #     print "====="
    if g1_vertex_num<=g2_vertex_num:
        for v1 in g1.vertices():
            if v1 not in g2.vertices():
                return 0
        # for e1 in g1.edges():
        #     if e1 not in g2.edges():
        #         return 0
        return 1
    else:
        for v2 in g2.vertices():
            if v2 not in g1.vertices():
                return 0
        # for e2 in g2.edges():
        #     if e2 not in g1.edges():
        #         return 0
        return 2


def add_new_pg(G_s, max_perfect_subgraphs):
    '''
    Check whether G_s is a new PG
    '''
    if G_s == None:
        return False
    removeset=set()
    for g in max_perfect_subgraphs:
        num_type=is_the_same_ball(g, G_s)
        if num_type ==2:
            return False
        elif num_type==1:
            removeset.add(g)
    max_perfect_subgraphs.add(G_s)
    max_perfect_subgraphs -= removeset
    return True
def add_new_pg_and_simset(G_s, max_perfect_subgraphs,S_w,balllistsim):
    '''
    Check whether G_s is a new PG
    '''
    if G_s == None:
        return False
    refresh_max_perfect_subgraphs=[]
    refresh_balllistsim=[]
    for i in xrange(0,len(max_perfect_subgraphs)):
        num_type=is_the_same_ball(max_perfect_subgraphs[i],G_s)
        if num_type==2:
            return max_perfect_subgraphs,balllistsim
        elif num_type==0:
            refresh_max_perfect_subgraphs.append(max_perfect_subgraphs[i])
            refresh_balllistsim.append((balllistsim[i]))
    refresh_max_perfect_subgraphs.append(G_s)
    refresh_balllistsim.append(S_w)
    return refresh_max_perfect_subgraphs,refresh_balllistsim

def project_sim(ball, sim,Qgraph):
    '''
    Project global matching relation sim to ball and return the projected relation.
    '''
    #global Qgraph
    ball_sim = {}
    for u in Qgraph.vertices():
        ball_sim[u] = set(v for v in ball.vertices() if v in sim[u])
    return ball_sim

def connectivity_prune(ball, w, sim, d_Q,Qgraph):
    '''
    Use the matching relation sim to prune the ball
    '''
    #输出的结果是 以w为球心，d_Q为半径的球，满足数据图dual simulation约束的球
    #if write as follow then the result will be wrong
    # tmp = set(v for u in Qgraph.vertices() for v in sim[u] if v in ball.vertices())
    # view_1 = gt.GraphView(ball, vfilt = lambda v: v in tmp)
    vertex_matchset = set(v for u in Qgraph.vertices() for v in sim[u] if v in ball.vertices())
    edge_matchset = set()
    for e in Qgraph.edges():
        source=e.source()
        target=e.target()
        for sim_v1 in sim[source]:
            for sim_v2 in sim[target]:
                eg=ball.edge(sim_v1,sim_v2)
                if eg:
                    edge_matchset.add(eg)
    view_1 = gt.GraphView(ball, vfilt = lambda v: v in vertex_matchset, efilt = lambda e: e in edge_matchset)
    dist = gt.shortest_distance(view_1, w, None, None, None,None,False)
    view_2 = gt.GraphView(view_1, vfilt = lambda v: dist.a[int(v)] <= d_Q)
    return view_2

def remap_counter_id(ftf, ball):
    '''
    Remap vertex id
    '''
    fid = 0
    for v in ball.vertices():
        ftf[int(v)] = fid
        fid += 1
#        assert fid <= ball.num_vertices()
def ss_counter_initialization(counter_ps, counter_ss, ball, qgraph, sim, ftf):
    '''
    Counter initialization
    '''
    #为父节点子节点数量 数组统计赋值
    for v in ball.vertices():
        for u in qgraph.vertices():
            counter_ps[ftf[int(v)]][int(u)] = len(set(vp for vp in v.in_neighbors() if vp in sim[u]))#数据图中 和 查询图u label相同 且是数据图v 的父节点的数量
            counter_ss[ftf[int(v)]][int(u)] = len(set(vs for vs in v.out_neighbors() if vs in sim[u]))#数据图中 和 查询图u label相同 且是数据图v的子节点的数量

def is_border_node(v, ball, w, d_Q):
    '''
    Judge whether v is a border node of ball[w]
    '''
    dist = gt.shortest_distance(ball, w, None, None, None,None,False)
    if dist.a[int(v)] == d_Q:
        return True
    return False

def push_phase(counter_ps, counter_ss, ball, qgraph, sim, filter_set, w, ftf, d_Q):
    '''
    Push all possible fake matches that involve the border nodes
    '''
    #推动阶段
    for u in qgraph.vertices():
        for v in sim[u]:
            if is_border_node(v, ball, w, d_Q) == True:
                for u_s in u.out_neighbors():
                    if counter_ss[ftf[int(v)]][int(u_s)] == 0:#数据图中 和 查询图u_s label相同 且是数据图v的子节点的数量
                        filter_set.add((u,v))
                        break
                for u_p in u.in_neighbors():
                    if counter_ps[ftf[int(v)]][int(u_p)] == 0:
                        filter_set.add((u,v))
                        break

def update_counter(counter_ps, counter_ss, ball, u, v, ftf):
    '''
    Update counters
    '''
    for vp in v.in_neighbors():
#        assert vp in ball.vertices()
        if counter_ss[ftf[int(vp)]][int(u)] > 0:
            counter_ss[ftf[int(vp)]][int(u)] -= 1

    for vs in v.out_neighbors():
#        assert vs in ball.vertices()
        if counter_ps[ftf[int(vs)]][int(u)] > 0:
            counter_ps[ftf[int(vs)]][int(u)] -= 1

def decremental_refine(counter_ps, counter_ss, ball, qgraph, S_w, filter_set, ftf):
    '''
    Decrementally refine S_w
    '''
    while len(filter_set) != 0:
        (u, v) = filter_set.pop()
        S_w[u].remove(v)
        update_counter(counter_ps, counter_ss, ball, u, v, ftf)
        for u_p in u.in_neighbors():
            for v_p in v.in_neighbors():
#                assert v_p in ball.vertices()
                if v_p in S_w[u_p]:
                    if counter_ss[ftf[int(v_p)]][int(u)] == 0:
                        filter_set.add((u_p, v_p))
        for u_s in u.out_neighbors():
            for v_s in v.out_neighbors():
#                assert v_s in ball.vertices()
                if v_s in S_w[u_s]:
                    if counter_ps[ftf[int(v_s)]][int(u)] == 0:
                        filter_set.add((u_s, v_s))

def dual_filter_match(refined_ball, qgraph, S_w, w, d_Q):
    '''
    dualFilter: refine the initial S_w to get the final matching relation
    '''
    filter_set = set()
    counter_ps = [[0 for col in xrange(0, qgraph.num_vertices())] for row in xrange(0, refined_ball.num_vertices())]
    counter_ss = [[0 for col in xrange(0, qgraph.num_vertices())] for row in xrange(0, refined_ball.num_vertices())]
    ftf = {}
    #将数据库节点id映射到数组id
    remap_counter_id(ftf, refined_ball)
    ss_counter_initialization(counter_ps, counter_ss, refined_ball, qgraph, S_w, ftf)
    push_phase(counter_ps, counter_ss, refined_ball, qgraph, S_w, filter_set, w, ftf, d_Q)
    decremental_refine(counter_ps, counter_ss, refined_ball, qgraph, S_w, filter_set, ftf)
    for u in qgraph.vertices():
        if len(S_w[u]) == 0:
            S_w = {}
            return S_w

    return S_w

def rename_sim(S_w, ball,Qgraph):
    '''
    Reconstruct S_w using vertices in ball
    '''
    #global Qgraph
    tmp = {}
    for u in Qgraph.vertices():
        tmp[u] = set()
        for v in ball.vertices():
            if v in S_w[u]:
                tmp[u].add(v)
    return tmp

def strong_simulation_sim(Qgraph,Dgraph):
    '''
    Enhanced strong simulation algorithm
    '''
    #global Dgraph
    #global Qgraph

    d_Q = cal_diameter_qgraph(Qgraph)
    max_perfect_subgraphs = []
    Qgraph = query_minimization(Qgraph)
    global_sim = {}
    global_sim=ds.dual_simulation(Qgraph, Dgraph, global_sim, False)
    balllistsim=[]
    for w in Dgraph.vertices():
        if valid_sim_w(global_sim, w, Qgraph) == True:
            ball_view = create_ball_view(w, d_Q,Dgraph)
            S_w = project_sim(ball_view, global_sim,Qgraph)
            refined_ball_view = connectivity_prune(ball_view, w, S_w, d_Q,Qgraph)
            S_w = rename_sim(S_w, refined_ball_view,Qgraph)
#            ds.dual_simulation(refined_ball_view, Qgraph, S_w, True)
            dual_filter_match(refined_ball_view, Qgraph, S_w, w, d_Q)
            G_s = extract_max_pg(refined_ball_view, Qgraph, w, S_w, d_Q)
            max_perfect_subgraphs,balllistsim = add_new_pg_and_simset(G_s, max_perfect_subgraphs,S_w,balllistsim)
            # print len(max_perfect_subgraphs)
     # output_max_pgs(max_perfect_subgraphs)
    return balllistsim

def strong_simulation_ball(Qgraph,Dgraph):
    '''
    Enhanced strong simulation algorithm
    '''
    #global Dgraph
    #global Qgraph
    d_Q = cal_diameter_qgraph(Qgraph)
    max_perfect_subgraphs = set()
    Qgraph = query_minimization(Qgraph)  #the graph is change here,but real input Qgraph is not
    global_sim = {}
    global_sim=ds.dual_simulation(Qgraph, Dgraph, global_sim, False)
    for w in Dgraph.vertices():
        if valid_sim_w(global_sim, w, Qgraph) == True:
            ball_view = create_ball_view(w, d_Q,Dgraph)
            S_w = project_sim(ball_view, global_sim,Qgraph)
            refined_ball_view = connectivity_prune(ball_view, w, S_w, d_Q,Qgraph)
            S_w = rename_sim(S_w, refined_ball_view,Qgraph)
#            ds.dual_simulation(refined_ball_view, Qgraph, S_w, True)
            dual_filter_match(refined_ball_view, Qgraph, S_w, w, d_Q)
            G_s = extract_max_pg(refined_ball_view, Qgraph, w, S_w, d_Q)
            add_new_pg(G_s, max_perfect_subgraphs)
    return max_perfect_subgraphs
    # return output_max_pgs(max_perfect_subgraphs)
    # return balllistsim

if __name__ == "__main__":
    v_file="twitter/twitter.v"
    e_file="twitter/twitter.e"
    s=time.time()
    Dgraph = lg.load_graph_from_grapeformat(v_file,e_file)
    e=time.time()
    print "load graph",e-s
    print Dgraph.num_vertices()
    print Dgraph.num_edges()
    # balllistsim=strong_simulation_ball(Qgraph,Dgraph)
    # for ball in balllistsim:
    #     print "result------------"
    #     for e in ball.edges():
    #         print e
    # print e1
    # print e2
    # v1=Dgraph.vertex(726)
    # v2=Dgraph.vertex(901)
    # v3=Dgraph.vertex(1325)
    # e1=Dgraph.edge(v1,v3)
    # e2=Dgraph.edge(v2,v3)
    # for e in Qgraph.edges():
    #     print e," ",Qgraph.vertex_properties["label"][e.source()],"-->",Qgraph.vertex_properties["label"][e.target()]


