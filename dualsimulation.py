import graph_generator as gr
import graph_tool.all as gt
import loadgraph as lg
import graph_generator as gr
_DEBUG = False

#Qgraph = gt.load_graph("Qgraph.xml.gz")
#Dgraph = gt.load_graph("Dgraph.xml.gz")

def dual_sim_initialization(dgraph, qgraph, sim, initialized_sim, remove_pred, remove_succ):
    '''
    Initialize main data structures for simulation algorithm
    '''
#    global Qgraph
#    global Dgraph
    #pred_dgraph_vertices all vertices can be parent
    #succ_dgraph_vertices all vertices can be child
    pred_dgraph_vertices = set(v for v in dgraph.vertices() if v.out_degree() != 0)
    succ_dgraph_vertices = set(v for v in dgraph.vertices() if v.in_degree() != 0)
    #initial sim set
    for u in qgraph.vertices():
        # Initialize prevsim[u]
#        prevsim[u] = set(dgraph.vertices())

        # Initialize sim[u] if it hasn't been initialized
        if initialized_sim == False:
            if u.out_degree() == 0 and u.in_degree() == 0:
                sim[u] = set(v for v in dgraph.vertices() if qgraph.vertex_properties["label"][u] == dgraph.vertex_properties["label"][v])
            elif u.out_degree() != 0 and u.in_degree() == 0:
                sim[u] = set(v for v in dgraph.vertices() if qgraph.vertex_properties["label"][u] == dgraph.vertex_properties["label"][v] and v.out_degree() != 0)
            elif u.out_degree() ==0 and u.in_degree() != 0:
                sim[u] = set(v for v in dgraph.vertices() if qgraph.vertex_properties["label"][u] == dgraph.vertex_properties["label"][v] and v.in_degree() != 0)
            else:
                sim[u] = set(v for v in dgraph.vertices() if qgraph.vertex_properties["label"][u] == dgraph.vertex_properties["label"][v] and v.out_degree() != 0 and v.in_degree() != 0)
       
        # Initialize remove_pred, remove_succ
        #remove_pred record the node can not match the parent of u
        #remove_succ record the node can not match the child of u
        remove_pred[u] = pred_dgraph_vertices.difference(set(v for w in sim[u] for v in w.in_neighbors()))
        remove_succ[u] = succ_dgraph_vertices.difference(set(v for w in sim[u] for v in w.out_neighbors()))
        # print u
        # for v in remove_pred[u]:
        #     print v,
        # print "\n","--"
        # for v in remove_succ[u]:
        #     print v,
        # print "===="

def remap_data_id(t_f, dgraph):
    '''
    Remap vertex id of nodes in dgraph (ball_view) to the range [0, num_vertex) via dict t_f (true_id --> fake_id);
    '''
    fid = 0
    for v in dgraph.vertices():
        t_f[int(v)] = fid
        fid = fid + 1
#        assert fid <= dgraph.num_vertices()
        


def dual_counter_initialization(dgraph, qgraph, sim_counter_post, sim_counter_pre, sim, t_f):
    '''
    Initialize the 2-dimensional list sim_counter_post and sim_counter_pre such that 
    sim_counter_post[v][u] = |post(v) \cap sim(u)|
    sim_counter_pre[v][u] = |pre(v) \cap sim(u)|
    '''
#    global Qgraph
#    global Dgraph
    for w in dgraph.vertices():
        for u in qgraph.vertices():
            sim_counter_post[t_f[int(w)]][int(u)] = len(set(w.out_neighbors()).intersection(sim[u]))
            sim_counter_pre[t_f[int(w)]][int(u)] = len(set(w.in_neighbors()).intersection(sim[u]))
 

def find_nonempty_remove(qgraph, remove_pred, remove_succ):
    '''
    Return (the first) u if remove_pred/succ[u] is not empty. Otherwise return None.
    '''
#    global Qgraph
    for u in qgraph.vertices():
        if len(remove_pred[u]) !=  0:
            return u
        if len(remove_succ[u]) != 0:
            return u

    return None


def update_sim_counter(sim_counter_post, sim_counter_pre, u, w, t_f):
    '''
    '''
    for wp in w.in_neighbors():
        if sim_counter_post[t_f[int(wp)]][int(u)] > 0:
            sim_counter_post[t_f[int(wp)]][int(u)] = sim_counter_post[t_f[int(wp)]][int(u)] - 1
    for ws in w.out_neighbours():
        if sim_counter_pre[t_f[int(ws)]][int(u)] > 0:
            sim_counter_pre[t_f[int(ws)]][int(u)] = sim_counter_pre[t_f[int(ws)]][int(u)] -1


def assert_check(setA, setB):
    '''
    Return True if vertices in setA have the same indices to those in setB
    '''
    for v in setA:
        flag = 1
        for u in setB:
            if int(v) == int(u):
                flag = 0
                break
        if flag == 1:
            return False
    return True


def dual_sim_refinement(dgraph, qgraph, sim, remove_pred, remove_succ):
    '''
    Decrementally refine sim untile all remove sets are all empty.
    '''
#    global Qgraph
#    global Dgraph
    #a counter used to speedup the refinement
    sim_counter_post = [[0 for col in xrange(0, qgraph.num_vertices())] for row in xrange(0, dgraph.num_vertices())]
    sim_counter_pre = [[0 for col in xrange(0, qgraph.num_vertices())] for row in xrange(0, dgraph.num_vertices())]
    t_f = {}
    #note that if the memory of your machine is relative small, you can (c)pickle it to harddisk in order to save memory

    #First remap vertex indices of nodes in dgraph to the range [0, dgraph.num_vertices())
    remap_data_id(t_f, dgraph)
    #Then initialize counters using the fake indices
    dual_counter_initialization(dgraph, qgraph, sim_counter_post, sim_counter_pre, sim, t_f)
    u = find_nonempty_remove(qgraph, remove_pred, remove_succ)
    while u != None:
        #a set of assertions
        #Assertion1: for all u, remove_pred[u] == pre(prevsim[u]) \ pre(sim[u])
        #Assertion2: for all u, remove_succ[u] == succ(prevsim[u]) \ succ(sim[u])
#        for ass_u in qgraph.vertices():
#            assert assert_check(remove_pred[ass_u], set(v for w in prevsim[ass_u] for v in w.in_neighbours()).difference(set(v for w in sim[ass_u] for v in w.in_neighbours()))) is True
#            assert assert_check(remove_succ[ass_u], set(v for w in prevsim[ass_u] for v in w.out_neighbours()).difference(set(v for w in sim[ass_u] for v in w.out_neighbours()))) is True

        if len(remove_pred[u]) != 0:
            for u_p in u.in_neighbors():
                for w_pred in remove_pred[u]:
                    if w_pred in sim[u_p]:
                        sim[u_p].discard(w_pred)
                        update_sim_counter(sim_counter_post, sim_counter_pre, u_p, w_pred, t_f)
                        for w_pp in w_pred.in_neighbors():
                            if sim_counter_post[t_f[int(w_pp)]][int(u_p)] == 0: 
                                #check whether post(w_pp) \cap sim[u_p] == \emptyset
                                remove_pred[u_p].add(w_pp)
                        for w_ps in w_pred.out_neighbors():
                            if sim_counter_pre[t_f[int(w_ps)]][int(u_p)] == 0:
                                #check whether pre(w_ps) \cap sim[u_p] == \emptyset
                                remove_succ[u_p].add(w_ps)
            if _DEBUG == True:
                import pdb
                pdb.set_trace()
#           prevsim[u] = set(v for v in sim[u]) #prevsim[u] is a hardcopy of sim[u]
            remove_pred[u].clear()

        if len(remove_succ[u]) != 0:
            for u_s in u.out_neighbors():
                for w_succ in remove_succ[u]:
                    if w_succ in sim[u_s]:
                        sim[u_s].discard(w_succ)
                        update_sim_counter(sim_counter_post, sim_counter_pre, u_s, w_succ, t_f)
                        for w_sp in w_succ.in_neighbors():
                            if sim_counter_post[t_f[int(w_sp)]][int(u_s)] == 0:
                                #check whether post(w_sp) \cap sim[u_s] == \emptyset
                                remove_pred[u_s].add(w_sp)
                        for w_ss in w_succ.out_neighbors():
                            if sim_counter_pre[t_f[int(w_ss)]][int(u_s)] == 0:
                                #check whether pre(w_ss) \cap sim[u_s] == \emptyset
                                remove_succ[u_s].add(w_ss)
            if _DEBUG == True:
                import pdb
                pdb.set_trace()
#            prevsim[u] = set(v for v in sim[u]) #prevsim[u] is a hardcopy of sim[u]
            remove_succ[u].clear()

        u = find_nonempty_remove(qgraph, remove_pred, remove_succ)


def match_check(qgraph, sim):
    '''
    Check whether sim is a matching relation.
    '''
    for u in qgraph.vertices():
        if len(sim[u]) == 0:
            return False

    return True

def dual_sim_output(qgraph, sim):
    '''
    Output the matching relation if exists
    '''
#    global Qgraph
    if match_check(qgraph, sim) == True: 
#        print 'return sim=%s' % sim
        return sim
    else:
#        print 'return None'
        return None



def dual_simulation(qgraph, dgraph, sim, initialized_sim):
    '''
    Dual simulation algorithm returns the set of dual-simulation relations of Dgraph for Qgraph
    '''
#    prevsim = {}
    remove_pred = {}
    remove_succ = {}

    dual_sim_initialization(dgraph, qgraph, sim, initialized_sim, remove_pred, remove_succ)
    dual_sim_refinement(dgraph, qgraph, sim, remove_pred, remove_succ)
    return dual_sim_output(qgraph, sim)


if __name__ == "__main__":
    # Qgraph = gt.Graph()
    Qgraph.add_vertex(5)
    Qgraph.vertex_properties["label"] = Qgraph.new_vertex_property("string")
    Qgraph.vertex_properties["label"][0] ="0"
    Qgraph.vertex_properties["label"][1] ="1"
    Qgraph.vertex_properties["label"][2] ="2"
    Qgraph.vertex_properties["label"][3] ="3"
    Qgraph.vertex_properties["label"][3] ="4"
    Qgraph.add_edge(0,2)
    Qgraph.add_edge(0,1)
    Qgraph.add_edge(1,4)
    Qgraph.add_edge(1,4)
    Qgraph.add_edge(2,3)
    Qgraph.add_edge(3,4)
    # global_sim = {}
    Qgraph = gt.load_graph("twitter/Qgraph.xml.gz")
    Dgraph = lg.load_graph_from_grapeformat("twitter/twitter.v","twitter/twitter.e")
    global_sim = {}
    dual_simulation(Qgraph, Dgraph, global_sim, False)
    for key in global_sim:
        print key,"--->",
        for u in global_sim[key]:
            print u,
        print "\n"
    # i =1;
    # while (i<=100):
    #     Qgraph = gr.generate_qgraph_by_Dgraph(Dgraph,5)
    #     gt.graph_draw(Qgraph, vertex_text=Qgraph.vertex_properties["label"], output = "twitter"+"/"+"Qgraph"+str(i)+".png")
    #     Qgraph.save("twitter"+"/"+"Qgraph"+str(i)+".xml.gz")
    #     i +=1
    # dual_simulation(Qgraph, Dgraph, global_sim, False)
    # for key in global_sim:
    #     print key,"--->",
    #     for u in global_sim[key]:
    #         print u,
    #     print "\n"

