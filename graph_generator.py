#!/usr/bin/python
# -*- coding: UTF-8 -*-
import glob
import os
import graph_tool.all as gt
from itertools import izip
from numpy.random import randint
import numpy as np # used for matrix operation in a high speed
#import fpconst #inf, -inf, nom
import sys #print sys.maxint
import random
import Queue
def generate_rand_graphs(n=1000, a=1.20, l=6):
    '''
    @Attention: generated graph has "int" vertex_properties, different from graphs create by create_graph.py which has "string" vertex_properties. 
    Generate synthetic graphs
    @param n: number of vertices
    @param a: n^a is the number of edges (usually we use a in [1.05, 1.35])
    @param l: number of different labels [0,l)
    '''
    vertex_labels="abcdefghijklmnopqrstuvwxyz"
    # vertex_labels="abcd"
    g = gt.Graph()
    g.add_vertex(n)
    for c in xrange(0, int(n**a)):
        a = random.randint(0, n-1)
        b = random.randint(0, n-1)
        eg=g.edge(g.vertex(a), g.vertex(b))
        if a!=b and  not eg:
            g.add_edge(g.vertex(a), g.vertex(b))
    g.vertex_properties["label"] = g.new_vertex_property("string")
    for v in g.vertices():
        g.vertex_properties["label"][v] = vertex_labels[random.randint(0,l)]
    return g
def generate_connect_graphs_by_Dgraph(Dgraph,n=10):
    '''
    Generate graph by a given graph
    '''
    g = gt.Graph()
    g.add_vertex(n)
    g.vertex_properties["label"] = g.new_vertex_property("string")
    Dgraph_vertex_num=len(Dgraph.get_vertices())
    vertex_list=[]
    if(n>Dgraph_vertex_num):
        n=Dgraph_vertex_num
        return Dgraph
    continue_select_root=True
    while continue_select_root:
        vset=Queue.Queue()
        vertex_list=[]
        root = Dgraph.vertex(random.randint(0, Dgraph_vertex_num-1))
        vset.put(root)
        vertex_list.append(root)
        while not vset.empty():
            u=vset.get()
            # Avoid too many nodes with one node as the center
            tmpnum=0
            for v1 in u.out_neighbors():
                if v1 not in vertex_list and len(vertex_list)<n and tmpnum<2:
                    vertex_list.append(v1)
                    vset.put(v1)
                    tmpnum += 1
# Avoid too many nodes with one node as the center
                # for v2 in u.in_neighbors():
                #     if v2 not in vertex_list and len(vertex_list)<n:
                #         vertex_list.append(v2)
                #         vset.put(v2)
        if len(vertex_list)==n:
            continue_select_root = False
    for i in xrange(0,n):
       g.vertex_properties["label"][g.vertex(i)] = Dgraph.vertex_properties["label"][vertex_list[i]]
    for i in xrange(0,n):
        for j in xrange(0,n):
            eg=Dgraph.edge(vertex_list[i],vertex_list[j])
            if eg:
                g.add_edge(g.vertex(i),g.vertex(j))
    return g

def generate_rand_Qgraph():
    '''
    Generate query graph
    '''
    Qgraph = gt.Graph()
    Qgraph = generate_rand_graphs(6)
    #print 'synthetic_qgraph: %s' %Qgraph
    return Qgraph

def generate_rand_Dgraph():
    '''
    Generate data graph
    '''
    Dgraph = gt.Graph()
    Dgraph = generate_rand_graphs(100)
    #print 'synthetic_dgraph: %s' %Dgraph
    return Dgraph

def generate_view_by_Qgraph(Qgraph,n=2):
    # n=random.randint(3,5)
    g=generate_connect_graphs_by_Dgraph(Qgraph,n)
    return g

def generate_qgraph_by_Dgraph(Dgraph,n=3):
    # n=random.randint(8,12)
    g=generate_connect_graphs_by_Dgraph(Dgraph,n)
    return g

def generate_view_by_Dgraph(Dgraph,n=5):
    # n=random.randint(2,5)
    g=generate_connect_graphs_by_Dgraph(Dgraph,n)
    return g
# def generate_qgraph_by_Viewlist(viewlist,n=10):
#     n=random.randint(8,12)

def output_all_graph_data(Qgraph,Dgraph,viewlist,num):
    curdir = os.getcwd()
    path=curdir+"/data"+str(num)
    isExists=os.path.exists(path)
    if not isExists:
        os.makedirs(path)
    gt.graph_draw(Qgraph, vertex_text=Qgraph.vertex_properties["label"], output = path+"/"+"QueryGraph.png")
    Qgraph.save(path+"/"+"Qgraph.xml.gz")
    gt.graph_draw(Dgraph, vertex_text=Dgraph.vertex_properties["label"], output = path+"/"+"DataGraph.png")
    Dgraph.save(path+"/"+"Dgraph.xml.gz")
    for i in xrange(0,len(viewlist)):
        gt.graph_draw(viewlist[i], vertex_text=viewlist[i].vertex_properties["label"], output = path+"/"+"view"+str(i+1)+".png")
        viewlist[i].save(path+"/"+"view"+str(i+1)+".xml.gz")

def generate_graph_all_rand():
    Qgraph = generate_rand_Qgraph()
    Dgraph = generate_rand_Dgraph()
    return Qgraph,Dgraph

def generate_graph_all_by_graph():
    Dgraph = generate_rand_Dgraph()
    Qgraph = generate_qgraph_by_Dgraph(Dgraph)
    viewlist=[]
    for i in xrange(0,5):
        viewlist.append(generate_view_by_Qgraph(Qgraph))
    return Qgraph,Dgraph,viewlist
if __name__ == "__main__":
    for i in xrange(0,5):
        Qgraph,Dgraph,viewlist=generate_graph_all_by_graph()
        output_all_graph_data(Qgraph,Dgraph,viewlist,i)




