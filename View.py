__author__ = 'hh'
from time import time
import Queue
import sys
import glob
import os
import graph_tool.all as gt
import dualsimulation as ds

class partial(object):
    ans={}
    reverseMap={}

class Contain(object):
    P=gt.Graph()
    Vset=[]
    Vset_dgraph_result =[]
    MGSet =[]
    contain=[]
    dual_sim_list = []
    def __init__(self,P,Vset):
        self.P = P
        self.Vset = Vset
        self.MGSet = []
        self.contain = []
        self.Vset_dgraph_result =[]
        dual_sim_list = []

    def simTran(self,Pgraph,Dgraph,simMatchResult):
        ans={}
        for e in Pgraph.edges():
            eSet=set()
            source=e.source()
            target=e.target()
            simsource=simMatchResult[source]
            simtarget=simMatchResult[target]
            if(len(simsource)==0 or len(simtarget)==0):
                eSet.clear()
                ans.clear()
                break
            for sourcev in simsource:
                for targetv in simtarget:
                    eg=Dgraph.edge(sourcev,targetv)
                    if eg:
                        eSet.add(eg)
            ans[e]=eSet
        return ans

    def containCheck(self):
        ans=False
        eSet=set()
        counter=len(self.Vset)
        for i in xrange(0,counter):
            sim={}
            ds.dual_simulation(self.Vset[i],self.P,sim,False)
            part=self.simTran(self.Vset[i],self.P,sim)
            self.dual_sim_list.append(part)
            MG=part
            MGEdge=set()
            if MG:
                for e in MG.keys():
                    eset=MG[e]
                    if(eset):
                        MGEdge=(MGEdge | eset)
                self.contain.append(i)
            self.MGSet.append(MGEdge)
            if(MGEdge):
                eSet|=MGEdge
        if(set(self.P.edges()).issubset(eSet)):
            ans=True
        return ans

    def minContain(self):
        tmp=set()
        rem=set()
        ans=[]
        rem|=set(self.P.edges())
        max=0
        pos=set()
        while rem:
            max=0
            for i in xrange(0,len(self.MGSet)):
                eset=self.MGSet[i]
                if (eset and not (i in ans)):
                    tmp|=eset
                    tmp&=rem
                    if(max<len(tmp)):
                        max=len(tmp)
                        pos=eset
                    tmp.clear()
            if(pos):
                rem-=pos
                ans.append(self.MGSet.index(pos))
                # print len(self.MGSet),self.MGSet.index(pos)
            tmp.clear()
        return ans

    def clear_all_paramater(self):
        self.P=gt.Graph()
        self.Vset=[]
        self.Vset_dgraph_result =[]
        self.MGSet =[]
        self.contain=[]
        self.dual_sim_list= []
    def test3(self,datasetnum):
        curdir = os.getcwd()
        index=1
        while index<=datasetnum:
            # print "data "+str(index)+" test---------------------->:1"
            path=curdir+"/ci6/data"+str(index)
            isExists=os.path.exists(path)
            if not isExists:
                index += 1
                continue
            Qgraph = gt.load_graph(path+"/Qgraph.xml.gz")
            self.P = gt.Graph(Qgraph)
            # Dgraph = gt.load_graph(path+"/Dgraph.xml.gz")
            for i in xrange(1,6):
                view = gt.load_graph(path+"/view"+str(i)+".xml.gz")
                self.Vset.append(view)
            if self.containCheck():
                mincontain = self.minContain()
                print mincontain

            self.clear_all_paramater()
            index +=1
if __name__ == "__main__":
    containclass=Contain()
    containclass.test3(200)




