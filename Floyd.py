__author__ = 'Administrator'
import graph_tool.all as gt
def calculate_shortest_distance(Dgraph):
    matix = [[0 if(col==row) else float('inf') for col in xrange(0, Dgraph.num_vertices())] for row in xrange(0, Dgraph.num_vertices())]
    for e in Dgraph.edges():
        matix[int(e.source())][int(e.target())]=1
    for k in xrange(0, Dgraph.num_vertices()):
        for i in xrange(0, Dgraph.num_vertices()):
            for j in xrange(0, Dgraph.num_vertices()):
                if matix[i][j] > matix[i][k]+matix[k][j]:
                    matix[i][j] =matix[i][k]+matix[k][j]
if __name__ == "__main__":
    print "dd"
