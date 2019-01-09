import graph_tool.all as gt

def load_graph_from_matrix(graph,graphfilename):
    try:
        graphltextcontent = open(graphfilename, 'r')
    except IOError:
        print "Error: file %s not exists" % graphfilename
    graphcontentlines=graphltextcontent.readlines()
    if not graphcontentlines:
        return False
    lengraphcontentlines=len(graphcontentlines)
    if(lengraphcontentlines<3):
        print "file Error"
        return False
    num_vertices=int(graphcontentlines[0])
    graph.add_vertex(num_vertices)

    labels=graphcontentlines[1].split()
    assert len(labels) is num_vertices
    vpl=graph.new_vertex_property("string")
    graph.vertex_properties["label"]=vpl
    for i in xrange(0,num_vertices):
        vpl[graph.vertex(i)]=labels[i]

    edgelines=graphcontentlines[2:]
    try:
        for lines in edgelines:
            line=lines.split()
            if(len(line)!=2):
                break
            sourceid=int(line[0])
            targetid=int(line[1])
            assert (sourceid <= num_vertices)
            assert (targetid <= num_vertices)
            graph.add_edge(graph.vertex(sourceid), graph.vertex(targetid))
    finally:
        graphltextcontent.close()
        print 'The constructed graph is %s' % graph



def load_graph_from_grapeformat(v_file,e_file):
    g=gt.Graph()
    g.vertex_properties["label"] = g.new_vertex_property("string")
    try:
        vf = open(v_file, 'r')
    except IOError:
        print "Error: file %s not exists" % v_file
    try:
        ef = open(e_file, 'r')
    except IOError:
        print "Error: file %s not exists" % e_file

    v_lines=vf.readlines()
    num_vertex = len(v_lines)
    g.add_vertex(num_vertex)
    try:
        for lines in v_lines:
            lines = lines.strip('\n')
            line=lines.split('\t')
            if(len(line)!=2):
                break
            node_id=int(line[0])-1
            g.vertex_properties["label"][g.vertex(node_id)]=line[1]
    finally:
        vf.close()
        # print 'The constructed graph is %s' % graph
    e_lines = ef.readlines()
    try:
        for lines in e_lines:
            lines = lines.strip('\n')
            line=lines.split('\t')
            if(len(line)<2):
                break
            sourceid=int(line[0])-1
            targetid=int(line[1])-1
            eg=g.edge(g.vertex(sourceid), g.vertex(targetid))
            if not eg:
                g.add_edge(g.vertex(sourceid), g.vertex(targetid))
    finally:
        ef.close()
        # print 'The constructed graph is %s' % graph
    return g

if __name__ == "__main__":
    print "ok"
    
	
