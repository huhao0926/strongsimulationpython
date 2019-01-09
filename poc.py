# -*- coding:utf-8 -*-
__author__ = 'huhao09'
import sys
import codecs
import re
with codecs.open("soc-pokec-profiles.txt","r") as fw:
    c_null_l = [0 for i in xrange(0,59)]
    str =""
    for line in fw:
        line=line.strip('\n')
        line=line.strip('\t')
        line = line.strip(' ')
        tmplist = line.split('\t')
        for i in xrange(0,59):
            if tmplist[i] =="null":
                c_null_l[i]+=1
    print c_null_l
        # print line
fw.close()
# if __name__=="__main__":
