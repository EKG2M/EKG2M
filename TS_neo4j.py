# -*- coding: utf-8 -*-
"""
Created on Sun Mar 20 17:24:53 2022

@author: 陈生
"""

from py2neo import Graph, Node, Relationship,NodeMatcher,RelationshipMatcher
#from entity_match import *
import pandas as pd 
import numpy as np
import re
def attribute_analysis(string):
    string=string.split('+')
    att_list=list(set(string))
    if '' in att_list:
        att_list.remove('')
    if '0' in att_list:
        att_list.remove('0')
    return att_list


def relation_analysis(string):
    string=string.split('+')
    rel_list=list(set(string))
    if '' in rel_list:
        rel_list.remove('')
    if '0' in rel_list:
        rel_list.remove('0')
    for i in range(len(rel_list)):
        rel_list[i]=rel_list[i].split('/')
    return rel_list
def create_KG(data,rel_list):
    for i in range(len(data)):

        # 创建初始节点
        node_name=data.iloc[i,:]['Entity']
        concept=str(data.iloc[i,:]['Type']+'_'+str(data.iloc[i,:]['start_version']))
        node = Node(concept, name=node_name,start_version=str(data.iloc[i,:]['start_version']),end_version=str(data.iloc[i,:]['end_version']))
        ret = graph.create(node)
        if ret:
            print("Create Node Failed")
    print("创建节点完成")
    for i in range(len(data)):
        matcher_parent= NodeMatcher(graph)
        matcher_sub=NodeMatcher(graph)
        relmatch = RelationshipMatcher(graph)
        sub_node=matcher_sub.match('Class_'+str(data.iloc[i,:]['start_version']),name=data.iloc[i,:]['Entity']).first()
        if pd.isna(data.iloc[i,3])!=1:
            attribute_list=attribute_analysis(data.iloc[i,3])

            for j in range(len(attribute_list)):
                sub_node[data.columns.values[3]+'.'+str(j+1)]=attribute_list[j]
                graph.push(sub_node)
        for rel_column in rel_list:
            if pd.isna(data.iloc[i,rel_column])!=True:
                relation_list=relation_analysis(data.iloc[i,rel_column])

                for relation in relation_list:
                    parent_name=relation[0]
                    parent_class=data[(data['Entity']==parent_name)].iloc[:,:]['start_version'].values[0]
                    parent_node=matcher_parent.match('Class_'+str(parent_class),name=parent_name).first()

                    if len(relmatch.match({sub_node,parent_node},data.columns.values[rel_column]))>0:

                        if len(relation)==1:
                            continue
                        else:

                            rel=(relmatch.match({sub_node,parent_node},data.columns.values[rel_column])).first()
                            rel[''.join(re.findall(r'[A-Za-z]', relation[-1]))]=int(re.sub("\D", "", relation[-1]))

                            graph.push(rel)
                    else:
                        if len(relation)==1:

                            new_rel=Relationship(sub_node,data.columns.values[rel_column],parent_node)
                            graph.create(new_rel)
                        else:

                            properties={''.join(re.findall(r'[A-Za-z]', relation[-1])):int(re.sub("\D", "", relation[-1]))}
                            new_rel=Relationship(sub_node,data.columns.values[rel_column],parent_node,**properties)

                            graph.create(new_rel)
    print("属性、关系添加完成")







#%%




graph = Graph('bolt://localhost:7687', auth=('neo4j', 'password'))
graph.delete_all()
unify_data=pd.read_csv('E:\ATS知识图谱/ATS_unify_KG.csv')

rel_list=[2,4,5,6,9,10,11]
create_KG(unify_data,rel_list)