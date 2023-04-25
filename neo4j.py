# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 11:15:26 2022

@author: 陈生
"""

from py2neo import Graph, Node, Relationship,NodeMatcher
#from entity_match import *
import pandas as pd 
import numpy as np
import re
def MatchNode(m_graph, m_label):
    matcher = NodeMatcher(m_graph)
    # m_n = "_.id="+"\'"+m_attrs['id']+"\'"
#    m_n = "_.name=" + "\"" + m_attrs + "\""
    re_value = matcher.match(m_label).first()
    # print(type(re_value))
    # print(re_value)
    # re_value.id = 0
    # print(re_value['id'])
    return re_value

def relation_analysis(data):
    data=data.split('+')
    for i in range(len(data)):
        data[i]=data[i].split('/')
    return data

def create_KG(original_ver,att_list,rel_list):
    
    ori_data=pd.read_csv('E:\差异检测/unify_KG.csv')
#    ori_data=pd.read_csv('E:\dataset/testT.csv')
    for i in range(len(ori_data)):
        # 创建初始节点
        node_name=ori_data.iloc[i,:]['Entity']
        node_ID=str(int(ori_data.iloc[i,:]['id']))
        concept=str(ori_data.iloc[i,:]['Type']+'_'+str(ori_data.iloc[i,:]['start_version']))
        node = Node(concept, name=node_name, id=node_ID, start_version=str(ori_data.iloc[i,:]['start_version']),end_version=ori_data.iloc[i,:]['end_version'])
        ret = graph.create(node)
        if ret:
            print("Create Node Failed")
            
    print("创建节点完成")
    
    i=0
    for i in range(len(ori_data)):
        # 添加属性
        matcher_1 = NodeMatcher(graph)
        newnode = matcher_1.match('Class_'+str(ori_data.iloc[i,:]['start_version']),name=ori_data.iloc[i,:]['Entity']).first()
        for att_column in att_list:
            if pd.isna(ori_data.iloc[i,att_column])!=True:
                newnode.update({ori_data.columns.values.tolist()[att_column]: ori_data.iloc[i,att_column]})
        graph.push(newnode)
        
        matcher_parent= NodeMatcher(graph)
        matcher_sub=NodeMatcher(graph)
        for rel_column in rel_list:
            if pd.isna(ori_data.iloc[i,rel_column])!=True:
                relation_list=relation_analysis(ori_data.iloc[i,rel_column])
                for relation in relation_list:
                    if len(relation)==1:
                        parent_data=ori_data[(ori_data['Entity']==relation[0])].reset_index(drop=True)
                        if len(parent_data)==0:
                            continue
                        else:
                            parent_name=parent_data.iloc[0,:]['Entity']
                            parent_version=parent_data.iloc[0,:]['start_version']
                            parent_node=matcher_parent.match('Class_'+str(parent_version),name=parent_name).first()
                            sub_node=matcher_sub.match('Class_'+str(ori_data.iloc[i,:]['start_version']),name=ori_data.iloc[i,:]['Entity']).first()
                            properties={'start_version':str(ori_data.iloc[i,:]['start_version'])}
                            relation=Relationship(sub_node,ori_data.columns.values.tolist()[rel_column],parent_node,**properties)
                            graph.create(relation)
                    else:
                        parent_data=ori_data[(ori_data['Entity']==relation[0])].reset_index(drop=True)
                        if len(parent_data)==0:
                            continue
                        else:
                            parent_name=parent_data.iloc[0,:]['Entity']
                            parent_version=parent_data.iloc[0,:]['start_version']
                            parent_node=matcher_parent.match('Class_'+str(parent_version),name=parent_name).first()
                            sub_node=matcher_sub.match('Class_'+str(ori_data.iloc[i,:]['start_version']),name=ori_data.iloc[i,:]['Entity']).first()
                            properties={''.join(re.findall(r'[A-Za-z]', relation[-1])):int(re.sub("\D", "", relation[-1]))}
                            relation=Relationship(sub_node,ori_data.columns.values.tolist()[rel_column],parent_node,**properties)
                            graph.create(relation)
                        
    print('共计%d个节点的属性、关系添加完成'%i)
        
    
    print("添加属性关系完成")
    print("创建节点完成")
   
#%%    
graph = Graph('bolt://localhost:7687', auth=('neo4j', 'password'))
graph.delete_all()
#%%
rel_extend=[17,18,19,20]
rel_list=[i for i in range(6,16)]
rel_list.extend(rel_extend)
att_list=[3,4,5]
#%%
create_KG(0,att_list,rel_list)


