# -*- coding: utf-8 -*-
"""
Created on Thu Mar 17 15:15:17 2022

@author: 陈生
"""
import pandas as pd 
import numpy as np
from difflib import SequenceMatcher
import csv
import time
import matplotlib as plt
plt.rcParams['font.sans-serif'] = ['SimHei']
plt.rcParams['axes.unicode_minus'] = False 
def similarity(a, b):
    return SequenceMatcher(None, a, b).ratio()

def same_type_name(original_data,evolved_data):
    start_time=time.time()
    print('\t正在配对名称不变的实体：')
    pair_data=[]
    unmatch_original=pd.DataFrame()
    match_evolved=pd.DataFrame()
    unmatch_evolved=pd.DataFrame()
    for i in range(len(original_data)):
        temp=pd.DataFrame()
        for j in range(len(evolved_data)):
            if original_data.iloc[i,:]['Entity'].lower()==evolved_data.iloc[j,:]['Entity'].lower():
                temp=pd.concat([original_data.iloc[[i]],evolved_data.iloc[[j]]])
                temp=temp.reset_index(drop=True)

        if len(temp)==0:
    #            print('第%d行处理完成：未找到直接匹配结果'%(i+1))
            unmatch_original=pd.concat([unmatch_original,original_data.iloc[[i]]])
        else:
            match_evolved=pd.concat([match_evolved,temp.iloc[[1]]])
            pair_data.append(temp)
#            print('第%d行处理完成：找到直接匹配结果'%(i+1))
    unmatch_evolved=pd.concat([match_evolved,evolved_data])
    unmatch_evolved=unmatch_evolved.drop_duplicates(keep=False)
    unmatch_original=unmatch_original.reset_index(drop=True)
    unmatch_evolved=unmatch_evolved.reset_index(drop=True)
    end_time=time.time()
    print('\t配对完成，用时：%.2fs' %(end_time-start_time))
    return pair_data,unmatch_original,unmatch_evolved

def deliver_unmatch(unmatch_ori_1,unmatch_evo_1,pair_first):
    print('正在分配未匹配实体')
    delete_entity=pd.DataFrame()
    added_entity=pd.DataFrame()
    for i in range(len(unmatch_ori_1)):
    
        p=0
        if pd.isna(unmatch_ori_1.iloc[i,:]['Superclass(es)'])==1:
            delete_entity=delete_entity.append(unmatch_ori_1.iloc[i,:])
        else:
            for p in range(len(unmatch_ori_1)):
                if unmatch_ori_1.iloc[i,:]['Superclass(es)'].lower()==unmatch_ori_1.iloc[p,:]['Entity'].lower():
                    delete_entity=delete_entity.append(unmatch_ori_1.iloc[i,:])
            for j in range(len(pair_first)):
                if pair_first[j].iloc[0,:]['Entity'].lower()==unmatch_ori_1.iloc[i,:]['Superclass(es)'].lower():
                    pair_first[j]=pd.concat([pair_first[j],unmatch_ori_1.iloc[[i]]])
        print('ori%d分配完成'%i)
    for x in range(len(unmatch_evo_1)):
        p=0
        if pd.isna(unmatch_evo_1.iloc[x,:]['Superclass(es)'])==1:
            added_entity=added_entity.append(unmatch_evo_1.iloc[x,:])
        for p in range(len(unmatch_evo_1)):
            if unmatch_evo_1.iloc[x,:]['Superclass(es)'].lower()==unmatch_evo_1.iloc[p,:]['Entity'].lower():
                added_entity=added_entity.append(unmatch_evo_1.iloc[x,:])
        for y in range(len(pair_first)):
            if pair_first[y].iloc[0,:]['Entity'].lower()==unmatch_evo_1.iloc[x,:]['Superclass(es)'].lower():
                pair_first[y]=pd.concat([pair_first[y],unmatch_evo_1.iloc[[x]]])

        print('evo%d分配完成'%x)

    print('分配完成')
    return pair_first,added_entity,delete_entity

def count_similarity(pair_data,delete_entity,added_entity,change_entity,ori,evo,threshold):
    start_time=time.time()
    print('\t正在划分split、merge、map实体：')

    need_count=[]
    si=[]

    for i in range(len(pair_data)):
        if len(pair_data[i])>2:
            need_count.append(pair_data[i].reset_index(drop=True))

    for subgraph in need_count:
        i=0
        j=0
        x=0
        y=0
        change_index=[]
        del_index=[]
        add_index=[]
        temp=subgraph.iloc[2:,:]
        
        ver_0=temp[(temp['start_version']==ori)]
        ver_1=temp[(temp['start_version']==evo)]
        
        ver_0=pd.concat([subgraph.iloc[[0]],ver_0])
        ver_1=pd.concat([subgraph.iloc[[1]],ver_1])
        
        ver_0=ver_0.reset_index(drop=True) 
        ver_1=ver_1.reset_index(drop=True)
        
        similarity_matrix=np.zeros([len(ver_0),len(ver_1)])
        for i in range(len(ver_0)):
            for j in range(len(ver_1)):
                similarity_matrix[i][j]=similarity(ver_0.iloc[i,:]['Entity'],ver_1.iloc[j,:]['Entity'])
        si.append(similarity_matrix)
        
        #对应split
        if len(ver_0)==1:
            for x in range(len(ver_1)-1):
                if similarity_matrix[0][x+1]>threshold:
                    change_index.append(x+1)
                else:
                    add_index.append(x+1)
            for index in change_index:
                ver_1.loc[index,'change_operation']='split_from/'+str(ver_0.iloc[0,:]['Entity'])
                change_entity=change_entity.append(ver_1.iloc[[index]])
            for index in add_index:
                added_entity=added_entity.append(ver_1.iloc[[index]])
                
        #对应merge
        elif len(ver_1)==1:
            for y in range(len(ver_0)-1):

                if similarity_matrix[y+1][0]>threshold:
                    
                    change_index.append(y+1)
                else:
                    del_index.append(y+1)

            for index in change_index:
                ver_0.loc[index,'change_operation']='merge_into/'+str(ver_1.iloc[0,:]['Entity'])

                change_entity=change_entity.append(ver_0.iloc[[index]])
            for index in del_index:
                delete_entity=delete_entity.append(ver_0.iloc[[index]])
        
        
        else:
            unmatch_index=np.arange(1,len(ver_1))
            unmatch_index=unmatch_index.tolist()
            sub_similarity_matrix=similarity_matrix[1:len(ver_0),1:len(ver_1)]

            for x in range(len(sub_similarity_matrix)):
                
                max_index=np.argmax(sub_similarity_matrix[x])

                if max(sub_similarity_matrix[x])>threshold:
                    ver_0.loc[x+1,'change_operation']='map_into/'+str(ver_1.iloc[max_index+1,:]['Entity'])
                    if max_index+1 in unmatch_index:
                        unmatch_index.remove(max_index+1)
                    else:
                        continue
                    change_entity=change_entity.append(ver_0.iloc[[x+1]])
                else:
                    delete_entity=delete_entity.append(ver_0.iloc[[x+1]])
            for unmatch_evo in unmatch_index:
                print(ver_1.iloc[[unmatch_evo]])
                added_entity=added_entity.append(ver_1.iloc[[unmatch_evo]])

    delete_entity['change_operation']='delete'           
    added_entity['change_operation']='add'
    delete_entity=delete_entity.reset_index(drop=True)
    added_entity=added_entity.reset_index(drop=True)
    change_entity=change_entity.reset_index(drop=True)

    end_time=time.time()
    
    print('\t划分完成，用时：%.2fs'%(end_time-start_time))
    return delete_entity,added_entity,change_entity,si,need_count

def change_att(pair_data,change_entity):
    start_time=time.time()
    print('\t正在配对属性发生变化的实体：')
    compare_columns=[i for i in range(2,15)]
    
    for i in range(len(pair_data)):
        
        pair_data[i]= pair_data[i].reset_index(drop=True)
        temp=pair_data[i]
        temp=temp.fillna(0)
        sign=0
        sign2=0
        sign3=0
        sign4=0
        for column in compare_columns:
            if sign==1:
                continue
            elif temp.columns.values[column].find('Superclass(es)')!=-1:
                serves=[4,5]
                sign2=0
                for col in serves:
                    if temp.iloc[0,column]==temp.iloc[1,col]:
                        sign2=1
                if sign2==0:
                    pair_data[i].loc[0,'change_operation']='att_change'
                    pair_data[i].loc[1,'change_operation']='att_change'
                    change_entity=change_entity.append(pair_data[i].iloc[0,:]) 
                    change_entity=change_entity.append(pair_data[i].iloc[1,:]) 
                    sign=1
            elif temp.columns.values[column].find('has_part')!=-1:
                supports=[6,7,8,9,10,11,12]
                sign3=0
                for col in supports:
                    if temp.iloc[0,column]==temp.iloc[1,col]:
                        sign3=1
                if sign3==0:
                    pair_data[i].loc[0,'change_operation']='att_change'
                    pair_data[i].loc[1,'change_operation']='att_change'
                    change_entity=change_entity.append(pair_data[i].iloc[0,:]) 
                    change_entity=change_entity.append(pair_data[i].iloc[1,:]) 
                    sign=1

            elif temp.iloc[0,column]!=temp.iloc[1,column]:
                pair_data[i].loc[0,'change_operation']='att_change'
                pair_data[i].loc[1,'change_operation']='att_change'
                change_entity=change_entity.append(pair_data[i].iloc[0,:]) 
                change_entity=change_entity.append(pair_data[i].iloc[1,:]) 
                sign=1
            
    end_time=time.time()
    print('\t配对完成，用时：%.2fs'%(end_time-start_time))
    change_entity=change_entity.reset_index(drop=True)
    return(change_entity,compare_columns,pair_data)

def change_detection(ori,evo,thres):
    start_time=time.time()
    ori_data=pd.read_csv('E:\wenjian\大四下\ATS知识图谱/ATS_T'+str(ori)+'.csv')
    evo_data=pd.read_csv('E:\wenjian\大四下\ATS知识图谱/ATS_T'+str(evo)+'.csv')
    ori_data['start_version']=ori
    evo_data['start_version']=evo
    pair_data,unmatch_ori_1,unmatch_evo_1=same_type_name(ori_data,evo_data)
    pair_second,added_entity,delete_entity=deliver_unmatch(unmatch_ori_1,unmatch_evo_1,pair_data)
    
    change_entity=pd.DataFrame()
    delete_entity,added_entity,change_entity,si,need_count=count_similarity(pair_second,delete_entity,added_entity,change_entity,ori,evo,thres)
    change_entity,compare_columns,pair_third=change_att(pair_second,change_entity)
    
    end_time=time.time()
    print('计算完成，用时：%.2fs'%(end_time-start_time))
    return(need_count,unmatch_ori_1,unmatch_evo_1,delete_entity,added_entity,change_entity,pair_third)
#    return(unmatch_ori_1,unmatch_evo_1,delete_entity,added_entity)
def restore(delete_entity,added_entity,change_entity,ori,evo,thres):
    data=pd.concat([change_entity,added_entity,delete_entity])
    data=data.drop_duplicates()
    data.to_csv('E:\ATS知识图谱/ATS_diff'+str(ori)+str(evo)+'_threshold_'+str(thres)+'.csv',index=0)
#%%

if __name__ == "__main__":
    need_count,unmatch_ori_1,unmatch_evo_1,delete_entity,added_entity,change_entity,pair_third=change_detection(1,2,0.5)


    restore(delete_entity,added_entity,change_entity,1,2,0.5)