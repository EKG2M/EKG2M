# -*- coding: utf-8 -*-
"""
Created on Fri Mar  4 15:48:18 2022

@author: 陈生
"""

import pandas as pd 
import numpy as np
from difflib import SequenceMatcher
import csv
import time

def similarity(a, b):
    return SequenceMatcher(None, a, b).real_quick_ratio()

def same_type_name(original_data,evolved_data):
    start_time=time.time()
    print('\t正在配对名称不变的实体：')
    pair_data=[]
    unmatch_original=pd.DataFrame()
    match_evolved=pd.DataFrame()
    unmatch_evolved=pd.DataFrame()
    for i in range(len(original_data)):
        temp=evolved_data[(evolved_data['Entity']==original_data.iloc[i,:]['Entity'])&(evolved_data['Type']==original_data.iloc[i,:]['Type'])]
        if len(temp)==0:
#            print('第%d行处理完成：未找到直接匹配结果'%(i+1))
            unmatch_original=pd.concat([unmatch_original,(original_data.iloc[[i]])])
            continue
        else:
            match_evolved=pd.concat([match_evolved,temp])
            pair=pd.concat([original_data.iloc[[i]],temp])
            pair_data.append(pair)
#            print('第%d行处理完成：找到直接匹配结果'%(i+1))
    unmatch_evolved=pd.concat([match_evolved,evolved_data])
    unmatch_evolved=unmatch_evolved.drop_duplicates(keep=False)
    unmatch_original=unmatch_original.reset_index(drop=True)
    unmatch_evolved=unmatch_evolved.reset_index(drop=True)
    end_time=time.time()
    print('\t配对完成，用时：%.2fs' %(end_time-start_time))
    return pair_data,unmatch_original,unmatch_evolved
#%%
def deliver_unmatch(unmatch_ori_1,unmatch_evo_1,pair_first):
    print('正在分配未匹配实体')
    delete_entity=pd.DataFrame()
    added_entity=pd.DataFrame()
    for i in range(len(unmatch_ori_1)):
    
        p=0
        if pd.isna(unmatch_ori_1.iloc[i,:]['Superclass'])==1:
            delete_entity=delete_entity.append(unmatch_ori_1.iloc[i,:])
        else:
            for p in range(len(unmatch_ori_1)):
                if unmatch_ori_1.iloc[i,:]['Superclass']==unmatch_ori_1.iloc[p,:]['Entity']:
                    delete_entity=delete_entity.append(unmatch_ori_1.iloc[i,:])
            for j in range(len(pair_first)):
                if pair_first[j].iloc[0,:]['Entity']==unmatch_ori_1.iloc[i,:]['Superclass']:
                    pair_first[j]=pd.concat([pair_first[j],unmatch_ori_1.iloc[[i]]])
        print('ori%d分配完成'%i)
    for x in range(len(unmatch_evo_1)):
        sign=0
        p=0
        superclass=pd.isna(unmatch_evo_1.iloc[x,6:9].tolist())
             
        if np.sum(superclass==1)<=1:
            added_entity=added_entity.append(unmatch_evo_1.iloc[x,:])
        elif pd.isna(unmatch_evo_1.iloc[x,:]['Superclass'])==1:
            added_entity=added_entity.append(unmatch_evo_1.iloc[x,:])

        else:
            for p in range(len(unmatch_evo_1)):
                if unmatch_evo_1.iloc[x,:]['Superclass']==unmatch_evo_1.iloc[p,:]['Entity']:
                    added_entity=added_entity.append(unmatch_evo_1.iloc[x,:])
            for y in range(len(pair_first)):
                if pair_first[y].iloc[0,:]['Entity']==unmatch_evo_1.iloc[x,:]['Superclass']:
                    pair_first[y]=pd.concat([pair_first[y],unmatch_evo_1.iloc[[x]]])
                    sign=1
              
            if sign==0:
                added_entity=added_entity.append(unmatch_evo_1.iloc[[x]])
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
                    unmatch_index.remove(max_index+1)
                    change_entity=change_entity.append(ver_0.iloc[[x+1]])
                else:
                    delete_entity=delete_entity.append(ver_0.iloc[[x+1]])
            for unmatch_evo in unmatch_index:
                added_entity=added_entity.append(ver_1.iloc[[unmatch_evo]])

    delete_entity['change_operation']='delete'           
    added_entity['change_operation']='add'
    delete_entity=delete_entity.reset_index(drop=True)
    added_entity=added_entity.reset_index(drop=True)
    change_entity=change_entity.reset_index(drop=True)
    change_type=[change_entity,added_entity,delete_entity]
    '''
    for change in change_type:
        id = change['id']
        change.drop(labels=['id'], axis=1,inplace = True)
        change.insert(0, 'id', id)
    '''
    end_time=time.time()
    
    print('\t划分完成，用时：%.2fs'%(end_time-start_time))
    return delete_entity,added_entity,change_entity,si,need_count

def change_att(pair_data,change_entity):
    start_time=time.time()
    print('\t正在配对属性发生变化的实体：')
    compare_columns=[i for i in range(3,23)]
    
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
            elif temp.columns.values[column].find('Superclass')!=-1:
                superclass=[6,7,8]
                sign2=0
                for col in superclass:
                    if temp.iloc[0,column]==temp.iloc[1,col]:
                        sign2=1
                if sign2==0:
                    pair_data[i].loc[0,'change_operation']='att_change'
                    pair_data[i].loc[1,'change_operation']='att_change'
                    change_entity=change_entity.append(pair_data[i].iloc[0,:]) 
                    change_entity=change_entity.append(pair_data[i].iloc[1,:]) 
                    sign=1
            elif temp.columns.values[column].find('has_part')!=-1:
                haspart=[13,14,15,16,17]
                sign3=0
                for col in haspart:
                    if temp.iloc[0,column]==temp.iloc[1,col]:
                        sign3=1
                if sign3==0:
                    pair_data[i].loc[0,'change_operation']='att_change'
                    pair_data[i].loc[1,'change_operation']='att_change'
                    change_entity=change_entity.append(pair_data[i].iloc[0,:]) 
                    change_entity=change_entity.append(pair_data[i].iloc[1,:]) 
                    sign=1
            elif temp.columns.values[column].find('part_of')!=-1:
                partof=[20,21]
                sign4=0
                for col in partof:
                    if temp.iloc[0,column]==temp.iloc[1,col]:
                        sign4=1
                if sign4==0:
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

def write_res(change_entity,added_entity,delete_entity):
    change_type=[change_entity,added_entity,delete_entity]
    for change in change_type:
        id = change['id']
        change.drop(labels=['id'], axis=1,inplace = True)
        change.insert(0, 'id', id)
    header=change_entity.columns.tolist()
    with open('E:\wenjian\大四下\差异检测\diff01.csv', 'a') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerow(['Deleted'])
        for i in range(len(delete_entity)):
            writer.writerow(delete_entity.iloc[i,:])
        writer.writerow(['Added']) 
        for i in range(len(added_entity)):
            writer.writerow(added_entity.iloc[i,:])
        writer.writerow(['Changed']) 
        for i in range(len(change_entity)):
            writer.writerow(change_entity.iloc[i,:])

def change_detection(ori,evo,threshold):   
    start_time=time.time()
    print('正在计算版本%d和版本%d之间的差异：' %(ori,evo))
    ori_data=pd.read_csv('E:\wenjian\大四下\dataset\T'+str(ori)+'.csv')
    ori_data['start_version']=ori
    ori_data['change_operation']=0
    evo_data=pd.read_csv('E:\wenjian\大四下\dataset\T'+str(evo)+'.csv')
    evo_data['start_version']=evo
    evo_data['change_operation']=0
    ori_data=ori_data.drop_duplicates(keep=False)
    evo_data=evo_data.drop_duplicates(keep=False)
    pair_first,unmatch_ori_1,unmatch_evo_1=same_type_name(ori_data,evo_data)
    pair_second,added_entity,delete_entity=deliver_unmatch(unmatch_ori_1,unmatch_evo_1,pair_first)
    change_entity=pd.DataFrame()
    delete_entity,added_entity,change_entity,si,need_count=count_similarity(pair_second,delete_entity,added_entity,change_entity,ori,evo,threshold)
    
    change_entity,compare_columns,pair_third=change_att(pair_second,change_entity)
#    write_res(change_entity,added_entity,delete_entity)
    end_time=time.time()
    print('计算完成，用时：%.2fs'%(end_time-start_time))
    return(need_count,unmatch_ori_1,unmatch_evo_1,delete_entity,added_entity,change_entity,pair_third)

def restore(delete_entity,added_entity,change_entity,ori,evo,thres):
    data=pd.concat([change_entity,added_entity,delete_entity])
    data=data.drop_duplicates()
    data.to_csv('E:\差异检测/diff'+str(ori)+str(evo)+'_threshold_'+str(thres)+'.csv',index=0)
#%%
if __name__ == "__main__":
    need_count,unmatch_ori_1,unmatch_evo_1,delete_entity,added_entity,change_entity,pair_third=change_detection(0,1,0.2)
    restore(delete_entity,added_entity,change_entity,0,1,0.2)
#    delete_entity,added_entity,change_entity,pair_data=change_detection(1,2)
#%%

if __name__ == "__main__":
    thres_list=[0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1.0]
    for thres in thres_list:
        need_count,unmatch_ori_1,unmatch_evo_1,delete_entity,added_entity,change_entity,pair_third=change_detection(0,1,thres)
        restore(delete_entity,added_entity,change_entity,0,1,thres)

