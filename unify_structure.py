# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 10:08:25 2022

@author: 陈生
"""

import pandas as pd
from entity_match import *
import numpy as np

def unify(string):
    need_del=[] 
    string=string.split('+')
    string=list(set(string))
    del_element=[' ','0','nan']
    for element in del_element:
        if element in string:
            string.remove(element)
    
    if len(string)<=1:
        dealt='+'.join(string)

    else:

        temp=string

        for i in range(len(temp)):
            count=0
            for j in range(len(string)):
                split_string=string[j].split('/')
                for k in range(len(split_string)):
                    if temp[i]==split_string[k]:
                        count=count+1                                       
            if count>1:
                need_del.append(temp[i])

        string.extend(need_del)
        dealt=[]
        for x in range(len(string)):
            count1=0
            for y in range(len(string)):
                if string[x]==string[y]:
                    count1=count1+1
            if count1<=1:
                dealt.append(string[x])
      #  string=list(set(string))
        dealt='+'.join(dealt)
    return dealt


def unify_structure(data):
    print('\n\t正在融合为统一数据结构')
    split=[]
    split2=[]
    data=data.replace('+',None)
    data['Superclass']=data['Superclass']+'+'+data['Superclass.1']+'+'+data['Superclass.2']
    data['has_part']=data['has_part']+'+'+data['has_part.1']+'+'+data['has_part.2']+'+'+data['has_part.3']+'+'+data['has_part.4']
    data['part_of']=data['part_of']+'+'+data['part_of.1']
    
    superclass=['Superclass.1','Superclass.2']
    haspart=['has_part.1','has_part.2','has_part.3','has_part.4']
    partof=['part_of.1']
    data=data.drop(superclass,axis=1)
    data=data.drop(haspart,axis=1)
    data=data.drop(partof,axis=1)
    data=data.reset_index(drop=True)
    for i in range(len(data)):
        for j in range(6,16):

            data.iloc[i,j]=unify(str(data.iloc[i,j]))
    '''
    for i in range(len(data)):
        data.loc[i,'Superclass']=unify(data.loc[i,'Superclass'])
        data.loc[i,'part_of']=unify(data.loc[i,'part_of'])
        data.loc[i,'has_part']=unify(data.loc[i,'has_part'])
    '''
    print('\t融合完成')
    return data
    

    '''
    for i in range(len(unchange_data)):
        for column in superclass:
            if pd.isna(unchange_data.iloc[i,:][column])!=1:
                unchange_data.loc[i,'Superclass']=unchange_data.loc[i,'Superclass']+'+'+unchange_data.iloc[i,:][column]
        
        for column in haspart:
            if pd.isna(unchange_data.iloc[i,:][column])!=1:
                unchange_data.loc[i,'has_part']=unchange_data.loc[i,'has_part']+'+'+unchange_data.iloc[i,:][column]
        for column in partof:
            if pd.isna(unchange_data.iloc[i,:][column])!=1:
                unchange_data.loc[i,'part_of']=unchange_data.loc[i,'part_of']+'+'+unchange_data.iloc[i,:][column]
    unchange_data=unchange_data.drop(superclass,axis=1)
    unchange_data=unchange_data.drop(haspart,axis=1)
    unchange_data=unchange_data.drop(partof,axis=1)
    '''
    return data,split


def update(data,evo_data,diff,ori,evo):
    print('\t正在更新版本%d：'%ori)
    temp=[]
    c=0
    #删除实体，添加end_version
    delete_id=list(set(diff[(diff['change_operation']=='delete')]['id'].tolist()))
    for d_id in delete_id:
        data.loc[data[(data['id']==d_id)].index,'end_version']= evo
    #新增实体，直接插入，但start_version不同
    add_entity=diff[(diff['change_operation']=='add')]
    add_entity=add_entity.drop('change_operation',axis=1)
    data=pd.concat([data,add_entity])
    
    #变更实体，分情况处理
    change_entity=diff[(diff['change_operation']!='add')&(diff['change_operation']!='delete')&(diff['change_operation']!='att_change')]
    
    for i in range(len(change_entity)):
        data=data.reset_index(drop=True)
        temp_index=0
        temp_evo_index=0
        if change_entity.iloc[i,:]['change_operation'].find('map')!=-1:
            map_name=change_entity.iloc[i,:]['change_operation'].split('/')[-1]

            temp_index=data[(data['Entity']==change_entity.iloc[i,:]['Entity'])].index
            temp_evo_index=evo_data[(evo_data['Entity']==map_name)].index
            data.loc[temp_index,'end_version']=evo
            data.loc[temp_index,'map_into']=evo_data.loc[temp_evo_index,'Entity'].values[0]
            mapped_slice=evo_data[(evo_data['Entity']==map_name)]

            data=pd.concat([data,mapped_slice])

        elif change_entity.iloc[i,:]['change_operation'].find('merge')!=-1:
            merge_name=change_entity.iloc[i,:]['change_operation'].split('/')[-1]
            temp_evo_index=evo_data[(evo_data['Entity']==merge_name)].index
            temp_index=data[(data['Entity']==change_entity.iloc[i,:]['Entity'])].index
            data.loc[temp_index,'end_version']=evo
            data.loc[temp_index,'merge_into']=evo_data.loc[temp_evo_index,'Entity'].values[0]

            temp.append(evo_data.iloc[temp_evo_index,:]['Entity'])

        
        elif change_entity.iloc[i,:]['change_operation'].find('split')!=-1:

            split_name=change_entity.iloc[i,:]['change_operation'].split('/')[-1]
            temp_index=evo_data[(evo_data['Entity']==change_entity.iloc[i,:]['Entity'])].index
            temp_ori_index=data[(data['Entity']==split_name)].index
            if len(data[(data['Entity']==change_entity.iloc[i,:]['Entity'])])!=0:

                index=data[(data['Entity']==change_entity.iloc[i,:]['Entity'])].index
                data.loc[index,'start_version']=evo
                data.loc[index,'split_from']=split_name
            else:

                split_slice=evo_data[(evo_data['Entity']==change_entity.iloc[i,:]['Entity'])]

                split_slice.loc[temp_index,'start_version']=evo
                split_slice.loc[temp_index,'split_from']=split_name
                data=pd.concat([data,split_slice])
    
    #以下为属性更新
    aa=pd.DataFrame()
    att_change_entity=diff[(diff['change_operation']=='att_change')]
    att_change_entity=att_change_entity.reset_index(drop=True)
    
    att_change_name=list(set(att_change_entity['Entity'].values.tolist()))


    change_col=[i for i in range(6,23)]
    for name in att_change_name:
        att_change_temp=att_change_entity[(att_change_entity['Entity']==name)]
        att_change_temp=att_change_temp.reset_index(drop=True)
        att_change_temp=att_change_temp.fillna('0')
        unify_data=data[(data['Entity']==att_change_temp.iloc[0,:]['Entity'])]

        unify_data=unify_data.reset_index(drop=True)
        for i in range(len(att_change_temp)):
            for col in change_col:
                if pd.isna(att_change_temp.iloc[i,col])==1:
                    continue
                elif (col==6)|(col==7)|(col==8):
                    sign1=0
                    for j in range(3):
                        if i==0:
                            if att_change_temp.iloc[0,col]==att_change_temp.iloc[1,6+j]:
                                sign1=1
                        elif i==1:
                            if att_change_temp.iloc[1,col]==att_change_temp.iloc[0,6+j]:
                                sign1=1
                    if sign1==0:
                        if i==0:

                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/end'+str(evo)
                        else:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/start'+str(evo)
                        
                elif (col==13)|(col==14)|(col==15)|(col==16)|(col==17):
                    sign2=0                 
                    for j in range(5):
                        if i==0:
                            if att_change_temp.iloc[0,col]==att_change_temp.iloc[1,13+j]:
                                sign2=1
                        elif i==1:
                            if att_change_temp.iloc[1,col]==att_change_temp.iloc[0,13+j]:
                                sign2=1
                    if sign2==0:
                        if i==0:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/end'+str(evo)
                        else:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/start'+str(evo)
                            
                elif (col==20)|(col==21):
                    sign3=0                 
                    for j in range(2):
                        if i==0:
                            if att_change_temp.iloc[0,col]==att_change_temp.iloc[1,20+j]:
                                sign3=1
                        elif i==1:
                            if att_change_temp.iloc[1,col]==att_change_temp.iloc[0,20+j]:
                                sign3=1
                    if sign3==0:
                        if i==0:

                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/end'+str(evo)
                        else:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/start'+str(evo)
                else:
                    if att_change_temp.iloc[0,col]==att_change_temp.iloc[1,col]:
                        continue
                    else:
                        if i==0:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/end'+str(evo)
                        else:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/start'+str(evo)



        for k in range(17):
            if (att_change_temp.iloc[0,6+k]==att_change_temp.iloc[1,6+k])&(att_change_temp.iloc[0,6+k]==None):
                continue
            else:
                
                unify_data.iloc[0,6+k]=str(unify_data.iloc[0,6+k])+'+'+att_change_temp.iloc[0,6+k].strip("'")+'+'+att_change_temp.iloc[1,6+k].strip("'")
        unify_data=unify_data.replace('0+0',' ')
        aa=aa.append(unify_data)
        data=data.drop(data[(data['Entity']==unify_data['Entity'].values[0])].index)
        data=pd.concat([data,unify_data])
        data=data.reset_index(drop=True)
        data=data.fillna(' ')
        
 #   data=data.drop(columns=['change_operation'])
     
    print('\t版本%d更新完成'%ori)
    return data



    





#%%
#主函数
ori_data=pd.read_csv('E:\wenjian\大四下\dataset\T0.csv')
evo1_data=pd.read_csv('E:\wenjian\大四下\dataset\T1.csv')
evo2_data=pd.read_csv('E:\wenjian\大四下\dataset\T2.csv')
#%%
ori_data['start_version']=0
ori_data['end_version']=-1
ori_data['split_from']=''
ori_data['merge_into']=''
ori_data['map_into']=''

evo1_data['start_version']=1
evo1_data['end_version']=-1
evo1_data['end_version']=-1
evo1_data['split_from']=''
evo1_data['merge_into']=''
evo1_data['map_into']=''

evo2_data['start_version']=2
evo2_data['end_version']=-1
evo2_data['end_version']=-1
evo2_data['split_from']=''
evo2_data['merge_into']=''
evo2_data['map_into']=''
#%%
diff_01=pd.read_csv('E:\差异检测/diff01.csv')
diff_12=pd.read_csv('E:\差异检测/diff12.csv')
#%%
#更新
print('正在更新：')
update_1=update(ori_data,evo1_data,diff_01,0,1)
update_2=update(update_1,evo2_data,diff_12,1,2)
#融合为统一数据结构
final_KG=unify_structure(update_2)
print('更新完成')
#final_KG.to_csv('E:\wenjian\大四下\差异检测/unify_KG.csv',index=0)
#%%
#update_2=update(evo1_data,evo2_data,diff_12,1,2)
#%%
'''
def test(att_change_temp,evo):
    att_change_temp=att_change_temp.reset_index(drop=True)
    change_col=[i for i in range(6,23)]
    for i in range(len(att_change_temp)):
            for col in change_col:
                if pd.isna(att_change_temp.iloc[i,col])==1:
                    continue
                elif (col==6)|(col==7)|(col==8):
                    sign1=0
                    for j in range(3):
                        if i==0:
                            if att_change_temp.iloc[0,col]==att_change_temp.iloc[1,6+j]:
                                sign1=1
                        elif i==1:
                            if att_change_temp.iloc[1,col]==att_change_temp.iloc[0,6+j]:
                                sign1=1
                    if sign1==0:
                        if i==0:

                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/end'+str(evo)
                        else:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/start'+str(evo)
                else:
                    if att_change_temp.iloc[0,col]!=att_change_temp.iloc[1,col]:
                        if i==0:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/end'+str(evo)
                        else:
                            att_change_temp.iloc[i,col]=att_change_temp.iloc[i,col]+'/start'+str(evo)
    return att_change_temp

'''
#%%




#%%
'''
change_data,unchange_data=divide_data(ori_data,diff_01,diff_12)

unchange_unify_data=unify_unchange_structure(unchange_data)
add_unify_data=unify_added_structure(ori_data,diff_01,diff_12)    
delete_unify_data=unify_delete_structure(ori_data,diff_01,diff_12)      
unify_structure_data=pd.concat([unchange_unify_data,add_unify_data,delete_unify_data]) 

def divide_data(ori_data,diff1,diff2):
    diff=pd.concat([diff1,diff2])
    diff_id=list(set(diff['id'].tolist()))
    change_data=pd.DataFrame()
    for i in range(len(ori_data)):
        for temp_id in diff_id:
            if ori_data.iloc[i,:]['id']==temp_id:
                change_data=change_data.append(ori_data.iloc[[i]])
                continue
        print('实体%d对比完成'%i)
    print('实体对比完成')
    
    
    change_id=list(set(change_entity['id'].tolist()))
    print('共计%d个实体发生变化'%len(change_id))
    added_id=list(set(added_entity['id'].tolist()))
    print('共计添加%d个实体'%len(added_id))
    delete_id=list(set(delete_entity['id'].tolist()))
    print('共计删除%d个实体'%len(delete_id))
    
    print('实体对比完成')
    unchange_data=pd.concat([change_data,ori_data])
    unchange_data=unchange_data.drop_duplicates(keep=False)
    change_data=change_data.reset_index(drop=True)
    unchange_data=unchange_data.reset_index(drop=True)
    return change_data,unchange_data

def unify_added_structure(ori_data,diff1,diff2):
    diff=pd.concat([diff1,diff2])
    add_data=diff[(diff['change_operation']=='add')]
    add_data=add_data.reset_index(drop=True)
    superclass=['Superclass.1','Superclass.2']
    haspart=['has_part.1','has_part.2','has_part.3','has_part.4']
    partof=['part_of.1']
    for i in range(len(add_data)):
        for column in superclass:
            if pd.isna(add_data.iloc[i,:][column])!=1:
                add_data.loc[i,'Superclass']=add_data.loc[i,'Superclass']+'+'+add_data.iloc[i,:][column]
        
        for column in haspart:
            if pd.isna(add_data.iloc[i,:][column])!=1:
                add_data.loc[i,'has_part']=add_data.loc[i,'has_part']+'+'+add_data.iloc[i,:][column]
        for column in partof:
            if pd.isna(add_data.iloc[i,:][column])!=1:
                add_data.loc[i,'part_of']=add_data.loc[i,'part_of']+'+'+add_data.iloc[i,:][column]
    add_data=add_data.drop(superclass,axis=1)
    add_data=add_data.drop(haspart,axis=1)
    add_data=add_data.drop(partof,axis=1)
    add_data=add_data.drop('change_operation',axis=1)
    return add_data

def unify_delete_structure(ori_data,diff1,diff2):
    diff=pd.concat([diff1,diff2])
    delete_data=diff[(diff['change_operation']=='delete')]
    delete_data=delete_data.reset_index(drop=True)
    superclass=['Superclass.1','Superclass.2']
    haspart=['has_part.1','has_part.2','has_part.3','has_part.4']
    partof=['part_of.1']
    
    for i in range(len(delete_data)):
        delete_data.loc[i,'end_version']= delete_data.iloc[i,:]['start_version']+1
        for column in superclass:
            if pd.isna(delete_data.iloc[i,:][column])!=1:
                delete_data.loc[i,'Superclass']=delete_data.loc[i,'Superclass']+'+'+delete_data.iloc[i,:][column]
        
        for column in haspart:
            if pd.isna(delete_data.iloc[i,:][column])!=1:
                delete_data.loc[i,'has_part']=delete_data.loc[i,'has_part']+'+'+delete_data.iloc[i,:][column]
        for column in partof:
            if pd.isna(delete_data.iloc[i,:][column])!=1:
                delete_data.loc[i,'part_of']=delete_data.loc[i,'part_of']+'+'+delete_data.iloc[i,:][column]
    delete_data=delete_data.drop(superclass,axis=1)
    delete_data=delete_data.drop(haspart,axis=1)
    delete_data=delete_data.drop(partof,axis=1)
    delete_data=delete_data.drop('change_operation',axis=1)
    
    return delete_data
'''
#%%
