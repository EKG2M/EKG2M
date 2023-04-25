# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 14:46:20 2022

@author: 陈生
"""

import pandas as pd
from entity_match import *
import numpy as np




def read_data(ori,evo,thres):
    diff=pd.read_csv('E:\ATS知识图谱/ATS_diff'+str(ori)+str(evo)+'_threshold_'+str(thres)+'.csv')
    evo_data=pd.read_csv('E:\ATS知识图谱/ATS_T'+str(evo)+'.csv')
    evo_data['start_version']=evo
    evo_data['end_version']=-1
    evo_data['split_from']=''
    evo_data['merge_into']=''
    evo_data['map_into']=''
    return diff,evo_data

def update(data,evo_data,diff,ori,evo):
    print('\t正在更新版本%d：'%ori)
    temp=[]
    c=0
    #删除实体，添加end_version
    delete_entity=list(set(diff[(diff['change_operation']=='delete')]['Entity'].tolist()))
    for name in delete_entity:
        data.loc[data[(data['Entity']==name)].index,'end_version']=evo
    #新增实体，直接插入，但start_version不同
    add_entity=list(set(diff[(diff['change_operation']=='add')]['Entity'].tolist()))
    for name in add_entity:
        temp_add=evo_data[(evo_data['Entity']==name)]
        data=pd.concat([data,temp_add])
    
    #变更实体，分情况处理
    change_entity=diff[(diff['change_operation']!='add')&(diff['change_operation']!='delete')&(diff['change_operation']!='att_change')]

    for i in range(len(change_entity)):
        data=data.reset_index(drop=True)

        if change_entity.iloc[i,:]['change_operation'].find('map')!=-1:
            map_name=change_entity.iloc[i,:]['change_operation'].split('/')[-1]

            temp_index=data[(data['Entity']==change_entity.iloc[i,:]['Entity'])].index
            map_data=evo_data[(evo_data['Entity']==map_name)]
            data.loc[temp_index,'end_version']=evo
            data.loc[temp_index,'map_into']=map_name
            data=pd.concat([data,map_data])
            data=data.reset_index(drop=True)
            
        elif change_entity.iloc[i,:]['change_operation'].find('merge')!=-1:

            merge_name=change_entity.iloc[i,:]['change_operation'].split('/')[-1]

            temp_evo_index=evo_data[(evo_data['Entity']==merge_name)].index
            
            temp_index=data[(data['Entity']==change_entity.iloc[i,:]['Entity'])].index
            data.loc[temp_index,'end_version']=evo
            data.loc[temp_index,'merge_into']=merge_name

            temp.append(evo_data.iloc[temp_evo_index,:]['Entity'])

        elif change_entity.iloc[i,:]['change_operation'].find('split')!=-1:

            split_name=change_entity.iloc[i,:]['change_operation'].split('/')[-1]
            split_data=evo_data[(evo_data['Entity']==change_entity.iloc[i,:]['Entity'])]
            split_data.loc[:,'split_from']=split_name

            data=pd.concat([data,split_data])
            
    
    #以下为属性更新

    att_change_entity=diff[(diff['change_operation']=='att_change')]
    att_change_entity=att_change_entity.reset_index(drop=True)
    ats_evoForm=[3,4]
    serve=[6,7]
    support=[i for i in range(8,15)]
    change_col=[i for i in range(2,15)]
    for i in range(int(len(att_change_entity)/2)):
        temp_pair=att_change_entity.iloc[2*i:2*i+2,:]
        temp_pair=temp_pair.reset_index(drop=True)
        data_index=data[(data['Entity']==temp_pair.iloc[0,:]['Entity'])].index
        if len(data_index)==0:
            print(temp_pair.iloc[0,:]['Entity'])
        for col in change_col:
            if col in ats_evoForm:

                if pd.isna(temp_pair.iloc[0,col])!=1:
                    change_str=''
                    sign_ori=0
                    for j in range(2):
                        if pd.isna(temp_pair.iloc[1,3+j])==1:
                            continue
                        elif temp_pair.iloc[0,col].lower()==temp_pair.iloc[1,3+j].lower():
                                sign_ori=1     
                    if sign_ori==0:
                        change_str=change_str+'+'+temp_pair.iloc[0,col]+'/end'+str(evo)
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=change_str
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+change_str
                        
                if pd.isna(temp_pair.iloc[1,col])!=1:
                    change_str=''
                    sign_evo=0
                    for j in range(2):
                        if pd.isna(temp_pair.iloc[0,3+j])==1:
                            continue
                        elif temp_pair.iloc[1,col].lower()==temp_pair.iloc[0,3+j].lower():
                            sign_evo=1
                    if sign_evo==0:
                        change_str=change_str+'+'+temp_pair.iloc[1,col]+'/start'+str(evo)
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=change_str
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+change_str
            elif col in serve:


                if pd.isna(temp_pair.iloc[0,col])!=1:
                    change_str=''
                    sign_ori=0
                    for j in range(2):
                        if pd.isna(temp_pair.iloc[1,6+j])==1:
                            continue
                        elif temp_pair.iloc[0,col].lower()==temp_pair.iloc[1,6+j].lower():
                            sign_ori=1     
                    if sign_ori==0:
                        change_str=change_str+'+'+temp_pair.iloc[0,col]+'/end'+str(evo)

                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=change_str
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+change_str
                        
                if pd.isna(temp_pair.iloc[1,col])!=1:
                    change_str=''
                    sign_evo=0
                    for j in range(2):
                        if pd.isna(temp_pair.iloc[0,6+j])==1:
                            continue
                        elif temp_pair.iloc[1,col].lower()==temp_pair.iloc[0,6+j].lower():
                            sign_evo=1
                    if sign_evo==0:
                        change_str=change_str+'+'+temp_pair.iloc[1,col]+'/start'+str(evo)
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=change_str
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+change_str
            elif col in support:

                if pd.isna(temp_pair.iloc[0,col])!=1:
                    change_str=''
                    sign_ori=0
                    for j in range(7):
                        if pd.isna(temp_pair.iloc[1,8+j])==1:
                            continue
                        elif temp_pair.iloc[0,col].lower()==temp_pair.iloc[1,8+j].lower():
                            sign_ori=1     
                    if sign_ori==0:
                        change_str=change_str+'+'+temp_pair.iloc[0,col]+'/end'+str(evo)
                        if pd.isna(data.iloc[data_index,col].values[0])==1:

                            data.iloc[data_index,col]=change_str
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+change_str
                        
                if pd.isna(temp_pair.iloc[1,col])!=1:
                    change_str=''
                    sign_evo=0
                    for j in range(7):
                        if pd.isna(temp_pair.iloc[0,8+j])==1:
                            continue
                        elif temp_pair.iloc[1,col].lower()==temp_pair.iloc[0,8+j].lower():
                            sign_evo=1
                    if sign_evo==0:
                        change_str=change_str+'+'+temp_pair.iloc[1,col]+'/start'+str(evo)
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=change_str
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+change_str
                            
            else:                  
                if pd.isna(temp_pair.iloc[0,col])!=1:
                    if pd.isna(temp_pair.iloc[1,col])==1:
                        print(1)
                    elif temp_pair.iloc[0,col].lower()!=temp_pair.iloc[1,col].lower():
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=temp_pair.iloc[0,col]+'/end'+str(evo)
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+temp_pair.iloc[0,col]+'/end'+str(evo)
                if pd.isna(temp_pair.iloc[1,col])!=1:
                    if pd.isna(temp_pair.iloc[0,col])==1:
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=temp_pair.iloc[1,col]+'/start'+str(evo)
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+temp_pair.iloc[1,col]+'/start'+str(evo)

                    elif temp_pair.iloc[1,col].lower()!=temp_pair.iloc[0,col].lower():
                        if pd.isna(data.iloc[data_index,col].values[0])==1:
                            data.iloc[data_index,col]=temp_pair.iloc[1,col]+'/start'+str(evo)
                        else:
                            data.iloc[data_index,col]=data.iloc[data_index,col]+'+'+temp_pair.iloc[1,col]+'/start'+str(evo)
    test=data[(data['Entity']=='Vehicle_Auxiliary_Control')]
    return data,test

def unify(data):
    unify_data=data
    unify_data=unify_data.fillna('0')
    unify_data.loc[:,'ats:EvoFrom']=unify_data.loc[:,'ats:EvoFrom']+'+'+unify_data.loc[:,'ats:EvoFrom.1']
    unify_data.loc[:,'Serves']=unify_data.loc[:,'Serves']+'+'+unify_data.loc[:,'Serves.1']
    for i in range(6):
        unify_data.loc[:,'Supports']=unify_data.loc[:,'Supports']+'+'+unify_data.loc[:,'Supports.'+str(i+1)]
    unify_data=unify_data.drop(['ats:EvoFrom.1'],axis=1)
    unify_data=unify_data.drop(['Serves.1'],axis=1)
    support=['Supports.1','Supports.2','Supports.3','Supports.4','Supports.5','Supports.6']
    unify_data=unify_data.drop(support,axis=1)
    return unify_data
#%%

ori_data=pd.read_csv('E:\wenjian\大四下\ATS知识图谱/ATS_T1.csv')
ori_data['start_version']=1
ori_data['end_version']=-1
ori_data['split_from']=''
ori_data['merge_into']=''
ori_data['map_into']=''
diff_12,evo1_data=read_data(1,2,0.5)
diff_23,evo2_data=read_data(2,3,0.5)
update12,t1=update(ori_data,evo1_data,diff_12,1,2)
update23,t2=update(update12,evo2_data,diff_23,2,3)

unify_KG=unify(update23)

#%%
#unify_KG.to_csv('E:\ATS知识图谱/ATS_unify_KG.csv',index=0)
#%%
'''
ori_data=pd.read_csv('E:\ATS知识图谱/ATS_T1.csv')
ori_data['start_version']=1
ori_data['end_version']=-1
ori_data['split_from']=''
ori_data['merge_into']=''
ori_data['map_into']=''
diff_12,evo1_data=read_data(1,2,0.5)

update12=update(ori_data,evo1_data,diff_12,1,2)
'''
