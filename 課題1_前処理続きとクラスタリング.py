#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
from sklearn.cluster import KMeans


# ##### 血液データとバイタルデータ、病名データを結合したcsvファイルの読み込み

# In[2]:


vital_blood_entdisease = pd.read_csv('pID_vital_blood_entdisease.csv')


# In[3]:


vital_blood_entdisease


# In[4]:


print(vital_blood_entdisease.isnull().sum())


# ##### 病名の分割はexcelで行い、pID_vital_blood_entdisease_splitという名前のcsvファイルに保存した

# In[5]:


vital_blood_entdisease_split = pd.read_csv('pID_vital_blood_entdisease_split.csv')


# In[6]:


pd.set_option('display.max_columns', 50)


# In[7]:


vital_blood_entdisease_split


# In[8]:


vital_blood_entdisease_split = vital_blood_entdisease_split[['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7', 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15']]
vital_blood_entdisease_split
                                                            


# In[9]:


vital_blood_entdisease_split.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'ent_disease', 'PTINR']
vital_blood_entdisease_split = vital_blood_entdisease_split.drop(index = 0)
vital_blood_entdisease_split


# ##### 病名の列だけ抽出してリスト化し、中身を確認すると改行のために\n, \u3000が含まれている

# In[10]:


ent_disease_list = vital_blood_entdisease_split['ent_disease'].tolist()
ent_disease_list


# ##### \n, \u3000を空欄に置換する

# In[11]:


ent_disease_list_arr = [str(i) for i in ent_disease_list]
string = ",".join(ent_disease_list_arr)
string_new = string.replace('\n', '')
ent_disease_list_replace =  string_new.split(",")
print(ent_disease_list_replace)


# In[12]:


ent_disease_list_arr = [str(i) for i in ent_disease_list_replace]
string = ",".join(ent_disease_list_arr)
string_new = string.replace('\u3000', '')
ent_disease_list_replace =  string_new.split(",")
print(ent_disease_list_replace)


# ##### 上のリストをdataframe型に変換してindexを振り直す

# In[13]:


ent_disease_replace = pd.DataFrame(ent_disease_list_replace)
ent_disease_replace.index = ent_disease_replace.index + 1
ent_disease_replace


# ##### 上記で作成したdataframeを血液、バイタル結合データに結合する

# In[14]:


vital_blood_entdisease_split_replace = pd.concat([vital_blood_entdisease_split, ent_disease_replace], axis = 1)
vital_blood_entdisease_split_replace


# In[15]:


vital_blood_entdisease_split_replace = vital_blood_entdisease_split_replace.drop(['ent_disease'], axis = 1)
vital_blood_entdisease_split_replace.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'PTINR', 'ent_disease']
vital_blood_entdisease_split_replace


# ##### PTINRの項目だけ欠損値が6個あるので、元の血液データファイルを読み込んで、補完する

# In[16]:


vital_blood_entdisease_split_replace.isnull().sum()


# In[17]:


vital_blood_entdisease_split_replace.query('PTINR != PTINR')


# In[18]:


patient_blood = pd.read_csv('icu_blood_test_processed_20180801.csv')


# In[19]:


patient_blood.query('pID == 8144078')['PTINR']


# In[20]:


patient_blood.query('pID == 4441553')['PTINR']


# In[21]:


patient_blood.query('pID == 4560542')['PTINR']


# In[22]:


patient_blood.query('pID == "6836577"')['PTINR']


# In[23]:


PTINR_series = vital_blood_entdisease_split_replace['PTINR']
PTINR_series


# ##### 元の血液データを参照するとpID == 4560542の患者はPTINRの値が欠損しているので、この行をdropする
# ##### pID == 6836577の患者は2.18と0が計測されていたので、2.18を選んで補完した

# In[24]:


PTINR_series = PTINR_series.fillna({77: 0.95, 153: 2.18, 154: 2.18, 197: 0.91, 198: 0.91})
PTINR_series


# In[25]:


vital_blood_entdisease_split_replace = pd.concat([vital_blood_entdisease_split_replace, PTINR_series], axis = 1)
vital_blood_entdisease_split_replace.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'PTINR_drop', 'ent_disease', 'PTINR']
vital_blood_entdisease_split_replace


# In[26]:


vital_blood_entdisease_split_replace_drop = vital_blood_entdisease_split_replace.drop(['PTINR_drop'], axis = 1)
vital_blood_entdisease_split_replace_drop


# ##### 欠損値が無いバイタルデータ、血液データ、病名データ結合のdataframe作成

# In[51]:


vital_blood_entdisease_split_replace_drop = vital_blood_entdisease_split_replace_drop.drop(index = 84)
vital_blood_entdisease_split_replace_drop.reset_index()
vital_blood_entdisease_split_replace_drop.isnull().sum()


# ### クラスタリング（kmeans法を採用した）

# In[27]:


from sklearn.cluster import KMeans


# In[59]:


vital_blood_disease_arr = np.array([vital_blood_entdisease_split_replace_drop['WBC'].tolist(),
                                    vital_blood_entdisease_split_replace_drop['Hb'].tolist(), vital_blood_entdisease_split_replace_drop['PLT'].tolist(),
                                    vital_blood_entdisease_split_replace_drop['AST'].tolist(), vital_blood_entdisease_split_replace_drop['ALT'].tolist(),
                                    vital_blood_entdisease_split_replace_drop['Cre'].tolist(),  vital_blood_entdisease_split_replace_drop['Na'].tolist(),
                                    vital_blood_entdisease_split_replace_drop['K'].tolist(), vital_blood_entdisease_split_replace_drop['CRP'].tolist(),
                                    vital_blood_entdisease_split_replace_drop['BT'].tolist(), vital_blood_entdisease_split_replace_drop['PR'].tolist(),
                                    vital_blood_entdisease_split_replace_drop['RR'].tolist(), vital_blood_entdisease_split_replace_drop['PTINR'].tolist()], np.float32)


# In[60]:


vital_blood_disease_arr_T = vital_blood_disease_arr.T


# In[61]:


pred = KMeans(n_clusters=6).fit_predict(vital_blood_disease_arr_T)
pred


# In[62]:


vital_blood_entdisease_split_replace_drop['cluster_ID'] = pred
vital_blood_entdisease_split_replace_drop


# In[67]:


vital_blood_entdisease_split_replace_drop['cluster_ID'].value_counts()


# In[88]:


vital_blood_entdisease_split_replace_droppID = vital_blood_entdisease_split_replace_drop.drop(['pID', 'ent_disease'], axis = 1)
vital_blood_entdisease_split_replace_droppID


# In[93]:


vital_blood_entdisease_split_replace_droppID[vital_blood_entdisease_split_replace_droppID['cluster_ID'] == 1].mean()


# In[68]:


import matplotlib.pyplot as plt


# In[91]:


clusterinfo = pd.DataFrame()
for i in range(6):
    clusterinfo['cluster' + str(i)] = vital_blood_entdisease_split_replace_droppID[vital_blood_entdisease_split_replace_droppID['cluster_ID'] == i].mean()
clusterinfo = clusterinfo.drop('cluster_ID')
 
my_plot = clusterinfo.T.plot(kind='bar', stacked=True, title="Mean Value of 6 Clusters")
my_plot.set_xticklabels(my_plot.xaxis.get_majorticklabels(), rotation=0)


# In[ ]:




