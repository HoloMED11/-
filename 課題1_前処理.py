#!/usr/bin/env python
# coding: utf-8

# <br />
# 
# <div style="text-align: center;">
# <font size="7">課題１レポート</font>
# </div>
# <br />
# <div style="text-align: right;">
# <font size="4">Masaki Ozawa</font>
# </div>
# 
# <br />

# # 前処理
# 
# ### 入室時刻に最も近い時刻に検査している血液データとバイタルデータの抽出
# ##### ①必要な項目を血液データとバイタルデータから抽出する
# ##### ②それぞれの項目ごとにpIDとdata, Timeと一緒に抽出し、pIDをkeyとして病名ファイルと結合する
# ##### ③ent_dateと検査時のdateの時間差を計算する
# ##### ④欠損値を排除したのち、上の計算結果の最小値を取得することで、入室時に最も近い検査値を取得する
# ##### ⑤それぞれの項目に対して上の操作を行なったのち、全て結合する
# ### 病名が二つ以上同じ行に入力されている場合、一つ一つ異なる行に分割する
# ##### Excelにて病名を分ける作業を行なった

# In[1]:


import numpy as np
import pandas as pd


# In[2]:


#病名データ、血液データ、バイタルデータの３つのcsvファイルの読み込み
patient_problem = pd.read_csv('icu_problem_text_icu_only_20180801.csv')
patient_blood = pd.read_csv('icu_blood_test_processed_20180801.csv')
patient_vital = pd.read_csv('icu_vital_processed_20180801.csv')


# In[3]:


#patinet_problemから必要な列の抽出
patient_disease = patient_problem.loc[:, [ 'pID','ent_date', 'ent_time', 'ent_disease']]
patient_disease.head()


# ##### 必要な項目を血液データとバイタルデータから抽出する

# In[4]:


#patient_bloodから列の抽出
patient_blood_pickup = patient_blood.loc[:, ['date', 'Time', 'pID', 'pO2', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'PTINR', 'Ddimer','BNP']]
patient_blood_pickup


# ##### 必要な項目を抜き出したのち、各項目についてpIDとdateと一緒に抽出する
# ##### 血液データについては、PTINRとDdimerとBNPが欠損値を含み、それ以外の項目は含んでいなかったので、その他の項目は一括で抽出した

# In[17]:


patient_vital = patient_vital.loc[:, ['pID', 'date', 'time','BT', 'PR', 'RR']]
pID_date_time_BT = patient_vital[['pID', 'date', 'time', 'BT']]
pID_date_time_PR = patient_vital[['pID', 'date', 'time', 'PR']]
pID_date_time_RR = patient_vital[['pID', 'date', 'time', 'RR']]
pID_date_time_BT


# In[18]:


date_pID_PTINR = patient_blood_pickup[['date', 'pID', 'PTINR']]
date_pID_Ddimer = patient_blood_pickup[['date', 'pID', 'Ddimer']]
date_pID_BNP = patient_blood_pickup[['date', 'pID', 'BNP']]
date_pID_BloodOthers = patient_blood_pickup[['date', 'pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP']]
date_pID_PTINR


# ##### バイタルデータに関して、入室時刻と検査時刻の差分を計算できるように、それぞれdatetime型に変換する

# In[19]:


#date, time, ent_date, ent_timeをdatetime型に変換
BT_Datetime = pd.to_datetime(pID_date_time_BT['date'] + pID_date_time_BT['time'], format = '%Y/%m/%d%H:%M:%S')
PR_Datetime = pd.to_datetime(pID_date_time_PR['date'] + pID_date_time_PR['time'], format = '%Y/%m/%d%H:%M:%S')
RR_Datetime = pd.to_datetime(pID_date_time_RR['date'] + pID_date_time_RR['time'], format = '%Y/%m/%d%H:%M:%S')
ent_datetime = pd.to_datetime(patient_disease['ent_date'] + patient_disease['ent_time'], format = '%Y/%m/%d%H:%M:%S')
BT_Datetime


# ##### 上でdatetime型に変換した各データを、各項目ごとの抽出した元のファイルに結合する

# In[20]:


pID_datetime_BT = pd.concat([pID_date_time_BT, BT_Datetime], axis = 1)
pID_datetime_PR = pd.concat([pID_date_time_PR, PR_Datetime], axis = 1)
pID_datetime_RR = pd.concat([pID_date_time_RR, RR_Datetime], axis = 1)
patient_disease_datetime = pd.concat([patient_disease, ent_datetime], axis = 1)
pID_datetime_BT.columns = ['pID', 'date', 'time', 'BT', 'datetime']
pID_datetime_PR.columns = ['pID', 'date', 'time', 'PR', 'datetime']
pID_datetime_RR.columns = ['pID', 'date', 'time', 'RR', 'datetime']
patient_disease_datetime.columns = ['pID', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime']
pID_datetime_BT


# ##### datetime型に変換した時刻データを結合した上のファイルと、病名ファイルをkeyをpIDとしてmergeする

# In[8]:


pID_disease_datetime_BT = pd.merge(pID_datetime_BT, patient_disease_datetime, how = 'inner', on = 'pID')
pID_disease_datetime_PR = pd.merge(pID_datetime_PR, patient_disease_datetime, how = 'inner', on = 'pID')
pID_disease_datetime_RR = pd.merge(pID_datetime_RR, patient_disease_datetime, how = 'inner', on = 'pID')
pID_disease_datetime_RR


# ##### バイタルデータの各項目に対して、入室時刻との差分を計算する
# ##### 最も入室時刻に近い値を抽出したいので、差分の絶対値をとる
# ##### 絶対値をとったデータを上のファイルに結合する

# In[ ]:


time_interval_BT = (pID_disease_datetime_BT['datetime'] - pID_disease_datetime_BT['ent_datetime']). astype('timedelta64[m]').abs()
time_interval_PR = (pID_disease_datetime_PR['datetime'] - pID_disease_datetime_PR['ent_datetime']). astype('timedelta64[m]').abs()
time_interval_RR = (pID_disease_datetime_RR['datetime'] - pID_disease_datetime_RR['ent_datetime']). astype('timedelta64[m]').abs()
pID_disease_BT_interval = pd.concat([pID_disease_datetime_BT, time_interval_BT], axis = 1)
pID_disease_PR_interval = pd.concat([pID_disease_datetime_PR, time_interval_PR], axis = 1)
pID_disease_RR_interval = pd.concat([pID_disease_datetime_RR, time_interval_RR], axis = 1)


# ##### 列名の変更

# In[ ]:


pID_disease_BT_interval.columns = ['pID', 'date', 'time', 'BT', 'datetime', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime', 'time_interval_BT']
pID_disease_PR_interval.columns = ['pID', 'date', 'time', 'PR', 'datetime', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime', 'time_interval_PR']
pID_disease_RR_interval.columns = ['pID', 'date', 'time', 'RR', 'datetime', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime', 'time_interval_RR']


# ##### 入室時刻に最も近い検査値が欠損値で無いように、最小値を取得する前に欠損値をdropする

# In[ ]:


pID_disease_BT_interval = pID_disease_BT_interval.dropna(subset = ['BT'])
pID_disease_PR_interval = pID_disease_PR_interval.dropna(subset = ['PR'])
pID_disease_RR_interval = pID_disease_RR_interval.dropna(subset = ['RR'])


# In[9]:


pID_disease_BT_interval


# ##### 上で求めた時間差に対して最小値を取得する

# In[10]:


time_interval_BT_min = pID_disease_BT_interval.groupby(['pID'])['time_interval_BT'].min()
time_interval_PR_min = pID_disease_PR_interval.groupby(['pID'])['time_interval_PR'].min()
time_interval_RR_min = pID_disease_RR_interval.groupby(['pID'])['time_interval_RR'].min()
time_interval_PR_min_dataframe = pd.DataFrame(time_interval_PR_min)
time_interval_PR_min_dataframe.query('pID == 6142751')


# ##### 上で取得した時間差の最小値データを元の血液データファイルに結合する

# In[11]:


pID_disease_BT_intervalmin = pd.merge(time_interval_BT_min, pID_disease_BT_interval, how = 'left', on = ['pID', 'time_interval_BT'])
pID_disease_PR_intervalmin = pd.merge(time_interval_PR_min, pID_disease_PR_interval, how = 'inner', on = ['pID', 'time_interval_PR'])
pID_disease_RR_intervalmin = pd.merge(time_interval_RR_min, pID_disease_RR_interval, how = 'left', on =['pID',  'time_interval_RR'])
pID_disease_RR_intervalmin


# ##### 必要な列の抽出

# In[12]:


pID_disease_BT_pickup = pID_disease_BT_intervalmin[['pID', 'BT', 'ent_disease', 'ent_datetime']]
pID_disease_PR_pickup = pID_disease_PR_intervalmin[['pID', 'PR', 'ent_disease', 'ent_datetime']]
pID_disease_RR_pickup = pID_disease_RR_intervalmin[['pID', 'RR', 'ent_disease', 'ent_datetime']]


# ##### 各項目抽出した３つのファイルを結合する

# In[13]:


pID_disease_vital = pd.merge(pID_disease_BT_pickup, pID_disease_PR_pickup, how = 'outer', on = 'pID')
pID_disease_vital_second = pd.merge(pID_disease_vital, pID_disease_RR_pickup, how = 'outer', on = 'pID')


# ##### 病名、各データに関して重複があったので、重複の削除

# In[15]:


pID_disease_vital_second_duplicate = pID_disease_vital_second.drop_duplicates()


# In[16]:


pID_disease_vital_second_duplicate = pID_disease_vital_second_duplicate.dropna(subset = ['ent_disease'])
pID_disease_vital_second_duplicate


# In[18]:


pID_disease_vital_second_duplicate_drop = pID_disease_vital_second_duplicate.drop(['ent_disease_x', 'ent_datetime_x', 'ent_disease_y', 'ent_datetime_y'], axis = 1)
pID_disease_vital_second_duplicate_drop


# ##### 血液データに関してもバイタルデータの時と同様、datetime型に変更して入室時刻との差分を計算後、欠損値をdropしたのち、最小値を取得する

# ##### dateをdatetime型に変更する

# In[19]:


PTINR_date = pd.to_datetime(date_pID_PTINR['date'], format = '%Y/%m/%d')
Ddimer_date = pd.to_datetime(date_pID_Ddimer['date'], format = '%Y/%m/%d')
BNP_date = pd.to_datetime(date_pID_BNP['date'], format = '%Y/%m/%d')
BloodOthers_date = pd.to_datetime(date_pID_BloodOthers['date'], format = '%Y/%m/%d')


# ##### datetime型に変更する前のdateをdropする

# In[20]:


date_pID_PTINR_drop = date_pID_PTINR.drop(['date'], axis = 1)
date_pID_Ddimer_drop = date_pID_Ddimer.drop(['date'], axis = 1)
date_pID_BNP_drop = date_pID_BNP.drop(['date'], axis = 1)
date_pID_BloodOthers_drop = date_pID_BloodOthers.drop(['date'], axis = 1)


# In[21]:


date_pID_PTINR_merge = pd.concat([date_pID_PTINR_drop, PTINR_date], axis = 1)
date_pID_Ddimer_merge = pd.concat([date_pID_Ddimer_drop, Ddimer_date], axis = 1)
date_pID_BNP_merge = pd.concat([date_pID_BNP_drop, BNP_date], axis = 1)
date_pID_BloodOthers_merge = pd.concat([date_pID_BloodOthers_drop, BloodOthers_date], axis = 1)
date_pID_PTINR_merge


# ##### datatime型に変更した上のデータを前に作成したバイタルデータに結合する

# In[22]:


pID_vital_PTINR = pd.merge(date_pID_PTINR_merge, pID_disease_vital_second_duplicate_drop, how = 'inner', on = 'pID')
pID_vital_Ddimer = pd.merge(date_pID_Ddimer_merge, pID_disease_vital_second_duplicate_drop, how = 'inner', on = 'pID')
pID_vital_BNP = pd.merge(date_pID_BNP_merge, pID_disease_vital_second_duplicate_drop, how = 'inner', on = 'pID')
pID_vital_BloodOthers = pd.merge(date_pID_BloodOthers_merge, pID_disease_vital_second_duplicate_drop, how = 'inner', on = 'pID')
pID_vital_PTINR


# ##### 入室時刻と検査時の時刻の差分を計算して、欠損値をdropする

# In[26]:


#ent_datetimeとdateから入室してから計測するまでの時間を計算し、最も入室時刻に近い値を抽出する
pID_vital_PTINR['blood_time_interval'] = (pID_vital_PTINR['date'] - pID_vital_PTINR['ent_datetime']).astype('timedelta64[D]').abs()
pID_vital_Ddimer['blood_time_interval'] = (pID_vital_Ddimer['date'] - pID_vital_Ddimer['ent_datetime']).astype('timedelta64[D]').abs()
pID_vital_BNP['blood_time_interval'] = (pID_vital_BNP['date'] - pID_vital_BNP['ent_datetime']).astype('timedelta64[D]').abs()
pID_vital_BloodOthers['blood_time_interval'] = (pID_vital_BloodOthers['date'] - pID_vital_BloodOthers['ent_datetime']).astype('timedelta64[D]').abs()

#欠損値をdropする
pID_vital_PTINR_drop = pID_vital_PTINR.dropna(subset = ['PTINR'])
pID_vital_Ddimer_drop = pID_vital_Ddimer.dropna(subset = ['Ddimer'])
pID_vital_BNP_drop = pID_vital_BNP.dropna(subset = ['BNP'])
pID_vital_BloodOthers_drop = pID_vital_BloodOthers.dropna(subset = ['WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP'])
pID_vital_BloodOthers_drop


# ##### 上の差分の最小値を取得する

# In[27]:


#blood_time_interval_min = patient_vital_blood_merge.groupby(['pID'])['blood_time_interval'].min()
#patient_vital_blood_merge = pd.merge(blood_time_interval_min, patient_vital_blood_merge, how = 'inner', on = 'blood_time_interval')
#patient_vital_blood_merge
pID_vital_PTINR_min = pID_vital_PTINR.groupby(['pID'])['blood_time_interval'].min()
pID_vital_Ddimer_min = pID_vital_Ddimer.groupby(['pID'])['blood_time_interval'].min()
pID_vital_BNP_min = pID_vital_BNP.groupby(['pID'])['blood_time_interval'].min()
pID_vital_BloodOthers_min = pID_vital_BloodOthers.groupby(['pID'])['blood_time_interval'].min()


# In[31]:


pID_vital_PTINR_min_merge = pd.merge(pID_vital_PTINR_min, pID_vital_PTINR_drop, how = 'inner', on = ['pID', 'blood_time_interval'])
pID_vital_Ddimer_min_merge = pd.merge(pID_vital_Ddimer_min, pID_vital_Ddimer_drop, how = 'inner', on = ['pID', 'blood_time_interval'])
pID_vital_BNP_min_merge = pd.merge(pID_vital_BNP_min, pID_vital_BNP_drop, how = 'inner', on = ['pID', 'blood_time_interval'])
pID_vital_BloodOthers_min_merge = pd.merge(pID_vital_BloodOthers_min, pID_vital_BloodOthers_drop, how = 'inner', on = ['pID', 'blood_time_interval'])


# ##### 重複を削除する

# In[36]:


pID_vital_PTINR_min_merge_duplicate = pID_vital_PTINR_min_merge.drop_duplicates()
pID_vital_Ddimer_min_merge_duplicate = pID_vital_Ddimer_min_merge.drop_duplicates()
pID_vital_BNP_min_merge_duplicate = pID_vital_BNP_min_merge.drop_duplicates()
pID_vital_BloodOthers_min_merge_duplicate = pID_vital_BloodOthers_min_merge.drop_duplicates()
pID_vital_BloodOthers_min_merge_duplicate


# ##### 項目ごとに作成していた血液データを全て結合する

# In[37]:


pID_BloodOthers_PTINR_merge = pd.merge(pID_vital_BloodOthers_min_merge_duplicate, pID_vital_PTINR_min_merge_duplicate, how = 'outer', on = 'pID')
pID_BloodOthers_PTINR_Ddimer_merge = pd.merge(pID_BloodOthers_PTINR_merge, pID_vital_Ddimer_min_merge_duplicate, how = 'outer', on = 'pID')
pID_vital_merge = pd.merge(pID_vital_BloodOthers_min_merge_duplicate, pID_vital_PTINR_min_merge_duplicate, how = 'outer', on = 'pID')
pID_vital_merge


# In[38]:


pd.set_option('display.max_columns', 50)
pID_vital_merge


# In[40]:


pID_vital_merge_drop = pID_vital_merge.drop(['date_x', 'ent_datetime_x', 'blood_time_interval_y', 'blood_time_interval_x', 'date_y', 'BT_y', 'PR_y', 'RR_y', 'ent_disease_y', 'ent_datetime_y'], axis = 1)
pID_vital_merge_drop.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'ent_disease', 'PTINR']
pID_vital_merge_drop


# ##### 血液データ、バイタルデータ、病名データを結合したcsvファイルの出力

# In[41]:


pID_vital_merge_drop.to_csv('pID_vital_blood_entdisease.csv')


# In[ ]:




