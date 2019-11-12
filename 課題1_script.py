import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

#csvファイルの読み込み
df csv_read():
    patient_problem = pd.read_csv('icu_problem_text_icu_only_20180801.csv')
    patient_blood = pd.read_csv('icu_blood_test_processed_20180801.csv')
    patient_vital = pd.read_csv('icu_vital_processed_20180801.csv')
    original_csv_file = ['patient_problem', 'patient_blood', 'patient_vital']

    return original_csv_file

#バイタルデータファイルから列の抽出
df pickup_vital(original_csv_file):
    vital_list = []
    pID_date_time_BT = original_csv_file[2][['pID', 'date', 'time', 'BT']]
    pID_date_time_PR = original_csv_file[2][['pID', 'date', 'time', 'PR']]
    pID_date_time_RR = original_csv_file[2][['pID', 'date', 'time', 'RR']]
    vital_list = vital_list.append('pID_date_time_BT', 'pID_date_time_PR', 'pID_date_time_RR')

    return vital_list

#血液検査データファイルから列の抽出
df pickup_blood(original_csv_file):
    blood_list = []
    date_pID_PTINR = original_csv_file[1][['date', 'pID', 'PTINR']]
    date_pID_Ddimer = original_csv_file[1][['date', 'pID', 'Ddimer']]
    date_pID_BNP = original_csv_file[1][['date', 'pID', 'BNP']]
    date_pID_BloodOthers = original_csv_file[1][['date', 'pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP']]
    blood_list = blood_list.append('date_pID_PTINR', 'date_pID_Ddimer', 'date_pID_BloodOthers')

    return blood_list

#病名データファイルから列の抽出
df pickup_disease(original_csv_file):
    patient_disease = original_csv_file[0].loc[:, ['pID', 'ent_date', 'ent_time', 'ent_disease']]

    return patient_disease

#vitalデータの月日時刻をdatetime型に変更    
df To_datetime_vital(vital_list):
    patient_disease = pickup_disease(original_csv_file)
    Datetime_vital_list = []
    for i in range(3):
        Datetime_vital_list = Datetime_vital_list.append(pd.to_datetime(vital_list[i]['date'] + vital_list[i]['time'], format = '%Y/%m/%d%H:%M:%S')
    ent_datetime = pd.to_datetime(patient_disease['ent_date'] + patient_disease['ent_time'], format = '%Y/%m/%d%H:%M:%S')
    Datetime_vital_list = Datetime_vital_list.append(ent_datetime)

    return Datetime_vital_list

#bloodデータの月日時刻をdatetime型に変更
df To_datetime_blood(blood_list):
    Datetime_list = []
    blood_list_drop = []
    Datetime_blood_list = []
    for i in range(4):
        Datetime_list = Datetime_list.append(pd.to_datetime(blood_list[i]['date'], format = '%Y/%m/%d'))

    for i in range(4):
        blood_list_drop = blood_list_drop.append(blood_list[i].drop(['date'], axis = 1))
        Datetime_blood_list = Datetime_blood_list.append(pd.concat([blood_list_drop[i], Datetime_list[i]], axis = 1))

    return Datetime_blood_list
                                                        
#vitalデータと病名データを結合したデータの作成
df Merge(Datetime_vital_list):
    patient_disease = pickup_disease(original_csv_file)
    patient_disease_datetime = pd.concat([patient_disease, Datetime_vital_list[3]], axis = 1)
    vital_merge_list = []
    vital_disease_list = []
    vital_list = pickup_vital(original_csv_file)
    for i in range(3):
        vital_merge_list = vital_datetime_list.append(pd.concat([vital_list[i], Datetime_vital_list[i]], axis = 1))

    for i in range(3):
        vital_disease_list = vital_disease_list.append(pd.merge(vital_merge_list[i], patient_disease_datetime, how = 'inner', on = 'pID')) 

    return vital_disease_list

#vitalデータに対して入室時刻と検査時刻との差を計算して最小値を取得する
df vital_time_interval_get(vital_disease_list):
    vital_time_interval = []
    pID_disease_vital_time_interval = []
    pID_disease_vital_time_interval_min = []
    
    for i in range(3):
        vital_time_interval = vital_time_interval.append((vital_disease_list[i]['datetime'] - vital_disease_list[i]['ent_datetime']). astype('timedelta[m]').abs())

    for i in range(3):
        pID_disease_vital_time_interval = pID_disease_vital_time_interval.append(pd.concat([vital_disease_list[i], vital_time_interval[i]], axis = 1))

    pID_disease_vital_time_interval[0] = pID_disease_vital_time_interval[0].dropna(subset = ['BT'])
    pID_disease_vital_time_interval[1] = pID_disease_vital_time_interval[1].dropna(subset = ['PR'])
    pID_disease_vital_time_interval[2] = pID_disease_vital_time_interval[2].dropna(subset = ['RR'])

    #minを取得
    pID_disease_BT_time_interval_min = pID_disease_vital_time_interval[0].groupby(['pID'])['time_interval_BT'].min()
    pID_disease_PR_time_interval_min = pID_disease_vital_time_interval[1].groupby(['pID'])['time_interval_PR'].min()
    pID_disease_RR_time_interval_min = pID_disease_vital_time_interval[2].groupby(['pID'])['time_interval_RR'].min()

    pID_disease_BT_time_interval_min = pd.merge(pID_disease_BT_time_interval_min, pID_disease_vital_time_interval[0], how = 'inner', on = ['pID', 'time_interval_BT'])
    pID_disease_PR_time_interval_min = pd.merge(pID_disease_PR_time_interval_min, pID_disease_vital_time_interval[1], how = 'inner', on = ['pID', 'time_interval_PR'])
    pID_dsiease_RR_time_interval_min = pd.merge(pID_disease_RR_time_interval_min, pID_disease_vital_time_interval[2], how = 'inner', on = ['pID', 'time_interval_RR'])

    #項目ごとに作業していたのを最後に全て結合
    pID_disease_BT_PR = pd.merge(pID_disease_BT_time_interval_min, pID_disease_PR_time_interval_min, how = 'outer', on = 'pID')
    pID_disease_BT_PR_RR = pd.merge(pID_disease_BT_PR, pID_disease_RR_time_interval_min, how = 'outer', on = 'pID')

#上で作成したvitalデータと、datetime型に変更した血液検査データとを結合
df vital_blood_merge(Datetime_blood_list, pID_disease_BT_PR_RR):
    pID_vital_blood = []
    for i in range(4):
        pID_vital_blood = pID_vital_blood.append(pd.merge(Datetime_blood_list[i], pID_disease_BT_PR_RR[i], how = 'inner', on = 'pID'))

    return pID_vital_blood

#datetime型に変更した血液検査データとvitalデータを結合したデータに対して、入室時刻と検査時刻の差を計算して最小値を取得する
df vital_time_interval_get(pID_vital_blood):
    pID_vital_blood_min = []
    pID_vital_blood_min_merge = []
    
    #入室時刻と検査時刻の差の計算
    for i in range(4):
        pID_vital_blood[i]['blood_time_interval'] = (pID_vital_blood[i]['date'] - pID_vital_blood[i]['ent_datetime']).astype('timedelta64[D]').abs()

    #それぞれ欠損値のdrop
    pID_vital_blood[0] = pID_vital_blood.dropna(subset = ['PTINR'])
    pID_vital_blood[1] = pID_vital_blood.dropna(subset = ['Ddimer'])
    pID_vital_blood[2] = pID_vital_blood.dropna(subset = ['BNP'])
    pID_vital_blood[3] = pID_vital_blood.dropna(subset = ['WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP'])

    #minの取得
    for i in range(4):
        pID_vital_blood_min = pID_vital_blood_min.append(pID_vital_blood_min.groupby(['pID'])['blood_time_interval'].min())

    #取得したminを元のデータに結合
    for i in range(4):
        pID_vital_blood_min_merge = pID_vital_blood_min.append(pd.merge(pID_vital_blood_min[i], pID_vital_blood[i], how = 'inner', on = ['pID', 'blood_time_interval']))

    #項目ごとに作成していた血液検査データを全て結合する
    pID_vital_PTINR_Ddimer = pd.merge(pID_vital_blood_min_merge[0], pID_vital_blood_min_merge[1], how = 'outer', on = 'pID')
    pID_vital_PTINR_Ddimer_BNP = pd.merge(pID_vital_PTINR_Ddimer, pID_vital_blood_min_merge[2], how = 'outer', on = 'pID')
    pID_vital_PTINR_Ddimer_BNP_bloodothers = pd.merge(pID_vital_PTINR_Ddimer_BNP, pID_vital_blood_min_merge[3], how = 'outer', on ='pID')

    return pID_vital_PTINR_Ddimer_BNP_bloodothers

df csv_read_from_Excel():
    pID_vital_blood_entdisease = pd.read_csv('pID_vital_blood_entdisease.csv')
    return pID_vital_blood_entdisease

#pID_vital_PTINR_Ddimer_BNP_bloodothersに対してexcelで病名を分割してpID_vital_blood_entdisease.csvとして保存した
#pID = 8144078, 4441553, 4560542, 6836577の行のPTINRが欠損していたので、元データを辿って値を補完する
#ただし、pID = 4560542の行に関してはPTINRの値がそもそも欠損していたので、行ごと削除することにした

df PTINR_compensate(pID_vital_blood_entdisease):
    PTINR_series = pID_vital_blood_entdisease['PTINR']
    PTINR_series = PTINR_series.fillna({77: 0.95, 153: 2.18, 154: 2.18, 197: 0.91, 198: 0.91})
    pID_vital_blood_entdisease_replace = pd.concat([pID_vital_blood_entdisease, PTINR_series], axis = 1)
    pID_vital_blood_entdisease_replace.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'PTINR_drop', 'ent_disease', 'PTINR']
    pID_vital_blood_entdisease_replace = pID_vital_blood_entdisease_replace.drop(['PTINR_drop'], axis = 1)

    return pID_vital_blood_entdisease_replace
    

#KMeansでのクラスタリング
df KMeans(pID_vital_blood_entdisease_replace):
    vital_blood_disease_arr = np.array([pID_vital_blood_entdisease_replace['WBC'].tolist(),
                                    pID_vital_blood_entdisease_replace['Hb'].tolist(), pID_vital_blood_entdisease_replace['PLT'].tolist(),
                                    pID_vital_blood_entdisease_replace['AST'].tolist(), pID_vital_blood_entdisease_replace['ALT'].tolist(),
                                    pID_vital_blood_entdisease_replace['Cre'].tolist(),  pID_vital_blood_entdisease_replace['Na'].tolist(),
                                    pID_vital_blood_entdisease_replace['K'].tolist(), pID_vital_blood_entdisease_replace['CRP'].tolist(),
                                    pID_vital_blood_entdisease_replace['BT'].tolist(), pID_vital_blood_entdisease_replace['PR'].tolist(),
                                    pID_vital_blood_entdisease_replace['RR'].tolist(), pID_vital_blood_entdisease_replace['PTINR'].tolist()], np.float32)

    vital_blood_disease_arr_T = vital_blood_disease_arr.T
    pred = KMeans(n_clusters=6).fit_predict(vital_blood_disease_arr_T)
    pID_vital_blood_entdisease_replace['cluster_ID'] = pred
    pID_vital_blood_entdisease_replace = vital_blood_entdisease_split_replace_drop.drop(['pID', 'ent_disease'], axis = 1)

    return pID_vital_blood_entdisease_replace

#上のクラスタリング結果を描画する
df Plot(pID_vital_blood_entdisease_replace):
    clusterinfo = pd.DataFrame()
    for i in range(6):
        clusterinfo['cluster' + str(i)] = pID_vital_blood_entdisease_replace[pID_vital_blood_entdisease_replace['cluster_ID'] == i].mean()
        clusterinfo = clusterinfo.drop('cluster_ID')
 
    plot = clusterinfo.T.plot(kind='bar', stacked=True, title="Mean Value of 6 Clusters")

    return plot.set_xticklabels(plot.xaxis.get_majorticklabels(), rotation=0)

    
    

    
    


    

                                                        


   



                                    

    
