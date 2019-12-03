import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt

#csvファイルの読み込み
def csv_read():
    patient_problem = pd.read_csv('icu_problem_text_icu_only_20180801.csv')
    patient_blood = pd.read_csv('icu_blood_test_processed_20180801.csv')
    patient_vital = pd.read_csv('icu_vital_processed_20180801.csv')
    original_csv_file = [patient_problem, patient_blood, patient_vital]

    return original_csv_file

#バイタルデータファイルから列の抽出
def pickup_vital(original_csv_file):
    vital_list = []
    patient_vital = original_csv_file[2]
    pID_date_time_BT = patient_vital.loc[:, ['pID', 'date', 'time', 'BT']]
    pID_date_time_PR = patient_vital.loc[:, ['pID', 'date', 'time', 'PR']]
    pID_date_time_RR = patient_vital.loc[:, ['pID', 'date', 'time', 'RR']]
    vital_list.append(pID_date_time_BT)
    vital_list.append(pID_date_time_PR)
    vital_list.append(pID_date_time_RR)

    return vital_list

#血液検査データファイルから列の抽出
def pickup_blood(original_csv_file):
    blood_list = []
    patient_blood = original_csv_file[1]
    date_pID_PTINR = patient_blood[['date', 'pID', 'PTINR']]
    date_pID_Ddimer = patient_blood[['date', 'pID', 'Ddimer']]
    date_pID_BNP = patient_blood[['date', 'pID', 'BNP']]
    date_pID_BloodOthers = patient_blood[['date', 'pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP']]
    blood_list.append(date_pID_PTINR)
    blood_list.append(date_pID_Ddimer)
    blood_list.append(date_pID_BNP)
    blood_list.append(date_pID_BloodOthers)

    return blood_list

#病名データファイルから列の抽出
def pickup_disease(original_csv_file):
    patient_problem = original_csv_file[0]
    patient_disease = patient_problem.loc[:, ['pID', 'ent_date', 'ent_time', 'ent_disease']]

    return patient_disease

#vitalデータの月日時刻をdatetime型に変更    
def To_datetime_vital(vital_list, original_csv_file):
    patient_disease = pickup_disease(original_csv_file)
    patient_disease['ent_datetime'] = pd.to_datetime(patient_disease['ent_date'] + patient_disease['ent_time'], format = '%Y/%m/%d%H:%M:%S')
    for i in range(3):
        vital_list[i]['datetime'] = pd.to_datetime(vital_list[i]['date'] + vital_list[i]['time'], format = '%Y/%m/%d%H:%M:%S')
                                      
    return patient_disease, vital_list

#bloodデータの月日時刻をdatetime型に変更
def To_datetime_blood(blood_list):
    Datetime_list = []
    blood_list_drop = []
    Datetime_blood_list = []

    #dateをdatetime型に変更する
    for i in range(4):
        Datetime_list.append(pd.to_datetime(blood_list[i]['date'], format = '%Y/%m/%d'))

    #上でdatetime型に変更したdateを残し、変更する前のdateをdropする
    for i in range(4):
        blood_list_drop.append(blood_list[i].drop(['date'], axis = 1))
        Datetime_blood_list.append(pd.concat([blood_list_drop[i], Datetime_list[i]], axis = 1))
    Datetime_blood_list[0].columns = ['pID', 'PTINR', 'blood_date']
    Datetime_blood_list[1].columns = ['pID', 'Ddimer', 'blood_date']
    Datetime_blood_list[2].columns = ['pID', 'BNP', 'blood_date']
    Datetime_blood_list[3].columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'blood_date']

    return Datetime_blood_list

#vitalデータと病名ファイルをpIDをkeyにして結合する
def Merge_vital_disease(patient_disease, vital_list):
    vital_disease_list = []
    for i in range(3):
        vital_disease_list.append(pd.merge(vital_list[i], patient_disease, how = 'inner', on = 'pID'))

    vital_disease_list[0].columns = ['pID', 'date', 'time', 'BT', 'datetime', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime']
    vital_disease_list[1].columns = ['pID', 'date', 'time', 'PR', 'datetime', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime']
    vital_disease_list[2].columns = ['pID', 'date', 'time', 'RR', 'datetime', 'ent_date', 'ent_time', 'ent_disease', 'ent_datetime']

    return vital_disease_list
    
                                                    
#vitalデータに対して入室時刻と検査時刻との差を計算して最小値を取得する
def vital_time_interval_get(vital_disease_list):

    vital_time_interval = []
    vital_disease_timeintervalmin_list = []

    #検査時刻と入室時刻との差を列['vital_time_interval']に追加する
    for i in range(3):
        vital_disease_list[i]['vital_time_interval'] = (pd.to_datetime(vital_disease_list[i]['datetime']) - pd.to_datetime(vital_disease_list[i]['ent_datetime'])). astype('timedelta64[m]').abs()

    #上で作成したファイルについて、それぞれBT, PR, RRの列から欠損値をdropする
    vital_disease_list[0] = vital_disease_list[0].dropna(subset = ['BT'])
    vital_disease_list[1] = vital_disease_list[1].dropna(subset = ['PR'])
    vital_disease_list[2] = vital_disease_list[2].dropna(subset = ['RR'])

    #上でdropしたファイルから入室時刻と検査時刻の差のminを取得
    for i in range(3):
        vital_disease_timeintervalmin_list.append(vital_disease_list[i].groupby(['pID'])['vital_time_interval'].min())

    #上で作成したminのseriesを元のファイルに結合
    BT_disease_timeintervalmin = pd.merge(vital_disease_timeintervalmin_list[0], vital_disease_list[0], how = 'inner', on = ['pID', 'vital_time_interval'])
    PR_disease_timeintervalmin = pd.merge(vital_disease_timeintervalmin_list[1], vital_disease_list[1], how = 'inner', on = ['pID', 'vital_time_interval'])
    RR_disease_timeintervalmin = pd.merge(vital_disease_timeintervalmin_list[2], vital_disease_list[2], how = 'inner', on = ['pID', 'vital_time_interval'])
    
    #項目ごとに作業していたのを最後に全て結合
    BT_PR_disease = pd.merge(BT_disease_timeintervalmin, PR_disease_timeintervalmin, how = 'outer', on = 'pID')
    BT_PR_RR_disease = pd.merge(BT_PR_disease, RR_disease_timeintervalmin, how = 'outer', on = 'pID')

    return BT_PR_RR_disease

#上で作成したvitalデータと、datetime型に変更した血液検査データとを結合
def vital_blood_merge(Datetime_blood_list, BT_PR_RR_disease):
    pID_vital_blood = []
    for i in range(4):
        pID_vital_blood.append(pd.merge(Datetime_blood_list[i], BT_PR_RR_disease, how = 'inner', on = 'pID'))

    return pID_vital_blood

#datetime型に変更した血液検査データとvitalデータを結合したデータに対して、入室時刻と検査時刻の差を計算して最小値を取得する
def vital_time_interval_min_get(pID_vital_blood):
    pID_vital_blood_min = []
    
    #入室時刻と検査時刻の差の計算
    for i in range(4):
        pID_vital_blood[i]['blood_time_interval'] = (pd.to_datetime(pID_vital_blood[i]['blood_date']) - pd.to_datetime(pID_vital_blood[i]['ent_datetime'])).astype('timedelta64[D]').abs()

    #それぞれ欠損値のdrop
    pID_vital_blood[0] = pID_vital_blood[0].dropna(subset = ['PTINR'])
    pID_vital_blood[1] = pID_vital_blood[1].dropna(subset = ['Ddimer'])
    pID_vital_blood[2] = pID_vital_blood[2].dropna(subset = ['BNP'])
    pID_vital_blood[3] = pID_vital_blood[3].dropna(subset = ['WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP'])

    #minの取得
    for i in range(4):
        pID_vital_blood_min.append(pID_vital_blood[i].groupby(['pID'])['blood_time_interval'].min())
        
    #取得したminを元のデータに結合
    for i in range(4):
        pID_vital_blood_min.append(pd.merge(pID_vital_blood_min[i], pID_vital_blood[i], how = 'inner', on = ['pID', 'blood_time_interval']))
    pID_vital_blood_min[0] = pID_vital_blood_min[0].drop_duplicates()
    pID_vital_blood_min[1] = pID_vital_blood_min[1].drop_duplicates()
    pID_vital_blood_min[2] = pID_vital_blood_min[2].drop_duplicates()
    pID_vital_blood_min[3] = pID_vital_blood_min[3].drop_duplicates()
        
    #項目ごとに作成していた血液検査データを全て結合する
    pID_vital_PTINR_Ddimer = pd.merge(pID_vital_blood_min[0], pID_vital_blood_min[1], how = 'outer', on = 'pID')
    pID_vital_PTINR_Ddimer_BNP = pd.merge(pID_vital_PTINR_Ddimer, pID_vital_blood_min[2], how = 'outer', on = 'pID')
    pID_vital_PTINR_Ddimer_BNP_bloodothers = pd.merge(pID_vital_PTINR_Ddimer_BNP, pID_vital_blood_min[3], how = 'outer', on ='pID')

    return pID_vital_PTINR_Ddimer_BNP_bloodothers

#vital_blood_entdisease_splitに対してexcelで病名を分割してpID_vital_blood_entdisease.csvとして保存し、読み込み
def csv_read_from_Excel():
    pID_vital_blood_entdisease = pd.read_csv('pID_vital_blood_entdisease_split.csv')
    vital_blood_entdisease_split = pID_vital_blood_entdisease[['Unnamed: 1', 'Unnamed: 2', 'Unnamed: 3', 'Unnamed: 4', 'Unnamed: 5', 'Unnamed: 6', 'Unnamed: 7',
                                                                 'Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10', 'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13', 'Unnamed: 14', 'Unnamed: 15']]
    vital_blood_entdisease_split.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'ent_disease', 'PTINR']
    vital_blood_entdisease_split = vital_blood_entdisease_split.drop(index = 0)
    return vital_blood_entdisease_split


#pID = 8144078, 4441553, 4560542, 6836577の行のPTINRが欠損していたので、元データを辿って値を補完する
#ただし、pID = 4560542の行に関してはPTINRの値がそもそも欠損していたので、行ごと削除することにした

def PTINR_compensate(vital_blood_entdisease_split):
    PTINR_series = vital_blood_entdisease_split['PTINR']
    PTINR_series = PTINR_series.fillna({77: 0.95, 153: 2.18, 154: 2.18, 197: 0.91, 198: 0.91})
    vital_blood_entdisease_split_replace = pd.concat([vital_blood_entdisease_split, PTINR_series], axis = 1)
    vital_blood_entdisease_split_replace.columns = ['pID', 'WBC', 'Hb', 'PLT', 'AST', 'ALT', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'PTINR_drop', 'ent_disease', 'PTINR']
    vital_blood_entdisease_split_replace = vital_blood_entdisease_split_replace.drop(['PTINR_drop'], axis = 1)

    #pID = 4560542の行の削除
    vital_blood_entdisease_split_replace = vital_blood_entdisease_split_replace.drop(index = 84)

    return vital_blood_entdisease_split_replace
    

#KMeansでのクラスタリング
def Cluster_analysis(vital_blood_entdisease_split_replace):
    vital_blood_disease_arr = np.array([vital_blood_entdisease_split_replace['WBC'].tolist(),
                                    vital_blood_entdisease_split_replace['Hb'].tolist(), vital_blood_entdisease_split_replace['PLT'].tolist(),
                                    vital_blood_entdisease_split_replace['AST'].tolist(), vital_blood_entdisease_split_replace['ALT'].tolist(),
                                    vital_blood_entdisease_split_replace['Cre'].tolist(),  vital_blood_entdisease_split_replace['Na'].tolist(),
                                    vital_blood_entdisease_split_replace['K'].tolist(), vital_blood_entdisease_split_replace['CRP'].tolist(),
                                    vital_blood_entdisease_split_replace['BT'].tolist(), vital_blood_entdisease_split_replace['PR'].tolist(),
                                    vital_blood_entdisease_split_replace['RR'].tolist(), vital_blood_entdisease_split_replace['PTINR'].tolist()], np.float32)

    vital_blood_disease_arr_T = vital_blood_disease_arr.T
    vital_blood_entdisease_split_replace['cluster_ID'] = KMeans(n_clusters=6).fit_predict(vital_blood_disease_arr_T)
    vital_blood_entdisease_split_replace = vital_blood_entdisease_split_replace.drop(['pID', 'ent_disease'], axis = 1)

    return vital_blood_entdisease_split_replace

#上のクラスタリング結果を描画する
def Plot(vital_blood_entdisease_split_replace):
    vital_blood_entdisease_split_replace.columns = ['WBC', 'Hb', 'PLT', 'AST', 'ALT,', 'Cre', 'Na', 'K', 'CRP', 'BT', 'PR', 'RR', 'PTINR', 'cluster_ID']
    clusterinfo = pd.DataFrame()
    for i in range(6):
        clusterinfo['cluster' + str(i)] = (vital_blood_entdisease_split_replace.query('cluster_ID == "i"')).mean()
 
    plot = clusterinfo.T.plot(kind='bar', stacked=True, title="Mean Value of 6 Clusters")
    output = plot.set_xticklabels(plot.xaxis.get_majorticklabels(), rotation=0)


    return output

def main():
    original_csv_file = csv_read()
    vital_list = pickup_vital(original_csv_file)
    blood_list = pickup_blood(original_csv_file)
    patient_disease = pickup_disease(original_csv_file)
    patient_disease, vital_list = To_datetime_vital(vital_list, original_csv_file)
    Datetime_blood_list = To_datetime_blood(blood_list)
    vital_disease_list = Merge_vital_disease(patient_disease, vital_list)
    BT_PR_RR_disease = vital_time_interval_get(vital_disease_list)
    pID_vital_blood = vital_blood_merge(Datetime_blood_list, BT_PR_RR_disease)
    pID_vital_PTINR_Ddimer_BNP_bloodothers = vital_time_interval_min_get(pID_vital_blood)
    vital_blood_entdisease_split = csv_read_from_Excel()
    vital_blood_entdisease_split_replace = PTINR_compensate(vital_blood_entdisease_split)
    vital_blood_entdisease_split_replace = Cluster_analysis(vital_blood_entdisease_split_replace)
    output = Plot(vital_blood_entdisease_split_replace)

    return output

if __name__ == "__main__":
    main()



    


    

                                                        


   



                                    

    
