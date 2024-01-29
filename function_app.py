
import datetime
import pandas as pd
import numpy as np
import re
# !pip install nltk
import nltk
# nltk.download()
nltk.download('punkt')
nltk.download('stopwords')
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from functools import reduce
import pickle
# !pip install Levenshtein
from Levenshtein import seqratio
# import multiprocessing.pool
from azure.storage.blob import ContainerClient

from io import StringIO
from io import BytesIO
import time
import logging
import azure.functions as func

app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.schedule(schedule="0 30 20 * * 1", 
              arg_name="mytimer",
              run_on_startup=True) 
def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
        
    container_client = ContainerClient.from_connection_string(
    'DefaultEndpointsProtocol=https;AccountName=treewiseblobstorage;AccountKey=jE3f/ogf+EH2cZyJEEagULdbWrIFvtKOnJB655pvrSn+9jzniIx8hGjHlBvnb3Py2I6h7b5zO2NO+AStfk0NPA==;EndpointSuffix=core.windows.net', container_name='maersk-vendor-recommendation-db')

    def read_data_from_blob(dataset_name):
        # try:
        data = pd.read_csv(StringIO(container_client.download_blob(dataset_name).content_as_text()))
        return data

    def read_data_from_blob_parquet(dataset_name):

            
        blob_client = container_client.get_blob_client(blob=dataset_name)
        stream_downloader = blob_client.download_blob()
        stream = BytesIO()
        stream_downloader.readinto(stream)
        processed_df = pd.read_parquet(stream, engine='fastparquet')
        return processed_df

    data = read_data_from_blob_parquet("export")
    data['row_id'] = [i for i in range(1,len(data)+1)]


    data_id  = read_data_from_blob("Maersk_id_export.csv")
    data = data[~data['row_id'].isin(data_id['row_id'])]

    data_id = pd.concat([data_id,data])
    data_id['row_id'] = [i for i in range(1,len(data_id)+1)]
    data_id_csv = data_id.to_csv(index=False)
    data_id_csv_bytes = bytes(data_id_csv, 'utf-8')
    data_id_csv_stream = StringIO(data_id_csv)
    container_client.upload_blob(name='Maersk_id_export.csv', data=data_id_csv_bytes,overwrite=True)
    data_id_csv_stream.close()


    data1 = data.dropna(subset=['Equipment','Item','Maker','Model','Part_Number','Lead_Time','Unit_Price','Delivery_Port_Id','Port'],how='any')
    data1.drop_duplicates(inplace=True)
    data1.reset_index(drop=True,inplace=True)
    df_all = data1
    df_all

    # for preprocessing Equipment name only for filtering main engine data only
    new_vend = []
    ps = PorterStemmer()
    for w in df_all['Equipment']:
        # k = w.replace(',',' ')
        k = re.sub('\W',' ',w).lower() #to remove special charaters
        # new_vend.append(re.sub('\s\s,','',k).strip().lower()) #replace double spaces into single and also trim
        lwr_k = re.sub(r'\s{2,}',' ',k).strip()
        lwr_k = re.sub(r' [nm]o \d+| [nm]\d+ | [nm]\d \d+|^[nm]o | [nm]o | [nm]o$',' ',lwr_k) #removing NO.1 similar patterns
        lwr_k = re.sub(r'^\d*\s*|\s*\d*$','',lwr_k) #remove staring n leading numbers
        lwr_k = re.sub(r'\s{2,}',' ',lwr_k).strip()
        token = word_tokenize(lwr_k) #splitting sentence into words
        stemmed_sentence = reduce(lambda x, y: x + " " + ps.stem(y), token, "") #apply stemming
        new_vend.append(stemmed_sentence.strip())
    # full_frame['EQUIPMENT_processed'] = new_vend
    # full_frame['EQUIPMENT_processed'] = full_frame['EQUIPMENT']
    # full_frame['EQUIPMENT_processed'].replace(new_vend,inplace=True)
    df_all['Equipment_processed'] = new_vend

    for reg in range(len(df_all)):
        # reg1 = re.sub(r'auxiliari engin |ae |a e |auxiliari engin \d+ |ae\d+ |ae \d+ ','auxiliari engin ',full_frame.loc[reg,'Equipment']).strip().lower()
        reg1 = re.sub(r'main engin | me |^me | m e |^m e |main engin \d+ | me\d+ | me \d+ ',' main engin ',df_all.loc[reg,'Equipment_processed']).strip()
        reg1 = re.sub(r'\s{2,}',' ',reg1).strip()
        df_all.loc[reg,'Equipment_processed'] = reg1

    df_all = df_all[df_all['Equipment_processed'].str.contains('main engin ')]
    df_all['Item_mak'] = df_all['Item']+' '+df_all['Maker']

    # for preprocessing Item_mak
    new_vend = []
    ps = PorterStemmer()
    for w in df_all['Item_mak']:
        # k = w.replace(',',' ')
        k = re.sub(r'\W',' ',w).lower() #to remove special charaters
        # new_vend.append(re.sub('\s\s,','',k).strip().lower()) #replace double spaces into single and also trim
        # lwr_k = re.sub(r'\s{2,}',' ',k).strip()
        # lwr_k = re.sub(r' [nm]o \d+| [nm]\d+ | [nm]\d \d+',' ',lwr_k) #removing NO.1 similar patterns
        # lwr_k = re.sub(r'^\d*\s*|\s*\d*$','',lwr_k) #remove staring n leading numbers
        lwr_k = re.sub(r' [nm]o \d+| [nm]\d+ | [nm]\d \d+|^[nm]o | [nm]o | [nm]o$',' ',k)
        lwr_k = re.sub(r'\s{2,}',' ',lwr_k).strip()
        token = word_tokenize(lwr_k) #splitting sentence into words
        stemmed_sentence = reduce(lambda x, y: x + " " + ps.stem(y), token, "") #apply stemming
        new_vend.append(stemmed_sentence.strip())
    # full_frame['EQUIPMENT_processed'] = new_vend
    # full_frame['EQUIPMENT_processed'] = full_frame['EQUIPMENT']
    # full_frame['EQUIPMENT_processed'].replace(new_vend,inplace=True)
    df_all['Item_mak'] = new_vend    

    # for preprocessing Model
    new_vend = []
    ps = PorterStemmer()
    for w in df_all['Model']:
        # k = w.replace(',',' ')
        k = re.sub(r'\W',' ',w).lower() #to remove special charaters
        # new_vend.append(re.sub('\s\s,','',k).strip().lower()) #replace double spaces into single and also trim
        # lwr_k = re.sub(r'\s{2,}',' ',k).strip()
        # lwr_k = re.sub(r' [nm]o \d+| [nm]\d+ | [nm]\d \d+',' ',lwr_k) #removing NO.1 similar patterns
        # lwr_k = re.sub(r'^\d*\s*|\s*\d*$','',lwr_k) #remove staring n leading numbers
        lwr_k = re.sub(r'\s{2,}',' ',k).strip()
        # token = word_tokenize(lwr_k) #splitting sentence into words
        # stemmed_sentence = reduce(lambda x, y: x + " " + ps.stem(y), token, "") #apply stemming
        new_vend.append(lwr_k)
    # full_frame['EQUIPMENT_processed'] = new_vend
    # full_frame['EQUIPMENT_processed'] = full_frame['EQUIPMENT']
    # full_frame['EQUIPMENT_processed'].replace(new_vend,inplace=True)
    df_all['Model_processed'] = new_vend

    # for preprocessing Part_Number
    new_vend = []
    ps = PorterStemmer()
    for w in df_all['Part_Number']:
        # k = w.replace(',',' ')
        k = re.sub('\W','',str(w)).lower() #to remove special charaters
        # new_vend.append(re.sub('\s\s,','',k).strip().lower()) #replace double spaces into single and also trim
        # lwr_k = re.sub(r'\s{2,}',' ',k).strip()
        # lwr_k = re.sub(r' [nm]o \d+| [nm]\d+ | [nm]\d \d+',' ',lwr_k) #removing NO.1 similar patterns
        # lwr_k = re.sub(r'^\d*\s*|\s*\d*$','',lwr_k) #remove staring n leading numbers
        lwr_k = re.sub(r'\s{1,}','',k).strip()
        # token = word_tokenize(lwr_k) #splitting sentence into words
        # stemmed_sentence = reduce(lambda x, y: x + " " + ps.stem(y), token, "") #apply stemming
        new_vend.append(lwr_k)
    # full_frame['EQUIPMENT_processed'] = new_vend
    # full_frame['EQUIPMENT_processed'] = full_frame['EQUIPMENT']
    # full_frame['EQUIPMENT_processed'].replace(new_vend,inplace=True)
    df_all['Part_Number_processed'] = new_vend

    df_all['Item_mak_mod_processed'] = df_all['Item_mak']+' '+df_all['Model_processed']

    df_all.reset_index(drop=True,inplace=True)
    for reg in range(len(df_all)):
        reg1 = re.sub(r'0 ring ','o ring ',df_all.loc[reg,'Item_mak_mod_processed']).strip()
        reg1 = re.sub(r'\s{2,}',' ',reg1).strip()
        df_all.loc[reg,'Item_mak_mod_processed'] = reg1
    df_all['Client'] = 'Maersk'    

    df_all.rename(columns={'Port':'Delivery_Port','Unit_Price':'Po_Unit_Price'},inplace=True)

    # std_path = r'C:\Users\ashin.johnson\Downloads/'
    # std_df = pd.read_csv(std_path+'ME_item_stnd_data_v2_new (1).csv')
    # std_path = r'D:\envs\recommendation\Codes\Item\maresk_recommendation\bala_data2\BSM_MOL_Maersk_Po_Items_Main_Engine_Spares_Script_&_Data_03-jan-2024\BSM_MOL_Maersk_Po_Items_Main_Engine_Spares_Script_&_Data_03-jan-2024/'
    # std_df = pd.read_csv(std_path+'ME_item_stnd_data_v2_new3.csv')
    std_df = read_data_from_blob("ME_item_stnd_data_v2_new3.csv")
    std_df_copy = std_df.copy()
    std_df = std_df.dropna(subset=['Item_mak_mod_processed_uid_80'])

    df_all.reset_index(drop=True,inplace=True) #Grn_Rating
    df_all.dropna(subset=['Client','Equipment','Item','Maker','Model','Part_Number'],how='any',inplace=True) #,'Lead_Time','Po_Unit_Price','Delivery_Port_Id','Delivery_Port'
    df_all.reset_index(drop=True,inplace=True)
    df_all

    for i in range(len(df_all)):#['Item_mak_mod_processed'].unique():
        if df_all.loc[i,'Item_mak_mod_processed']+' '+df_all.loc[i,'Part_Number_processed'] in std_df['Item_Mapps_Id_80'].values:
            print('direct match')
            df_all.loc[i,'Item_mak_mod_processed_uid_80'] = list(std_df[std_df['Item_Mapps_Id_80']==df_all.loc[i,'Item_mak_mod_processed']+' '+df_all.loc[i,'Part_Number_processed']]['Item_mak_mod_processed_uid_80'])[0]
            df_all.loc[i,'Item_Mapps_Id_80'] = df_all.loc[i,'Item_mak_mod_processed']+' '+df_all.loc[i,'Part_Number_processed']
        elif df_all.loc[i,'Item_mak_mod_processed'] in std_df['Item_mak_mod_processed'].values:
            print('partial match 1')
            df_all.loc[i,'Item_mak_mod_processed_uid_80'] = list(std_df[std_df['Item_mak_mod_processed']==df_all.loc[i,'Item_mak_mod_processed']]['Item_mak_mod_processed_uid_80'])[0]
            df_all.loc[i,'Item_Mapps_Id_80'] = df_all.loc[i,'Item_mak_mod_processed_uid_80']+' '+str(df_all.loc[i,'Part_Number_processed'])
            # if df_all.loc[i,'Item_mak_mod_processed_uid_80']+' '+df_all.loc[i,'Part_Number_processed'] in std_df['Item_Mapps_Id_80'].values:
            #     df_all.loc[i,'Item_Mapps_Id_80'] = list(std_df[std_df['Item_Mapps_Id_80']==df_all.loc[i,'Item_mak_mod_processed_uid_80']+' '+df_all.loc[i,'Part_Number_processed']])[0]
            # else: 
            #     df_all.loc[i,'Item_Mapps_Id_80'] = df_all.loc[i,'Item_mak_mod_processed_uid_80']+' '+df_all.loc[i,'Part_Number_processed']
        elif True:
            print('similarity calculation started!!!')
            main_dict = {}
            itm_sim = df_all.loc[i,'Item_mak_mod_processed']
            for j in list(std_df['Item_mak_mod_processed'].unique()):
                main_dict[j] = seqratio(itm_sim.split(),j.split())
            s_dict = {k: v for k,v in main_dict.items() if v>.8} #80%
            if len(s_dict.keys())>=1:
                df_all.loc[i,'Item_mak_mod_processed_uid_80'] = sorted(s_dict.items(),key=lambda x:x[1],reverse=True)[0][0]   
                df_all.loc[i,'Item_Mapps_Id_80'] = df_all.loc[i,'Item_mak_mod_processed_uid_80']+' '+df_all.loc[i,'Part_Number_processed']    
            else:
                df_all.loc[i,'Item_mak_mod_processed_uid_80'] = np.nan
                df_all.loc[i,'Item_Mapps_Id_80'] = np.nan
        else:
                df_all.loc[i,'Item_mak_mod_processed_uid_80'] = np.nan
                df_all.loc[i,'Item_Mapps_Id_80'] = np.nan        
            # def itm_sim(y):
            #     dict1 = {}
            #     dict1[y] = seqratio(df_all.loc[i,'Item_mak_mod_processed'].split(), y.split())
            #     return dict1   
            # if __name__ == "__main__":
            #     pool = multiprocessing.pool.Pool(16)
            #     t1 = time.time()
            #     results = pool.map(itm_sim,list(df_all['Item_mak_mod_processed'].unique()))
            #     pool.close()
            #     pool.join()
            #     print('secs -',time.time()-t1)
            #     for res in results:
            #         main_dict.update(res)
            #     s_dict = {k: v for k,v in main_dict.items() if v>.8} #80%
            #     if len(s_dict.keys())>=1:
            #         df_all.loc[i,'Item_mak_mod_processed_uid_80'] = sorted(s_dict.items(),key=lambda x:x[1],reverse=True)[0][0]   
            #         df_all.loc[i,'Item_Mapps_Id_80'] = df_all.loc[i,'Item_mak_mod_processed_uid_80']+' '+df_all.loc[i,'Part_Number_processed']
            #     df_all.to_csv(r'D:\envs\recommendation\Codes\Item\maresk_recommendation\bala_data2\BSM_MOL_Maersk_Po_Items_Main_Engine_Spares_Script_&_Data_03-jan-2024\BSM_MOL_Maersk_Po_Items_Main_Engine_Spares_Script_&_Data_03-jan-2024\export_new.csv')   
    df_all['Item_Mapps_id'] = 'check'
    df_all['Status'] = np.nan
    df_all['Vendor_Mapps_id'] = 'check'
    df_all['item_firstpart_processed'] = 'check'
    df_all['item_mak_mod'] = 'check'
    df_all['Equipment_std'] = 'check'
    std_df_copy
    std_df_copy = pd.concat([std_df_copy,df_all[std_df_copy.columns]])
    std_df_copy_csv = std_df.to_csv(index=False)
    std_df_copy_csv_bytes = bytes(std_df_copy_csv, 'utf-8')
    std_df_copy_csv_stream = StringIO(std_df_copy_csv)
    container_client.upload_blob(name='ME_item_stnd_data_v2_new3.csv', data=std_df_copy_csv_bytes,overwrite=True)
    std_df_copy_csv_stream.close()      
        
    
    
# import logging
# import azure.functions as func
# import requests
# app = func.FunctionApp()

# @app.schedule(schedule="0 */1 * * * *", arg_name="myTimer", run_on_startup=True,
#               use_monitor=False) 
# def bbb(myTimer: func.TimerRequest) -> None:
#     if myTimer.past_due:
#         logging.info('The timer is past due!')
#     response = requests.get("https://detention-api.azurewebsites.net/test")
#     logging.info('Python timer trigger fwunction executed.')
