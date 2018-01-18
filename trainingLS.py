import setGPU
import pandas as pd
import numpy as np
import pickle
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from scipy import stats
import tensorflow as tf
import seaborn as sns
from pylab import rcParams
from keras.layers import concatenate
from keras.layers.advanced_activations import LeakyReLU, ELU
from sklearn.model_selection import train_test_split
from keras.models import Model, load_model
from keras.layers import Input, Dense
from keras.callbacks import ModelCheckpoint, TensorBoard
from keras import regularizers

from sklearn.utils import shuffle
import h5py


sns.set(style='whitegrid', palette='muted', font_scale=1.5)

rcParams['figure.figsize'] = 14, 8

RANDOM_SEED = 42
LABELS = ["Normal", "Anomalous"]



#Choose where to load the files from
# b_h5 = '/eos/cms/store/user/fsiroky/hdf5_data/'
b_h5       = '/eos/cms/store/user/fsiroky/lumih5/'
# b_h5   = '/afs/cern.ch/user/f/fsiroky/public/'
# b_h5 = '/mnt/hdf5test/'
# b_h5   = '/home/test_local/'

pds  = {1: 'BTagCSV', 2: 'BTagMu', 3: 'Charmonium', 4:'DisplacedJet', 5: 'DoubleEG',
        6: 'DoubleMuon', 7: 'DoubleMuonLowMass',
       # 8: 'FSQJets', 9: 'HighMultiplicityEOF', #NOT ENOUGH DATA, NOTEBOOK FAILES
        10: 'HTMHT', 11: 'JetHT', 12: 'MET',
       # 13: 'MinimumBias', #NOT ENOUGH DATA
        14: 'MuonEG', 15: 'MuOnia',
       # 16: 'NoBPTX',
        17: 'SingleElectron', 18: 'SingleMuon', 19: 'SinglePhoton', 20: 'Tau', 21: 'ZeroBias'
}

      
def get_jets(bg_files, bg_jets, sig_files, sig_jets):
    #Use np.empty([0,2802]) for both good and bad jets, if you use b_h5 = '/eos/cms/store/user/fsiroky/hdf5_data/'
    good_jets = np.empty([0,2806])
    bad_jets  = np.empty([0,2806])
                   # Control which time intervals files per PD to load with range in the for loop
    for i in range(0,len(bg_files)):
        try:
            bg_jetfile  = h5py.File(bg_files[i],'r')
            bg_jet      = bg_jetfile[bg_jets[i]][:]
            sig_jetfile = h5py.File(sig_files[i],'r')
            sig_jet     = sig_jetfile[sig_jets[i]][:]

            bad_jets    = np.concatenate((bad_jets, bg_jet), axis=0)
            good_jets = np.concatenate((good_jets, sig_jet), axis=0)
            print( "Number of good lumis: ", len(sig_jet), " Number of bad lumis: ", len(bg_jet)) 

        except OSError as error:
            print("This Primary Dataset doesn't have ", bg_jets[i], error )
            continue
    return good_jets, bad_jets


#Choose which PD to load
nbr = 11 #Jvariable

bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']

bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]

sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']

sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]

#Load good and bad jets
good_jets, bad_jets = get_jets(bg_files, bg_jets, sig_files, sig_jets)



##Choose which PD to load
#nbr = 17 #SingleElectron
#
#bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
#             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']
#
#bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
#             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]
#
#sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
#             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']
#
#sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
#             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]
#
##Load good and bad jets
#good_jets2, bad_jets2 = get_jets(bg_files, bg_jets, sig_files, sig_jets)
#
#
##Choose which PD to load
#nbr = 18 #SingleMuon
#
#bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
#             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']
#
#bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
#             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]
#
#sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
#             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']
#
#sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
#             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]
#
##Load good and bad jets
#good_jets3, bad_jets3 = get_jets(bg_files, bg_jets, sig_files, sig_jets)
#


##Choose which PD to load
#nbr = 21 #ZeroBias
#
#bg_files  = [b_h5+pds[nbr]+'_C_background.h5',b_h5+pds[nbr]+'_D_background.h5', b_h5+pds[nbr]+'_E_background.h5',
#             b_h5+pds[nbr]+'_F_background.h5', b_h5+pds[nbr]+'_G_background.h5', b_h5+pds[nbr]+'_H_background.h5']
#
#bg_jets   = [pds[nbr]+"_C_background", pds[nbr]+"_D_background", pds[nbr]+"_E_background",
#             pds[nbr]+"_F_background", pds[nbr]+"_G_background", pds[nbr]+"_H_background"]
#
#sig_files = [b_h5+pds[nbr]+'_C_signal.h5',b_h5+pds[nbr]+'_D_signal.h5', b_h5+pds[nbr]+'_E_signal.h5',
#             b_h5+pds[nbr]+'_F_signal.h5', b_h5+pds[nbr]+'_G_signal.h5', b_h5+pds[nbr]+'_H_signal.h5']
#
#sig_jets  = [pds[nbr]+"_C_signal", pds[nbr]+"_D_signal", pds[nbr]+"_E_signal",
#             pds[nbr]+"_F_signal", pds[nbr]+"_G_signal", pds[nbr]+"_H_signal"]
#
##Load good and bad jets
#good_jets4, bad_jets4 = get_jets(bg_files, bg_jets, sig_files, sig_jets)



#Assign good jets class label 0
df1 = pd.DataFrame(good_jets)
# cutted_df = df1.iloc[0:25000, :]   #Temporarily to make training faster
# df1 = cutted_df                   #Temporarily to make training faster
df1['class'] = 0

#Assign bad_jets class label  1
df2 = pd.DataFrame(bad_jets)
# cutted_df = df2.iloc[0:, :]    #Temporarily to make training faster
# df2 = cutted_df                   #Temporarily to make training faster
df2['class'] = 1

##Assign good jets class label 0
#df3 = pd.DataFrame(good_jets2)
#
#df3['class'] = 0
#
##Assign bad_jets class label  1
#df4 = pd.DataFrame(bad_jets2)
#
#df4['class'] = 1
#
#
##Assign good jets class label 0
#df5 = pd.DataFrame(good_jets3)
#
#df5['class'] = 0
#
##Assign bad_jets class label  1
#df6 = pd.DataFrame(bad_jets3)
#
#df6['class'] = 1
#
#


#df7 = pd.DataFrame(good_jets4)
#df7['class'] = 0
#
#df8 = pd.DataFrame(bad_jets4)
#df8['class'] = 1



# del(good_jets)
# del(bad_jets)
#Concatenate them
frames  = [df1,df2] 
#frames = [df1,df2,df3,df4,df5,df6]
#frames = [df1,df2,df3,df4,df5,df6,df7,df8]
data   = pd.concat(frames)
del(frames)
# del(df1)
# del(df2)

data.drop(2805, axis=1, inplace=True) #Drop per_pd flags

data = data.sort_values([2800,2801], ascending=[True,True]) #Sort by runID and then by lumiID
data = data.reset_index(drop=True)  #Reset index





# data = data.reindex(index=range(0,len(data)))
#Shuffle them randomly
# data = shuffle(data)
# data = data.reset_index(drop=True)

#Save labels and delete them from df not to cheat during training
# labels = data['class'].astype(int)
# del data['class']


import json

def json_checker(json_file, orig_runid, orig_lumid):
    outcome = 5
    for k,v in json_file.items():
        if (int(k) == orig_runid):
            for d in v: #Checks each inner loop of the json per runID
                for i in range (d[0], d[1]+1):
#                     print("key of json is ", k, " value of json is ", v)
# #                     print(v[0][0], "and", v[0][1])
#                     print("current inner list is", d, "and range is", d[0], " to ", d[1])
#                     print("i is ", i)
                    if i == orig_lumid:
#                         print("Flagging as bad")
                        outcome =0  #0 means good lumi! (to be compatible with code anomaly_detection.ipynb[mse ae])
                        return(outcome)
                
            
        
    outcome = 1 #1 means bad lumisection! (to be compatible with code anomaly_detection.ipynb [mse autoencoder])
    return(outcome)

json_file_path = '/afs/cern.ch/user/f/fsiroky/public/Cert_271036-284044_13TeV_PromptReco_Collisions16_JSON.txt'

def add_flags_from_json(output_json, data):
    output_json = json.load(open(json_file_path))
    new_json_class = np.empty([data.shape[0],1])
    for i in range(0, data.shape[0]):
        orig_runid = data[2800][i]
        orig_runid = int(orig_runid)
        orig_lumid = data[2801][i]
        orig_lumid = int(orig_lumid)
        new_json_class[i,0] = int(json_checker(output_json, orig_runid, orig_lumid))
    data['preco_json'] = new_json_class #PromptReco GOLDEN json
    return data

new_data = add_flags_from_json(json_file_path, data)
 
del(new_data)

# print("Laaalelaaa", data)

# anomalies = data[data['class'] == 1]
# normal    = data[data['class'] == 0]

# print("Number of anomalies: ", anomalies.shape)
# del(anomalies)

# print("Number of normals: ", normal.shape)
# del(normal)




runIDs  = data[2800].astype(int)
lumiIDs = data[2801].astype(int)
lumisections = data[2802].astype(float)

np.save('datarunIDs.npy',  runIDs)
np.save('datalumiIDs.npy', lumiIDs)
np.save('lumisections.npy', lumisections)


print("Save of RunIDs and LumiIDs done")

# print(data)
data.drop(2800, axis=1, inplace=True) #drop RunID before normalizing and training
data.drop(2801, axis=1, inplace=True) #drop LumiID before normalizing and training
print("RunID and LumiID dropped")
# print(data)

from sklearn.preprocessing import StandardScaler, MinMaxScaler, scale, RobustScaler

#Normalize the data to make training better
cutted_data = data.iloc[:, 0:2803]
#classes     = data.iloc[:, 2805:2806] 
classes      = data.iloc[:,-1] #Take PromptReco json


# print(classes.shape)
np_scaled = StandardScaler().fit_transform(cutted_data.values) #gives shitty results

# print("1111",np_scaled)


# np_scaled = scale(cutted_data, axis = 1, with_mean=True, with_std=True, copy=True)
datas = pd.DataFrame(np_scaled)
# datas = pd.DataFrame(np_scaled, index=cutted_data.index, columns=cutted_data.columns)

# print("2222",datas)

# del(np_scaled)
del(cutted_data)
# print("Datas first: ", datas)
datas[2803] = runIDs   #Append runID back after scaling
datas[2804] = lumiIDs  #Append lumiID back after scaling
datas['qlabel'] = classes  #qlabel is goldenJSON now

# print("After scale", datas)


# X_train, X_test = train_test_split(datas, test_size=0.15, random_state=RANDOM_SEED) # This works when we split rndmly
split_nbr = round(datas.shape[0]*0.20)  #0.10 means 10% to the validation set

print(datas.shape)
X_train = datas.iloc[0:(datas.shape[0] - split_nbr) ,:]
X_test  = datas.iloc[(datas.shape[0] - split_nbr): (datas.shape[0]) ,:]
last_train_idx = X_train.shape[0]

np.save('last_train_idx.npy', last_train_idx)
print(X_train.shape)
print(X_test.shape)

del(datas)
X_train = X_train[X_train['qlabel']== 0]
# print(X_train)
X_train = X_train.drop(['qlabel'], axis=1)

ae_lumis = X_train[2800].astype(float)
print("ae lumis", ae_lumis, "ae_lumis shape", ae_lumis.shape)
# print("XTEEEEST before PerPD json beginn")
# print(X_test)


json_file_path_PD = '/afs/cern.ch/user/f/fsiroky/Documents/gen_config/jsons/JetHT.json'

def add_flags_from_json_PD(output_json, X_test):
    output_json = json.load(open(json_file_path))
    new_json_class = np.empty([X_test.shape[0],1])
    for i in range(0, X_test.shape[0]):
        orig_runid = X_test[2803][i+last_train_idx]
        # orig_runid = int(orig_runid)
        orig_lumid = X_test[2804][i+last_train_idx]
        # orig_lumid = int(orig_lumid)
        new_json_class[i,0] = int(json_checker(output_json, orig_runid, orig_lumid))
    X_test['PD_json'] = new_json_class
    return X_test

new_data = add_flags_from_json_PD(json_file_path_PD, X_test)

del(new_data)
# print("Now new X_test label")
# print(X_test)




#y_test = X_test['qlabel']

y_test = X_test['PD_json']

print("Number of good lumis in X_test: ", len(X_test[y_test==0]))
print("Number of bad lumis in X_test: ",  len(X_test[y_test==1]))

X_test.drop(['qlabel'], axis=1, inplace=True)
X_test.drop(['PD_json'], axis=1, inplace=True)


X_train.drop(2803, axis=1, inplace=True) #drop RunID before training
X_train.drop(2804, axis=1, inplace=True) #drop LumiID before training
X_test.drop(2803, axis=1, inplace=True) #drop RunID before training
X_test.drop(2804, axis=1, inplace=True) #drop LumiID before training


# print("X_test before saving: ", X_test)

luminosity_vals = lumisections.iloc[:int(last_train_idx)].values
luminosity_valsTest =  lumisections.iloc[int(last_train_idx):].values

X_train = X_train.values
X_test = X_test.values

np.save('X_testfor3pds_model.npy', X_test)
np.save('y_testfor3pds_model.npy', y_test)

input_dim = X_train.shape[1]
encoding_dim = 50


input_layer = Input(shape=(input_dim, ))


encoder = Dense(2500, activation="LeakyReLU",
                activity_regularizer=regularizers.l1(10e-5)
               )(input_layer)

encoded = Dense(2200, activation="LeakyReLU")(encoder)
encoded = Dense(1600, activation="LeakyReLU")(encoded)
encoded = Dense(1200, activation="LeakyReLU")(encoded)
encoded = Dense(800, activation="LeakyReLU")(encoded)
encoded = Dense(500, activation="LeakyReLU")(encoded)
encoded = Dense(200, activation="LeakyReLU")(encoded)
encoded = Dense(100, activation="LeakyReLU")(encoded)



encoded = Dense(encoding_dim, activation="LeakyReLU")(encoded)

luminosity_neuron = Input(shape=(1,))

luminosity_neuron_dense = Dense(1,)(luminosity_neuron)

x = concatenate([encoded, luminosity_neuron_dense])

# luminosity_neuron = Input(shape=(1,), name='l_neu')
decoded = Dense(100, activation="LeakyReLU")(x)

# x = concatenate([decoded, luminosity_neuron])
decoded = Dense(200, activation="LeakyReLU")(decoded)
decoded = Dense(500, activation="LeakyReLU")(decoded)
decoded = Dense(800, activation="LeakyReLU")(decoded)
decoded = Dense(1200, activation="LeakyReLU")(decoded)
decoded = Dense(1600, activation="LeakyReLU")(decoded)
decoded = Dense(2200, activation="LeakyReLU")(decoded)
decoded = Dense(2500, activation="LeakyReLU")(decoded)


# encoder = Dense(int(encoding_dim / 1.2), activation="relu")(encoder)

# encoder = Dense(int(encoding_dim / 1.5), activation="relu")(encoder)

# decoder = Dense(int(encoding_dim / 1.2), activation='relu')(encoder)

decoder = Dense(input_dim, activation="LeakyReLU")(decoded)

autoencoder = Model(inputs=[input_layer,luminosity_neuron], outputs=decoder)


nb_epoch = 30
batch_size = 1000
from keras.optimizers import Adam
adam = Adam(lr = 0.001, beta_1 = 0.9, beta_2 = 0.999, epsilon=1e-08, decay = 0.0)

autoencoder.compile(optimizer=adam, 
                    loss='mean_squared_error',
#                     metrics=['accuracy']
                   )

checkpointer = ModelCheckpoint(filepath="luminosity_model.h5",
                               verbose=0,
                               save_best_only=False)
# tensorboard = TensorBoard(log_dir='./logs',
#                           histogram_freq=0,
#                           write_graph=True,
#                           write_images=True)

history = autoencoder.fit([X_train,ae_lumis], X_train,
                    epochs=nb_epoch,
                    batch_size=batch_size,
                    shuffle=True,
                    validation_data=([X_test,luminosity_valsTest], X_test),
                    verbose=2,
                    callbacks=[checkpointer
#                               ,tensorboard
                              ]).history

