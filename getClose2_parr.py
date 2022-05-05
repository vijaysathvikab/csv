from difflib import get_close_matches
from difflib import SequenceMatcher
import swifter
import pandas as pd
import multiprocessing.dummy as mp
import os
import time
entirestarttime = time.time()
masterlists=[]

for npidata in pd.read_csv("pendingNpiData.csv", index_col=0, iterator=True, chunksize=15000,low_memory=False):#pd.read_csv("pendingNpiData.csv",index_col = 0)
    masterlists.append(list(npidata['fullName']))


print(len(masterlists))



def scoreratio(my_str,best_match):
    return {best_match:SequenceMatcher(None, my_str, best_match).ratio()}



def scoresdict(bestMatchList,my_str):
    scorelist = []
    if bestMatchList:
        for best_match in bestMatchList:
            scorelist.append(scoreratio(my_str,best_match))
    return scorelist

def getkeyfromdict(dfdict):
    try:
        return list(dfdict.keys())[0]
    except:
        return None

def getvaluesfromdict(dfdict):
    try:
        return list(dfdict.values())[0]
    except:
        return None

# output1 = pd.DataFrame()
# output2 = pd.DataFrame()
output3 = pd.DataFrame()

count = 288

# df = pd.read_csv("PendingNamesToBeMatched.csv",index_col=0)
# df.reset_index(drop=True,inplace=True)
# df = df[6150:]
#df.to_csv("PendingNamesToBeMatched.csv")

print("csv, modified and saved")
def par(name, masterlist):
    return get_close_matches(name,masterlist)


for pendingnames in pd.read_csv("PendingNamesToBeMatched.csv", index_col=0, iterator=True, chunksize=50,low_memory=False):
    print(count)
    loopstart = time.time()
    try:
        pendingnames.reset_index(drop=True, inplace=True)
        length = len(pendingnames['Full_Name'])
        pendingnames['nameMatches'] = None
        def parallel(i):
            eachTime = time.time()
            print("Started ",i)
            try:
                print(pendingnames.loc[i,'Full_Name'])
                # with mp.Pool(5) as pro:
                #     print(1)
                concatList = list(map(par,[pendingnames.loc[i,'Full_Name'] for x in range(0,len(masterlists))],masterlists))
                # pro.close()
                # pro.join()
                #print(concatList)
            except Exception as e:
                print(e)
            pendingnames.at[i,'nameMatches'] = [y for x in concatList for y in x]
            eachEndTime = time.time() - eachTime
            #print(pendingnames.loc[i,'nameMatches'])
            print("completed {} in time --> {}".format(i,eachEndTime))
        proc = mp.Pool(7)
        proc.map(parallel,range(0,length))
        proc.close()
        proc.join()
        # pendingnames['nameMatches'] = pendingnames['Full_Name'].swifter.apply(lambda x:get_close_matches(x,masterlist))
        # output1 = pd.concat([output1,pendingnames])
        # pendingnames.to_csv(os.path.join('output1',str(count)+'.csv'))
        print("***************************************************OUTPUT1*******************")
        pendingnames['ScoreDict']=pendingnames.apply(lambda r: scoresdict(r['nameMatches'],r['Full_Name']), axis=1)
        # output2 = pd.concat([output2,pendingnames])
        # pendingnames.to_csv(os.path.join('output2',str(count)+'.csv'))
        print("***************************************************OUTPUT2*******************")
        pendingnames.drop(['nameMatches'],axis=1,inplace=True)
        pendingnames = pendingnames.explode('ScoreDict')
        pendingnames['fullName'] = pendingnames['ScoreDict'].apply(lambda x: getkeyfromdict(x))
        pendingnames['correspondingScore'] = pendingnames['ScoreDict'].apply(lambda x: getvaluesfromdict(x))
        output3 = pd.concat([output3,pendingnames])
        pendingnames.to_csv(os.path.join('output3',str(count)+'.csv'))
        print("***************************************************OUTPUT3*******************")
    except Exception as e:
        print(e)
    count= count+1
    print("the loop end in ",loopstart-time.time())


# output1.to_csv('output1.csv')
# output2.to_csv('output2.csv')
output3.to_csv('output3.csv')

print("total end time is", entirestarttime-time.time())







