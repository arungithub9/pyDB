#am10182
#ddv246
#database project : please read documentaion.txt for more info
import operator
import sys
import re
import numpy as np
import time
from BTrees.OOBTree import *

tables = {} #entire database

hashtIndexes = {} #global hashtable indexes

btreeIndexes = {} #global btree indexes

def removeColumn(tabl):
    newTbl = {}
    for d in tabl.keys():
        col = tabl[d]
        col = np.delete(col,1)
        newTbl[d] = col
    return newTbl

def fileToStructND(fileName):
    file = open(fileName,'r')
    file.readline()
    colTypes = []
    firstRecord = file.readline()
    
    firstRecord = firstRecord.split('|')
    for i in firstRecord:
        try:
            x = int(i)
            colTypes.append('f8')
        except ValueError:
            try:
                x = float(i)
                colTypes.append('f8')
            except ValueError:
                colTypes.append('U25')
    file.close()
    data = np.genfromtxt(fileName, dtype=colTypes, delimiter='|',names=True)
    return data

def inputFromFile(fileName):
    
    data = fileToStructND(fileName)

    table = {}

    for i in data.dtype.names:
        table[i] = data[i]

    return table

def writeToFile(fileName,tableName):
    tbl = tables[tableName]
    f = open(fileName,"w")
    totalColumns = len(tbl)
    ct = 1
    for columnNames in tbl:
        f.write(columnNames)
        if ct == totalColumns:
            ct = 1
        else:
            f.write("|")
            ct = ct+1
    f.write("\n")
    temp = list(tbl.keys())[0]
    temp = tbl[temp].size

    for i in range (0,temp):
        ct = 0
        for columnNames in tbl:
            x = tbl[columnNames]
            f.write(str(x[i]))
            ct = ct + 1
            if(ct!=totalColumns):
                f.write("|")
        ct = 0
        f.write("\n")

def select(tableVar,cond):
    #will return a bool ndarray for points of match
    #possible commands:
    # x > 5 #case3
    # x[+3] > 5 #CASE 2
    # 5 > x #case4
    # 7 > x[+3] #CASE 1

    col = ""
    RelOp = ""
    ConOp =""
    numMain = ""
    numInBracks = ""
    case =""
    #find case
    if re.search(".+>.+\[|.+<.+\[|.+=.+\[|.+!.+\[",cond):           #case1
        numMain = re.search(".+>|.+<|.+=|.+!",cond).group(0)[:-1]
        ConOp = re.search("\[.+\]",cond).group(0)[1]
        temp = "\\"+ConOp + ".+\]"
        numInBracks = re.search(temp,cond).group(0)[1:-1]
        if re.search(">=",cond):
            RelOp = ">="
        elif re.search("<=",cond):
            RelOp = "<="
        elif re.search("!=",cond):
            RelOp = "!=" 
        elif re.search("<",cond):
            RelOp = "<"
        elif re.search(">",cond):
            RelOp = ">"
        else:
            RelOp = "="
        x = len(RelOp)
        temp = RelOp+".+\["
        col = re.search(temp,cond).group(0)[1:-1]
        case = 1
    
    elif re.search("\]>|\]<|\]=|\]!",cond):             #case2:  x[+3]>5 
        col = re.search(".+\[",cond).group(0)[:-1]
        ConOp = re.search("\[.+\]",cond).group(0)[1]
        if re.search(">=",cond):
            RelOp = ">="
        elif re.search("<=",cond):
            RelOp = "<="
        elif re.search("!=",cond):
            RelOp = "!=" 
        elif re.search("<",cond):
            RelOp = "<"
        elif re.search(">",cond):
            RelOp = ">"
        else:
            RelOp = "="        
        numMain = re.search(">.+|<.+|=.+",cond).group(0)[1:]
        temp = "\\"+ConOp + ".+\]"
        numInBracks = re.search(temp,cond).group(0)[1:-1]
        case = 2

    else:
        temp = re.search(".+>|.+<|.+=|.+!",cond).group(0)[:-1]
        if(True):   #tableVar[temp]
            case = 3
            col = temp
            if re.search(">=",cond):
                RelOp = ">="
            elif re.search("<=",cond):
                RelOp = "<="
            elif re.search("!=",cond):
                RelOp = "!=" 
            elif re.search("<",cond):
                RelOp = "<"
            elif re.search(">",cond):
                RelOp = ">"
            else:
                RelOp = "="  
            numMain = re.search(">.+|<.+|=.+",cond).group(0)[1:]          
        else:
            case = 4
            numMain = temp
            if re.search(">=",cond):
                RelOp = ">="
            elif re.search("<=",cond):
                RelOp = "<="
            elif re.search("!=",cond):
                RelOp = "!=" 
            elif re.search("<",cond):
                RelOp = "<"
            elif re.search(">",cond):
                RelOp = ">"
            else:
                RelOp = "="  
            col = re.search(">.+|<.+|=.+",cond).group(0)[1:]  
    

    #if id + 4 > 8, taking 4 to other side so + become a -
    if case == 1 or case == 2:
        if(ConOp =="+"):
            numMain = float(numMain)-float(numInBracks)
        elif(ConOp == "-"):
            numMain = float(numMain)+float(numInBracks)
        elif(ConOp =="\\"):
            numMain = float(numMain)*float(numInBracks)
        else:
            numMain = float(numMain)/float(numInBracks)
    
    numMain = float(numMain)
    column = tableVar[col]
    if RelOp == ">":
        column = column > numMain
    elif RelOp == ">=":
        column = column >= numMain
    elif RelOp == "<=":
        column = column <= numMain
    elif RelOp == "<":
        column = column < numMain
    elif RelOp == "!=":
        column = column != numMain
    else:
        column = column == numMain

    return column

def mask(boolArr,tableVar):
    colNames = tableVar.keys()
    
    tbl = {}

    for i in colNames:
        x = tableVar[i]
        tbl[i] = x[boolArr==True]

    return tbl   

def sort(structArray):


    #order=['age', 'height']
    #myData.dtype.names
    #use list(tuple) to make list.
    c =6
    
def joinHelp(tableVar1,tableVar2,tname1,tname2):

    newTable = {}
    
    for i in tableVar1:
        tempname = tname1+"_"+i
        temp = tableVar1[i]
        newTable[tempname] = temp[0] #for col type preserving

    for i in tableVar2:
        tempname = tname2+"_"+i
        temp = tableVar2[i]
        newTable[tempname] = temp[0] #for col type preserving

    # x = tableVar1[list(tableVar1.keys())[0]].size
    # y = tableVar2[list(tableVar2.keys())[0]].size
    # maxSize = x*y #maximum no of rows possible is cartesian product number in join

    # tTable = newTable

    # for x in newTable.keys():
    #     tTable[x] = np.tile(newTable[x],maxSize-1)
    # print(tTable[x])
    
    return newTable
    

def main():
    #Reading from stdin
    print("Welcome to database project. am10182 ddv246")
    f= open("commands.txt","r")
    for line in f:
        print("\n")
        print("-------------------------------------------------------------")
        print("The Command Entered is: \n")
        
        start = time.time()
        mid = 0
        print(line)

        if line[0] == "\n":
            print("Empty Command Entered \n") #do nothing, it's empty line, go back to stdin

        elif "outputtofile(" in line:
            line = line.replace(" ","")
            temp = re.search("\(.+\)",line).group(0)
            temp = temp[1:-1]
            n = re.split(",",temp)
            tablename = n[0]
            filename = n[1]
            writeToFile(tablename,filename)
            mid = time.time()
            
        elif "ash(" in line: #hashing implementation
            #do hash
            line = line.replace(" ","")
            temp = re.search("\(.+\)",line).group(0)
            temp = temp[1:-1]
            n = re.split(",",temp)
            tablename = n[0]
            colname = n[1]
            a = tables[tablename]
            column = a[colname]            
            hashtable ={}

            ct = 0

            for k in column:
                if k not in hashtable.keys():
                    lis = []
                    lis.append(ct)
                    hashtable[k] = lis

                else:
                    lis = hashtable[k]
                    lis.append(ct)
                    hashtable[k] = lis
                ct = ct+1

            hashDirectory = {}
            hashDirectory[colname] = hashtable
            hashtIndexes[tablename] = hashDirectory
            
            print("Status of Hash indexes are:")

            for p in hashtable:
                print(p," : ",hashtable[p])

 
        elif "tree(" in line:
            line = line.replace(" ","")
            temp = re.search("\(.+\)",line).group(0)
            temp = temp[1:-1]
            n = re.split(",",temp)
            tablename = n[0]
            colname = n[1]
            a = tables[tablename]
            column = a[colname] 
            btree = OOBTree() 


            ct = 0

            for k in column:
                if k not in btree.keys():
                    lis = []
                    lis.append(ct)
                    btree[k] = lis

                else:
                    lis = btree[k]
                    lis.append(ct)
                    btree[k] = lis
                ct = ct+1

            btreeDirectory = {}
            btreeDirectory[colname] = btree
            btreeIndexes[tablename] = btreeDirectory
 
            print("Status of Btree table for each of key indexes are:")

            for p in btree.keys():
                print(p," : ",btree[p])          #do btree
        
        else:
            func = line.replace(" ", "")
            func = func.replace("\n","")
            func = re.sub('/.+', '/', func) 
            if "/" not in func[0:1]: 
                #doing a regexer
                try:
                    tablename = re.search(".+:",func).group(0)[0:-1]
                except:
                    print("Wrong format in command. pls retry")
                    continue

                tablename = re.search(".+:",func).group(0)[0:-1]
                command = re.search(':=.+?\(',func).group(0)[2:-1] # :to distinguish from other equal tos
                params = re.search('\(.+\)',func).group(0)[1:-1]


                if(command == "inputfromfile"): #done
                    newTable = inputFromFile(params)
                    tables[tablename] = newTable
                    mid = time.time()
                    writeToFile(tablename,tablename)
                    
                if(command == "select"):#done
                    
                    if not (re.search(",",params)):
                        ans = tables[params]                   #means no condition like: R
                        tables[tablename] = ans
                        print(tables[tablename])
                        mid = time.time()
                        writeToFile(tablename,tablename)
                    
                    elif not re.search("\(",params):                  #means one condition like : R,id = 4
                        tblName = re.search(".+,",params).group(0)[:-1]
                        tblVar = tables[tblName]
                        condn = re.search(",.+",params).group(0)[1:]
                        boolean = select(tblVar,condn)
                        ansTbl = mask(boolean,tblVar)
                        tables[tablename] = ansTbl
                        mid = time.time()
                        writeToFile(tablename,tablename)


                    elif re.search("\(.+\)",params):          #means more than one condition
                        tblName = re.search(".+,",params).group(0)[:-1]
                        tblVar = tables[tblName]
                        
                        conditions = re.findall("\(.+?\)",params)
                        
                        booleans = []

                        for cond in conditions:             
                            cond = cond[1:-1]               #each of the conditions : time >30 after removing ()
                            boolean = select(tblVar,cond)
                            booleans.append(boolean)
                        
                        finalBoolArr = booleans[0]

                        if re.search("or",params):
                            for x in booleans:
                                finalBoolArr = np.logical_or(x,finalBoolArr)
                        else:
                            for x in booleans:
                                finalBoolArr = np.logical_and(x,finalBoolArr)
                            
                        ansTbl = mask(finalBoolArr,tblVar)
                        tables[tablename] = ansTbl
                        mid = time.time()
                        writeToFile(tablename,tablename)

                    else:
                        mid = time.time()
                        print("wrong format of select")

                if(command == "project"): #done
                    temp = re.split(',',params)
                    sourceTable = tables[temp[0]]
                    temp.pop(0)
                    ansTable = {}
                    for x in temp:
                        col = sourceTable[x]
                        ansTable[x] = col
                    
                    tables[tablename] = ansTable
                    mid = time.time()
                    writeToFile(tablename,tablename)

                if(command == "avg"): #done
                    temp = re.split(",",params)
                    sourceTable = tables[temp[0]]
                    col = sourceTable[temp[1]]
                    avg = np.mean(col)
                    avg = np.array([avg],np.float)
                    ansTable = {}
                    newColName = "avg("+temp[1]+")"
                    ansTable[newColName] = avg
                    tables[tablename] = ansTable
                    mid = time.time()
                    writeToFile(tablename,tablename)

                if(command == "sum"): #done
                    temp = re.split(",",params)
                    sourceTable = tables[temp[0]]
                    col = sourceTable[temp[1]]
                    avg = np.sum(col)
                    avg = np.array([avg],np.float)
                    ansTable = {}
                    newColName = "sum("+temp[1]+")"
                    ansTable[newColName] = avg
                    tables[tablename] = ansTable
                    mid = time.time()
                    writeToFile(tablename,tablename)  

                if(command == "count"): #done
                    temp = re.split(",",params)
                    sourceTable = tables[temp[0]]
                    col = sourceTable[temp[1]]
                    avg = col.size
                    avg = np.array([avg],np.float)
                    ansTable = {}
                    newColName = "count("+temp[1]+")"
                    ansTable[newColName] = avg
                    tables[tablename] = ansTable
                    mid = time.time()
                    writeToFile(tablename,tablename)                                    

                if(command == "sumgroup"): #done
                    temp = re.split(",",params)
                    sourceTable = tables[temp[0]]
                    sumColumn = sourceTable[temp[1]]
                    if len(temp) == 3:  
                        groupbyColumn = sourceTable[temp[2]]
                        name_un=np.unique(groupbyColumn)
                        newgbc = []
                        newsc = []
                        for nm in name_un:
                            arr=np.array([(True if x==nm else False) for x in groupbyColumn])
                            if arr.any():
                                newgbc.append(nm)
                                newsc.append(np.sum(sumColumn[arr]))
                                
                        newgbc = np.array(newgbc)
                        newsc = np.array(newsc)

                        sumColumn = "sum("+temp[1]+")"

                        newTable = {}
                        newTable[temp[2]] = newgbc
                        newTable[sumColumn] = newsc

                        tables[tablename] = newTable
                        mid = time.time()
                        writeToFile(tablename,tablename)
                    
                    else:  
                        groupbyColumn = sourceTable[temp[2]]
                        groupbyColumn2 = sourceTable[temp[3]]
                        name_un=np.unique(groupbyColumn)
                        name2_un=np.unique(groupbyColumn2)
                        newgbc = []
                        newgbc2 = []
                        newsc = []
                        for nm in name_un:
                            for nm2 in name2_un:
                                arr=np.array([(True if x==nm and y==nm2 else False) for x,y in zip(groupbyColumn,groupbyColumn2)])
                                if arr.any():
                                    newgbc.append(nm)
                                    newgbc2.append(nm2)
                                    newsc.append(np.sum(sumColumn[arr]))

                        newgbc = np.array(newgbc)
                        newgbc2 = np.array(newgbc2)
                        newsc = np.array(newsc)

                        sumColumn = "sum("+temp[1]+")"

                        newTable = {}
                        newTable[temp[2]] = newgbc
                        newTable[temp[3]] = newgbc2
                        newTable[sumColumn] = newsc

                        tables[tablename] = newTable
                        mid = time.time()
                        writeToFile(tablename,tablename)

                if(command == "avggroup"):#done
                    temp = re.split(",",params)
                    sourceTable = tables[temp[0]]
                    sumColumn = sourceTable[temp[1]]
                    if len(temp) == 3:  #oneparam
                        groupbyColumn = sourceTable[temp[2]]
                        name_un=np.unique(groupbyColumn)
                        newgbc = []
                        newsc = []
                        for nm in name_un:
                            arr=np.array([(True if x==nm else False) for x in groupbyColumn])
                            if arr.any():
                                newgbc.append(nm)
                                newsc.append(np.mean(sumColumn[arr]))
                                
                        newgbc = np.array(newgbc)
                        newsc = np.array(newsc)

                        sumColumn = "avg("+temp[1]+")"

                        newTable = {}
                        newTable[temp[2]] = newgbc
                        newTable[sumColumn] = newsc

                        tables[tablename] = newTable
                        mid = time.time()
                        writeToFile(tablename,tablename)
                    
                    else:  #means groupby 2 cols
                        groupbyColumn = sourceTable[temp[2]]
                        groupbyColumn2 = sourceTable[temp[3]]
                        name_un=np.unique(groupbyColumn)
                        name2_un=np.unique(groupbyColumn2)
                        newgbc = []
                        newgbc2 = []
                        newsc = []
                        for nm in name_un:
                            for nm2 in name2_un:
                                arr=np.array([(True if x==nm and y==nm2 else False) for x,y in zip(groupbyColumn,groupbyColumn2)])
                                if arr.any():
                                    newgbc.append(nm)
                                    newgbc2.append(nm2)
                                    newsc.append(np.mean(sumColumn[arr]))

                        newgbc = np.array(newgbc)
                        newgbc2 = np.array(newgbc2)
                        newsc = np.array(newsc)

                        sumColumn = "mean("+temp[1]+")"

                        newTable = {}
                        newTable[temp[2]] = newgbc
                        newTable[temp[3]] = newgbc2
                        newTable[sumColumn] = newsc

                        tables[tablename] = newTable
                        mid = time.time()
                        writeToFile(tablename,tablename)

                if(command == "countgroup"): #done
                    temp = re.split(",",params)
                    sourceTable = tables[temp[0]]
                    sumColumn = sourceTable[temp[1]]
                    if len(temp) == 3:  #oneparam
                        groupbyColumn = sourceTable[temp[2]]
                        name_un=np.unique(groupbyColumn)
                        newgbc = []
                        newsc = []
                        for nm in name_un:
                            arr=np.array([(True if x==nm else False) for x in groupbyColumn])
                            if arr.any():
                                newgbc.append(nm)
                                newsc.append(len(sumColumn[arr]))
                                
                        newgbc = np.array(newgbc)
                        newsc = np.array(newsc)

                        sumColumn = "count("+temp[1]+")"

                        newTable = {}
                        newTable[temp[2]] = newgbc
                        newTable[sumColumn] = newsc

                        tables[tablename] = newTable
                        mid = time.time()
                        writeToFile(tablename,tablename)
                    
                    else:  #means groupby 2 cols
                        groupbyColumn = sourceTable[temp[2]]
                        groupbyColumn2 = sourceTable[temp[3]]
                        name_un=np.unique(groupbyColumn)
                        name2_un=np.unique(groupbyColumn2)
                        newgbc = []
                        newgbc2 = []
                        newsc = []
                        for nm in name_un:
                            for nm2 in name2_un:
                                arr=np.array([(True if x==nm and y==nm2 else False) for x,y in zip(groupbyColumn,groupbyColumn2)])
                                if arr.any():
                                    newgbc.append(nm)
                                    newgbc2.append(nm2)
                                    newsc.append(len(sumColumn[arr]))

                        newgbc = np.array(newgbc)
                        newgbc2 = np.array(newgbc2)
                        newsc = np.array(newsc)

                        sumColumn = "count("+temp[1]+")"

                        newTable = {}
                        newTable[temp[2]] = newgbc
                        newTable[temp[3]] = newgbc2
                        newTable[sumColumn] = newsc

                        tables[tablename] = newTable
                        mid = time.time()
                        writeToFile(tablename,tablename)

                if(command == "sort"): #done

                    sortTableName = re.split(",",params)[0]
                    sortByColumns = re.split(",",params)[1:]
                    sortTableND = fileToStructND(sortTableName)
                    sortTableND = np.sort(sortTableND, order=sortByColumns) 
                    
                    table = {}

                    for i in sortTableND.dtype.names:
                        table[i] = sortTableND[i]

                    tables[tablename] = table
                    mid = time.time()
                    writeToFile(tablename,tablename)

                if(command == "movsum"): #done
                    temp = re.split(",",params)
                    movingTable = tables[temp[0]]
                    movingColumn = movingTable[temp[1]]
                    movingNo = int(temp[2])
                    x = np.cumsum(movingColumn)
                    y = np.zeros(x.size)

                    for i in range(0,movingNo):
                        y[i] = x[i]  
    
                    for i in range(0,x.size):
                        if i >= movingNo:
                            y[i]=x[i]-x[i-movingNo]

                    movingTable["mov_sum"] = y
                    tables[tablename] = movingTable
                    mid = time.time()
                    writeToFile(tablename,tablename)

                if(command == "movavg"): #done

                    temp = re.split(",",params)
                    movingTable = tables[temp[0]]
                    movingColumn = movingTable[temp[1]]
                    movingNo = int(temp[2])
                    x = np.cumsum(movingColumn)
                    y = np.zeros(x.size)

                    for i in range(0,movingNo):
                        y[i] = float(x[i])/float(i+1)  
    
                    for i in range(0,x.size):
                        if i >= movingNo:
                            y[i]=float(x[i]-x[i-movingNo])/float(movingNo)

                    movingTable["mov_avg"] = y
                    tables[tablename] = movingTable
                    mid = time.time()
                    writeToFile(tablename,tablename)

                if(command == "join"): #done

                    #first join the two tables.
                    temp = re.split(',',params)
                    table1 = tables[temp[0]]
                    table2 = tables[temp[1]]
                    conditions = temp[2]
                    newTable = joinHelp(table1,table2,temp[0],temp[1])  #skeleton of new table

                    #todo : get conditions
                    #(R1.qty[+3] > S.Q[-3]) and (R1.saleid = S.saleid)
                    #table.attribute [arithop constant] relop table.attribute [artithop con- stant]

                    t1columns = []
                    t2columns = []
                    relOperators = []
                    leftNums = []
                    rightNums = []
                    arithOpsL = []
                    arithOpsR = []
                    
                    def helper(cond):
                        left = ""
                        right = ""
                        if re.search("!=",cond):
                            left = re.search(".+!=",cond).group(0)
                            left = left[:-1]
                            right = re.search("!=.+",cond).group(0)
                            right = right[1:] 
                            relOperators.append("!=")                        
                        elif re.search(">=",cond):
                            relOperators.append(">=")
                            left = re.search(".+>=",cond).group(0)
                            left = left[:-1]
                            right = re.search(">=.+",cond).group(0)
                            right = right[1:]                            
                        elif re.search("<=",cond):
                            relOperators.append("<=")
                            left = re.search(".+<=",cond).group(0)
                            left = left[:-1]
                            right = re.search("<=.+",cond).group(0)
                            right = right[1:]  
                        elif re.search("=",cond):
                            relOperators.append("=")
                            left = re.search(".+=",cond).group(0)
                            left = left[:-1]
                            right = re.search("=.+",cond).group(0)
                            right = right[1:] 
                        elif re.search("<",cond):
                            relOperators.append("<")
                            left = re.search(".+<",cond).group(0)
                            left = left[:-1]
                            right = re.search("<.+",cond).group(0)
                            right = right[1:]
                        else:
                            relOperators.append(">")
                            left = re.search(".+>",cond).group(0)
                            left = left[:-1]
                            right = re.search(">.+",cond).group(0)
                            right = right[1:]

                        #R1.qty[+3] > S.Q[-3] and R1.saleid = S.saleid
                        # t1columns.append(re.search(".+\.",left).group(0)[:-1])
                        # t2columns.append(re.search(".+\.",right).group(0)[:-1])

                        if re.search("\[",left):
                            t1columns.append(re.search("\..+\[",left).group(0)[1:-1])
                            arithOpsL.append(re.search("\[.+\]",left).group(0)[1])
                            leftNums.append(re.search("\[.+\]",left).group(0)[2:-1])
                        else:
                            t1columns.append(re.search("\..+",left).group(0)[1:])
                            arithOpsL.append("+")
                            leftNums.append("0")

                        if re.search("\[",right):
                            t2columns.append(re.search("\..+\[",right).group(0)[1:-1])
                            arithOpsR.append(re.search("\[.+\]",right).group(0)[1])
                            rightNums.append(re.search("\[.+\]",right).group(0)[2:-1])
                        else:
                            t2columns.append(re.search("\..+",right).group(0)[1:])
                            arithOpsR.append("+")
                            rightNums.append("0")


                    if "and" in conditions:
                        conditions = re.split("and",temp[2])

                        for con in conditions:
                            con = con[1:-1] #removing bracks
                            helper(con)

                    else:
                        helper(conditions)                             


                    temp1 = table1[list(table1.keys())[0]]
                    temp2 = table2[list(table2.keys())[0]]

                    x = temp1.size
                    y = temp2.size

                    ops = { "+": operator.add, "-": operator.sub, "*":operator.truediv, "\\":operator.mul } # etc.

                    

                    for i in range(0,x):
                        #get record of t1
                        rowt1 = {}
                        for k in table1.keys():
                            val = table1[k]
                            val = val[i]
                            rowt1[k] = val
                    
                        
                        # print("entering t2")
                        for j in range(0,y):
                            rowt2 = {}
                            for l in table2.keys():
                                val = table2[l]
                                val = val[j]
                                rowt2[l] = val

                            #need to check if conditions hold good for row 1 and row2
                            
                            boolVal = True
                            count = 0
                            
                            for op in relOperators:
                                
                                templ1 = ops[arithOpsL[count]](float(rowt1[t1columns[count]]),float(leftNums[count]))
                                tempr1 = ops[arithOpsR[count]](float(rowt2[t2columns[count]]),float(rightNums[count]))


                                if(op==">"):
                                    boolVal = boolVal and templ1 > tempr1
                                if(op=="<"):
                                    boolVal = boolVal and templ1 < tempr1
                                if(op=="="):
                                    boolVal = boolVal and templ1 == tempr1
                                if(op==">="):
                                    boolVal = boolVal and templ1 >= tempr1
                                if(op=="<="):
                                    boolVal = boolVal and templ1 <= tempr1
                                if(op=="!="):
                                    boolVal = boolVal and templ1 != tempr1
                                

                                count = count+1

                            if(boolVal == True):
                                listnames = list(newTable.keys())
                                listnamesrt1 = list(rowt1.keys())
                                listnamesrt2 = list(rowt2.keys())
                                ct = 0
                                r1ct = 0
                                r2ct = 0
                                # print(listnames)
                                # print(listnamesrt1)
                                # print(listnamesrt2)
                                for p in rowt1.keys():
                                    newTable[listnames[ct]]=np.append(newTable[listnames[ct]],rowt1[listnamesrt1[r1ct]])
                                    ct = ct+1
                                    r1ct = r1ct+1
                                
                                for q in rowt2.keys():
                                    newTable[listnames[ct]]=np.append(newTable[listnames[ct]],rowt2[listnamesrt2[r2ct]])
                                    ct = ct+1
                                    r2ct = r2ct+1
                    
                    newTable = removeColumn(newTable) #removing redundant record one used to preserve datatype
                    tables[tablename] = newTable
                    mid = time.time()
                    writeToFile(tablename,tablename)
                    #print(t1columns,t2columns,relOperators,leftNums,rightNums,arithOpsL,arithOpsR)

                if(command == "concat"): #Q5 := concat(Q4, Q2) 
                    temp = re.split(",",params)
                    concatTable1 = tables[temp[0]]
                    concatTable2 = tables[temp[0]]

                    x = list(concatTable1.keys())
                    y = list(concatTable2.keys())
                    #checking if schema matches

                    if(x==y):
                        #print("schema matches")
                        for key in x:
                            concatTable1[key] = np.append(concatTable1[key],concatTable2[key])
                        tables[tablename] = concatTable1
                        mid = time.time()
                        writeToFile(tablename,tablename)
                    else:
                        print("schema of given tables dont match")


            else:
                print("")  #its a comment
        
        end = time.time()
        if(mid!=0):
            print("---------------------------")
            print("Query output written to file")
            print("Query Execution time is "+str(mid-start))
            print("Time taken to write to file is"+str(end-mid))
            print("---------------------------\n")






                

main()




