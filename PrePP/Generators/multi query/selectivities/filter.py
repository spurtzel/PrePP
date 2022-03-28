#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 20 13:03:07 2021

@author: samira
"""
from structures import *

with open('singleSelectivities',  'rb') as  singleSelectivities_file:
    singleSelectivities = pickle.load(singleSelectivities_file)
    
with open('projrates',  'rb') as projratesfile:
    projrates = pickle.load(projratesfile)
    
with open('singleSelectivities',  'rb') as singleSelectivities_file:
    singleSelectivities = pickle.load(singleSelectivities_file)

def computePromisingType(projection):
    promisingEvent = "X"
    currentSave = 0
    for primEvent in projection.leafs():
        decomposedSum = getDecomposedTotal(primEvent, projection)
        rateSaved = (projrates[projection][1] * numETBs("", projection)) - ((longestPath * rates[primEvent] * len(IndexEventNodes[primEvent])) +  decomposedSum)
        if rateSaved > currentSave and rateSaved > 0: #saved rate must include costs for routing promising event to dest, prob not necessary 
            currentSave = rateSaved
            promisingEvent = primEvent
    if promisingEvent != "X":
        return(promisingEvent, getDecomposed(promisingEvent, projection))
    else:
        return(promisingEvent, getDecomposed(promisingEvent, projection))
        
        
def numETBs(primEvents, projection):
    count = 1
    for event in projection.leafs():
        if not event in primEvents:
            count *= len(IndexEventNodes[event])
    return count

def getDecomposed(primEvent, projection): #TODO: very primitive variant which only sends primitive events filtered by single selectivity, 
    mysum = 0
    for event in  projection.leafs():
        if not event == primEvent:
            myKey = getKeySingleSelect(event, projection)
            mysum += singleSelectivities[myKey] * rates[event]
    return mysum        

def getDecomposedTotal(primEvent, projection): #TODO: very primitive variant which only sends primitive events filtered by single selectivity, 
    mysum = 0
    for event in  projection.leafs():
        if not event == primEvent:
            myKey = getKeySingleSelect(event, projection)
            mysum += singleSelectivities[myKey] * rates[event] * len(IndexEventNodes[event])
    return mysum 
            
def getKeySingleSelect(primEvent, projection):
    myString = primEvent + "|" + "".join(sorted(projection.leafs()))
    return myString

def additionalFilters(projection, promisingEvent):
    additionalFiltersList = []
    for event in projection.leafs():
        if not event  == promisingEvent:
            myKey = getKeySingleSelect(event, projection)
            #commonETBS = len(IndexEventNodes[promisingEvent]) * numETBs([event, promisingEvent],projection)
            commonETBS = numETBs([event, promisingEvent],projection)
            additionalSavings = commonETBS *  singleSelectivities[myKey] * rates[event]
            additionalSavings -= rates[event] * longestPath        
            if additionalSavings > 0 :
                additionalSavings += rates[event] * longestPath # do not substract by upper bound of sending filters, as actual costs can be computed during placement
                additionalSavings *= len(IndexEventNodes[event])
                additionalFiltersList.append((event, additionalSavings))
    return additionalFiltersList 

ProjFilterDict = {}

def returnProjFilterDict(projection):
            ''' return for a projection with different subsets of primitive events that can be used as filters and the resulting rate of the projection '''
            ProjFilterDict = {}
            #totalRate = numETBs("", projection) * projrates[projection][1]
            totalRate = projrates[projection][1] #singleRate
            ProjFilterDict[projection] = {}
            ProjFilterDict[projection][""] = (totalRate, 0)
            promising = computePromisingType(projection)
            
            if promising[0] != "X" :
               currentKey = promising[0]
               currentRate = promising[1] 
               ProjFilterDict[projection][currentKey] = (currentRate, 0) 
               # additionalFiltersList = additionalFilters(projection, promising[0])
               # for myfilter in additionalFiltersList:
               #     # here actually get all combinations
               #     currentKey += myfilter[0]
               #     currentRate -= (myfilter[1]/numETBs("", projection)) #CHECK: rates after filters should be computed on ETB Base to fit with ETB based placement method
               #     ProjFilterDict[projection][currentKey] = (currentRate, 0)
            return ProjFilterDict    
               
def getMaximalFilter(filterdict, proj):
    return sorted(filterdict[proj].keys(), key = len)[0]   
    #return sorted(filterdict[proj].keys(), key = len, reverse = True)[0]          # USED TO SUPRESS FILTER!!!


def getPMs(projection, myfilter):
    totalETBs = numETBs("", projection)
    return 0

# for projection in combis.keys():
#     projFilterDict.update(returnProjFilterDict(projection))
# print(projFilterDict)
#


# compute all filters, additional partial matches and 

# compute additional pms for configuration of filters
# compute savings of an additional eventtype given a configuation 