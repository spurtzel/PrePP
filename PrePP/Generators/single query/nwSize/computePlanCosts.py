#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 18 16:32:53 2021

@author: samira
"""
from placement import *
import time
import csv
import sys


maxDist = max([max(x) for x in allPairs])


def getLowerBound(query): # lower bound
    MS = []
    for e in query.leafs():
        myprojs= [p for p in list(set(projlist).difference(set([query]))) if totalRate(p)<rates[e] and not e in p.leafs()] #TODO define over paths
        if myprojs:
            MS.append(e)
        for p in [x for x in projlist if e in x.leafs()]:
            part = returnPartitioning(p,p.leafs())            
            if e in part:
                MS.append(e)
    nonMS = [e for e in query.leafs() if not e in MS]  
    if nonMS:          
        minimalRate = sum(sorted([totalRate(e) for e in query.leafs() if not e in MS])) * longestPath
    else:
        minimalRate = min([totalRate(e) for e in query.leafs()]) * longestPath
    minimalProjs = sorted([totalRate(p) for p in projlist if not p==query])[:len(list(set(MS)))-1]
    minimalRate +=  sum(minimalProjs) * longestPath
              
    return minimalRate  


def normalize(x, data):
    return x
    #return (x - min(data)) / (max(data) - min(data))

def main():
    
    Filters = []
    shared = 0
    iteration = 0
    filename = "None"
    if len(sys.argv) > 1:
       filename = str(sys.argv[1])
  
    ccosts = NEWcomputeCentralCosts(wl) #TODO
    print("central costs : " + str(ccosts))
    centralHopLatency = max(allPairs[ccosts[1]])
    print("central Hop Latency: " + str(centralHopLatency))
    MSPlacements = {}
    curcosts = 1 
    start_time = time.time()
    
    hopLatency = {}
    
       
    EventNodes = initEventNodes()[0]
    IndexEventNodes = initEventNodes()[1]    
    
    myPlan = EvaluationPlan([], [])
    
    myPlan.initInstances(IndexEventNodes) # init with instances for primitive event types
    
    
    unfolded = mycombi
    sharedDict = getSharedMSinput(unfolded)    
    dependencies = compute_dependencies(unfolded)
    processingOrder = sorted(compute_dependencies(unfolded).keys(), key = lambda x : dependencies[x] ) # unfolded enthält kombi   
        
    costs = 0
    for projection in processingOrder:  #parallelize computation for all projections at the same level 
            
            if set(unfolded[projection]) == set(projection.leafs()): #initialize hop latency with maximum of children
               hopLatency[projection] = 0 
            else:
                hopLatency[projection] = max([hopLatency[x] for x in unfolded[projection] if x in hopLatency.keys()])

          
            partType = returnPartitioning(projection, unfolded[projection], criticalMSTypes)
            if partType : 
                MSPlacements[projection] = partType
                result = computeMSplacementCosts(projection, unfolded[projection], partType, sharedDict)
                additional = result[0]
                costs += additional
                hopLatency[projection] += result[1]
                
                myPlan.addProjection(result[2]) #!
                
                for newin in result[2].spawnedInstances: # add new spawned instances
                    myPlan.addInstances(projection, newin) 
                    
                myPlan.updateInstances(result[3]) #! update instances
                
                Filters += result[4]

                print("MS " + str(projection) + " At: " + str(partType) + " PC: " + str(additional) + " Hops:" + str(result[1]))
            else:
                
                result = ComputeSingleSinkPlacement(projection, unfolded[projection])
                additional = result[0]
                costs += additional
                hopLatency[projection] += result[2]
                
                myPlan.addProjection(result[3]) #!
                for newin in result[3].spawnedInstances: # add new spawned instances
                    myPlan.addInstances(projection, newin)
                
                myPlan.updateInstances(result[4]) #! update instances
                Filters += result[5]
               
                print("SiS " + str(projection) + "PC: " + str(additional)  + " Hops: " + str(result[2]))
                
    mycosts = costs/ccosts[0]
    print("Central " + str(ccosts[4]))
    
    print("Muse Transmission " + str(costs) )
    lowerBound = 0
    for query in wl:
        lowerBound += getLowerBound(query)
    print("Lower Bound: " + str(lowerBound))
    print("Transmission Ratio: " + str(mycosts))
    print("MuSE Depth: " + str(float(max(list(dependencies.values()))+1)/2))
    
    hoplatency = max([hopLatency[x] for x in wl if x in hopLatency.keys()]) # in case layers remove queries
    
    print("Hop Latency: " + str(hoplatency))
    
    
    
  #  print("Levels : " + str((float(max(list(dependencies.values())))/2) * maxDist))
    totaltime = str(round(time.time() - start_time, 2))

    print(totaltime)  
    
    
#    with open("EvalPlan", "a") as evalPlan:
#        writer = csv.writer(evalPlan)        
#        for i in myPlan.instances:
#            writer.writerow([i])
            
    #getNetworkParameters, selectivityParameters, combigenParameters
        
    with open('networkExperimentData', 'rb') as networkExperimentData_file: 
          networkParams = pickle.load(networkExperimentData_file)   
    with open('selectivitiesExperimentData', 'rb') as selectivities_file: 
          selectivityParams  = pickle.load(selectivities_file)   
    with open('combiExperimentData', 'rb') as combiExperimentData_file: 
          combigenParams = pickle.load(combiExperimentData_file) 
    with open('processingLatency', 'rb') as processingLatency_file: 
          processingLatencyParams = pickle.load(processingLatency_file)            
                      
    ID = int(np.random.uniform(0,10000000))
    
    data = [hoplatency, processingLatencyParams[1], processingLatencyParams[2], centralHopLatency]
    totalLatencyRatio = (normalize(hoplatency, data) + normalize(processingLatencyParams[1], data))/ (normalize(centralHopLatency, data) +  normalize(processingLatencyParams[2], data))
    
    museLatency = processingLatencyParams[1]  + hoplatency * hopfactor
    centralLatency = processingLatencyParams[2] + centralHopLatency * hopfactor - hopfactor * longestPath * 1.5
    totalLatencyRatio = museLatency/centralLatency
    
    print("total LatencyRatio: " + str(totalLatencyRatio))
    
       
    myResult = [ID, mycosts,  Filters, networkParams[3], networkParams [0], networkParams[2], len(wl), combigenParams[3], selectivityParams[0], selectivityParams[1], combigenParams[1], longestPath, totaltime,hoplatency, float(max(list(dependencies.values()))/2), processingLatencyParams[0], ccosts[0], costs, ccosts[0] == ccosts[4]]
    schema = ["ID", "TransmissionRatio", "FilterUsed", "Nodes", "EventSkew", "EventNodeRatio", "WorkloadSize", "NumberProjections", "MinimalSelectivity", "MedianSelectivity","CombigenComputationTime", "Efficiency", "PlacementComputationTime", "HopCount", "Depth", "ProcessingLatencyRatio", "CentralTransmission", "MuseTransmission"] 
    
    new = False
    try:
        f = open("res/"+str(filename)+".csv")   
    except FileNotFoundError:
        new = True           
        
    with open("res/"+str(filename)+".csv", "a") as result:
      writer = csv.writer(result)  
      if new:
          writer.writerow(schema)              
      writer.writerow(myResult)
      
    with open('EvaluationPlan',  'wb') as EvaluationPlan_file:
        pickle.dump([myPlan, ID, MSPlacements], EvaluationPlan_file)
    
    with open('CentralEvaluationPlan',  'wb') as CentralEvaluationPlan_file:
        pickle.dump([ccosts[1],ccosts[3], wl], CentralEvaluationPlan_file)
    

if __name__ == "__main__":
    main()                    