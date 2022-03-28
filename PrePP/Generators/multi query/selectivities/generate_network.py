import sys
import pickle
import numpy as np



""" Experiment network rates 
event rates experiment set 1
ev = [[3, 12, 1054, 25, 1, 36, 46, 4533, 8, 4271, 1, 4, 7, 2, 10441, 4, 2, 1, 9, 9]]
"""    


ev = [[3, 12, 1054, 25, 1, 36, 46, 4533, 8, 4271, 1, 4, 7, 2, 10441, 4, 2, 1, 9, 9]]


def generate_eventrates(eventskew,numb_eventtypes):
    eventrates = np.random.zipf(eventskew,numb_eventtypes)
    while max(eventrates) > 100000:
        eventrates = np.random.zipf(eventskew,numb_eventtypes)
    return eventrates

def generate_events(eventrates, n_e_r):
    myevents = []
    for i in range(len(eventrates)):
        x = np.random.uniform(0,1)
        if x < n_e_r:
            myevents.append(eventrates[i])
        else:
            myevents.append(0)
    
    return myevents

def regain_eventrates(nw):
    eventrates = [0 for i in range(len(nw[0]))]
    interdict = {}
    for i in nw:
        for j in range(len(i)):
            if i[j] > 0 and not j in interdict.keys():
                interdict[j] = i[j]
    for j in sorted(interdict.keys()):
        eventrates[j] = interdict[j]
    return eventrates 

def allEvents(nw):
    for i in range(len(nw[0])) :
        column = [row[i] for row in nw]
        if sum(column) == 0:
            return False
    return True

def main():

    
     
    #default values for simulation 
    nwsize = 20
    node_event_ratio = 0.5
    num_eventtypes = 20
    eventskew = 1.3
    
    #generate network of size nwsize, with node_event_ratio for given event rates (ev)

    eventrates = generate_eventrates(eventskew,num_eventtypes)
    
    if len(sys.argv) > 1:
        nwsize =int(sys.argv[1])
    if len(sys.argv) > 2:
        node_event_ratio = float(sys.argv[2])
    if len(sys.argv) > 3:
        eventskew = float(sys.argv[3])
    if len(sys.argv) > 4:
        num_eventtypes = int(sys.argv[4])
        
    if len(sys.argv) > 3:    
        #eventrates = generate_eventrates(eventskew,num_eventtypes)
        pass
    else:
        eventrates = regain_eventrates(ev)
        
    #eventrates = generate_eventrates(eventskew,num_eventtypes)       
    nw= []
    
   
    # for i in [1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7]:
    #      myOnes = []
    #      myExps = []
    #      for k in range(1000):
    #          eventrates = generate_eventrates(i,num_eventtypes)  
    #          ones = [x for x in eventrates if x < 10]
    #          myE = [x for x in eventrates if x > np.median(eventrates)]
    #          myOnes.append(len(ones))
    #          myExps.append(len(myE))
    #      print(i, np.average(myOnes), np.average(myExps))
    
    for node in range(nwsize):
        nw.append(generate_events(eventrates, node_event_ratio))
    
    while not allEvents(nw):
        nw = []
        for node in range(nwsize):
            nw.append(generate_events(eventrates, node_event_ratio))
    
    #nw = [[6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604], [6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604],[6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604], [6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604], [0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604],[6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604], [6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604],[6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604], [6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604], [0, 0, 0, 0,3604],[0, 0, 0, 12,0 ],[0, 0, 0, 0,3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12, 3604],[0, 0, 0, 12,0 ],[6, 0, 20, 0,3604],[0, 0, 20, 12, 3604],[0, 0, 0, 12, 3604]]           
    #nw = [[6, 0, 20, 0, 0],[0, 50, 0, 0, 0],[0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604], [0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604],  [0, 50, 20, 0, 3604],[6, 0, 20, 12, 3604],[6, 0, 0, 0,3604]]
    
    #export eventskew, node_eventratio, networksize, maximal difference in rates
    networkExperimentData = [eventskew, num_eventtypes, node_event_ratio, nwsize, min(eventrates)/max(eventrates)]
    with open('networkExperimentData', 'wb') as networkExperimentDataFile:
        pickle.dump(networkExperimentData, networkExperimentDataFile)
    
    with open('network', 'wb') as network_file:
          pickle.dump(nw, network_file)       
    
   
    print("NETWORK")  
    print("--------") 
    for i in range(len(nw)):
      print("Node " + str(i) + " " + str(nw[i])) 
    print("\n")
    

        
if __name__ == "__main__":
    main()


        



