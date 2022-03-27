using System.Diagnostics;
using System.Collections.Immutable;
using System.Collections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection.Metadata;
using System.Runtime.Serialization;
using DCEP.Core;
using DCEP.Core.QueryProcessing;
using DCEP.Core.Utils;
using DCEP.Core.QueryProcessing.Constraints;
using DCEP.Core.Utils.LeastCommonAncestor;
using DCEP.Core.QueryProcessing.Operators;
using DCEP.Core.DCEPControlMessage;

namespace DCEP.Core
{
    [DataContract]
    public class QueryProcessorUniqueComponents : QueryProcessor
    {
        [DataMember]
        List<State> startStates;

        [DataMember]
        Dictionary<EventType, List<Activation>> activationsByInputEventType;

        [DataMember]
        ImmutableList<EventType> queryComponentNames;

        [DataMember]
        private LeastCommonAncestorFinder<QueryComponent> leastCommonAncestorFinder;

        [DataMember]
        private readonly List<PrimitiveBufferComponentAllMatchConstraint> pBCAllMatchConstraints;

        [DataMember]
        private readonly ExecutionPlan executionPlan;

        [DataMember]
        private readonly NodeName nodeName;

        [DataMember]
        private Dictionary<NodeName, List<AbstractEvent>> sourceBuffers;

        [DataMember]
        private HashSet<EventType> initiatorEvents;

        [DataMember]
        private Dictionary<NodeName, List<EventType>> eventTypesProducedByNode;

        [DataMember]
        private List<Tuple<AbstractEvent, DateTime>> locallyProducedEventTypeBuffer = new List<Tuple<AbstractEvent, DateTime>>();
        
        private Random randomNumberGenerator = new Random(42);
        
        [DataMember] private Dictionary<EventType, List<AbstractEvent>> pullEventBuffers = new Dictionary<EventType, List<AbstractEvent>>();
        
        [DataMember]
        private TimeSpan maxActiveEventBufferTime = new TimeSpan();
        
        [DataMember] 
        private HashSet<String> pullRequestAlreadySent = new HashSet<String>(); //Steven
        
        [DataMember]
        private Dictionary<EventType, int> pullRequestTypeCounter = new Dictionary<EventType, int>();
        
        
        [DataMember]
        private HashSet<String> activationAlreadyProduced = new HashSet<String>(); //Steven
        
        public QueryProcessorUniqueComponents(Query query, TimeSpan timeWindow, ExecutionPlan executionPlan_, NodeName nodeName_) : base(query, timeWindow)
        {
            startStates = new List<State>();
            activationsByInputEventType = new Dictionary<EventType, List<Activation>>();
            executionPlan = executionPlan_;
            nodeName = nodeName_;
            leastCommonAncestorFinder = new LeastCommonAncestorFinder<QueryComponent>(query.rootOperator);

            eventTypesProducedByNode = new Dictionary<NodeName, List<EventType>>();


            sourceBuffers = new Dictionary<NodeName, List<AbstractEvent>>();
            initiatorEvents = new HashSet<EventType>();
            this.pBCAllMatchConstraints = new List<PrimitiveBufferComponentAllMatchConstraint>(){
                    new WithinTimeWindowConstraint(timeWindow)
                };

            
            initialize();
            maxActiveEventBufferTime = timeWindow;
            Console.WriteLine("maxActiveEventBufferTime:" + maxActiveEventBufferTime);
        }


        private void initialize()
        {
            queryComponentNames = query.rootOperator.getComponentsAsEventTypes().ToImmutableList();

            startStates = createAutomata(new HashSet<EventType>(query.inputEvents), Enumerable.Empty<EventType>());

            //STNM == Skip Till Next Match (Policy)
            if(query.eventSelectionStrategy.Equals("STNM"))
            {
                initializeSourceBuffers();
            }

            foreach (var startState in startStates)
            {
                initiatorEvents.Add(startState.requiredEventType);
                activationsByInputEventType[startState.requiredEventType] = new List<Activation>() { new Activation(startState) };
            }
        }

        //only relevant for skip-till next match global watermark
        //determine all sources for this node to generate one buffer per source
        //initialize hashmap from nodename to produced event types for optimization step
        private void initializeSourceBuffers()
        {
            sourceBuffers[this.nodeName] = new List<AbstractEvent>();
            foreach (var forwardRules in executionPlan.forwardRulesByNodeName)
            {
                foreach (var forwardRule in forwardRules.Value)
                {
                    foreach (var nodeName in forwardRule.Value.destinations)
                    {
                        if (this.nodeName.Equals(nodeName))
                        {
                            sourceBuffers[forwardRules.Key] = new List<AbstractEvent>();

                            if (!eventTypesProducedByNode.ContainsKey(forwardRules.Key))
                                eventTypesProducedByNode[forwardRules.Key] = new List<EventType>();
                            eventTypesProducedByNode[forwardRules.Key].Add(forwardRule.Key);
                        }
                    }
                }
            }
        }


        private List<State> createAutomata(HashSet<EventType> remainingInputs, IEnumerable<EventType> preceedingEventTypes)
        {
            if (remainingInputs.Count == 0)
            {
                return null;
            }
            else
            {
                var result = new List<State>();

                foreach (var inputEvent in remainingInputs)
                {
                    // create state with constraints
                    var pBCAnyMatchConstraints = createPBCAnyMatchConstraints(inputEvent, preceedingEventTypes);

                    var bufferConstraints = new List<BufferConstraint>();
                    State s = new State(inputEvent, bufferConstraints, pBCAnyMatchConstraints, this.pBCAllMatchConstraints);

                    // create proceeding states
                    var newRemaining = new HashSet<EventType>(remainingInputs);
                    newRemaining.Remove(inputEvent);

                    var updatedPreceedingEventTypesList = preceedingEventTypes.ToList();
                    updatedPreceedingEventTypesList.Add(inputEvent);

                    s.nextStates = createAutomata(newRemaining, updatedPreceedingEventTypesList);

                    result.Add(s);
                }

                return result;
            }
        }

        internal List<PrimitiveBufferComponentAnyMatchConstraint> createPBCAnyMatchConstraints(EventType inputEvent, IEnumerable<EventType> preceedingEventTypes)
        {
            var output = new List<PrimitiveBufferComponentAnyMatchConstraint>();
            var equalIDGuaranteed = new HashSet<EventType>();

            foreach (var eventInBuffer in preceedingEventTypes)
            {
                // possibly deconstruct complex event types to primitive types for checking their respective constraints
                foreach (var primitiveInputEvent in inputEvent.parseToQueryComponent().getListOfPrimitiveEventTypes())
                {
                    foreach (var primitveEventInBuffer in eventInBuffer.parseToQueryComponent().getListOfPrimitiveEventTypes())
                    {
                        if (primitiveInputEvent.Equals(primitveEventInBuffer))
                        {
                            if (!equalIDGuaranteed.Contains(primitiveInputEvent))
                            {
                                output.Add(new EqualIDWhenEqualEventTypeConstraint(primitiveInputEvent));
                                equalIDGuaranteed.Add(primitiveInputEvent);
                            }

                            continue;
                        }

                        var op = (AbstractQueryOperator)leastCommonAncestorFinder.FindCommonParent(primitiveInputEvent, primitveEventInBuffer);

                        if (op is SEQOperator)
                        {
                            // derive predecessor or successor constraint
                            var first = op.getFirstOccuringChildOf(primitiveInputEvent, primitveEventInBuffer);

                            if (primitiveInputEvent.Equals(first))
                            {
                                output.Add(new SequenceConstraint(primitiveInputEvent, primitveEventInBuffer, SequenceType.IsPredecessor));
                            }
                            else if (primitveEventInBuffer.Equals(first))
                            {
                                output.Add(new SequenceConstraint(primitiveInputEvent, primitveEventInBuffer, SequenceType.IsSuccessor));
                            }
                        }

                    }
                }
            }

            return output;
        }

        public override (IEnumerable<ComplexEvent>, IEnumerable<PullRequestMessage>) processInputEvent(AbstractEvent e, DateTime t)
        {
            if (query.eventSelectionStrategy.Equals("STNM"))
                return skipTillNextMatch(e, t);
            else
                return skipTillAnyMatch(e, t);
        }


        private (IEnumerable<ComplexEvent>, IEnumerable<PullRequestMessage>) skipTillNextMatch(AbstractEvent e, DateTime t)
        {
            List<ComplexEvent> outputEvents = new List<ComplexEvent>();
            List<PullRequestMessage> pullRequests = new List<PullRequestMessage>();
            
            bool singleSource = sourceBuffers.Count==1?true:false;
            
            if(!singleSource)
            {
                if (!sourceBuffers.ContainsKey(e.nodeName))
                {
                    sourceBuffers[e.nodeName] = new List<AbstractEvent>();
                }

                sourceBuffers[e.nodeName].Add(e);
            }
            
            
            bool bufferOptimizationWasRun = false;

            //check for the current best local watermark (output guarantee)
            while (true)
            {
                if(!singleSource)
                {
                    bool anyBufferEmpty = false;
                    List<AbstractEvent> minimumBuffer = null;
                    List<EventType> forbiddenEventTypes = new List<EventType>();
                    
                    int count = 0;
                    int numberOfBufferedEvents = 0;
                    foreach (var sourceBuffer in sourceBuffers)
                    {
                        numberOfBufferedEvents += sourceBuffer.Value.Count;

                        if (sourceBuffer.Value.Count == 0 && !sourceBuffer.Key.Equals(this.nodeName))
                        {
                            anyBufferEmpty = true;
                            // collect from every empty source buffer the correlating event types
                            forbiddenEventTypes.AddRange(eventTypesProducedByNode[sourceBuffer.Key]);
                        }
                        else
                        {
                            if (sourceBuffer.Value.Count != 0 && (minimumBuffer == null || sourceBuffer.Value[0].timeCreated < minimumBuffer[0].timeCreated))
                            {
                                minimumBuffer = sourceBuffer.Value;
                            }
                        }
                    }


                    if(anyBufferEmpty)
                    {
                        // only if current oldest event type is not forbidden and only run optimization once
                        if (minimumBuffer == null || forbiddenEventTypes.Contains(minimumBuffer[0].type) || bufferOptimizationWasRun)
                            break;

                        bufferOptimizationWasRun = true;
                    }


                    e = minimumBuffer[0];

                    // only remove elements, if optimization is not active
                    if(!bufferOptimizationWasRun)
                        minimumBuffer.RemoveAt(0);
                }


                if(activationsByInputEventType.ContainsKey(e.type))
                {
                    // reverse loop through activations to remove the ones that made a transition in the loop
                    for (int activationIndex = activationsByInputEventType[e.type].Count - 1; activationIndex >= 0; activationIndex--)
                    {
                        // if buffer optimization is activated, then no instance shall be created
                        if (bufferOptimizationWasRun && activationIndex < startStates.Count)
                            continue;

                        var (newactivations, outputeventcomponents, invalid) = activationsByInputEventType[e.type][activationIndex].consumeEvent(e,t,timeWindow);


                        if (newactivations != null)
                        {
                            foreach (var newactivation in newactivations)
                            {
                                for(int state = newactivation.currentPushPullState; state < query.pushPullPlan.Count; ++state)
                                {
                                    if(query.pushPullPlan[state].toAcquire.Contains(newactivation.currentState.requiredEventType))
                                    {
                                        foreach(var eventToAcquire in query.pushPullPlan[state].toAcquire)
                                        {
                                            //PullRequestMessage(NodeName sendingNode, EventType pullEvent, List<AbstractEvent> eventsToPullWith, List<NodeName> destinations) : base(sendingNode)
                                            var eventsToPullWith = determineEventsToPullWith(query.pushPullPlan[state].dependencies, newactivation);
            
                                            var destinations = new List<NodeName>(query.placement.selectedNodes);
                                            if(query.placement.singleNode != null)
                                                destinations.Add(query.placement.singleNode);
                                            
                                            pullRequests.Add(new PullRequestMessage(nodeName, eventToAcquire, eventsToPullWith, destinations, query));
                                        }
                                        newactivation.currentPushPullState = state + 1;
                                        break;
                                    }
                                }
                                activationsByInputEventType[newactivation.currentState.requiredEventType].Add(newactivation);
                            }
                        }

                        if (outputeventcomponents != null)
                        {
                            outputEvents.Add(new ComplexEvent(query.name, outputeventcomponents, nodeName));
                        }

                        if((newactivations != null || outputeventcomponents != null) && activationIndex >= startStates.Count)
                        {
                            activationsByInputEventType[e.type].RemoveAt(activationIndex);
                        }
                    }
                }
                if(singleSource)
                    break;
            }
            return (outputEvents, pullRequests);
        }

        //searches through the automaton event buffer for the specified event types to pull with and if successful (i.e., all are available) returns them
        private List<AbstractEvent> determineEventsToPullWith(List<EventType> types, Activation activation)
        {
            var resultList = new List<AbstractEvent>();
            HashSet<EventType> resultListTypes = new HashSet<EventType>();
            foreach(var bufferedEvent in activation.eventBuffer)
            {
                foreach(var bufferedEventComponent in bufferedEvent.getAllEventComponents())
                {
                    if(types.Contains(bufferedEventComponent.type)) 
                    {
                        resultListTypes.Add(bufferedEventComponent.type);
                        resultList.Add(bufferedEventComponent);
                    }
                }
            }
            //the search was successful, if the number of required event types equals the number of available event types
            if(types.Count() == resultListTypes.Count())
            {
                if (resultList.Count() != resultListTypes.Count())
                    Console.WriteLine("IT HAPPENED!!!!!!1!");
                return resultList;
            }
            else
            {
                return new List<AbstractEvent>();
            }
        }


        //transforms an EventType to a list of chars containing alls primitive event types to construct the selectivity hash-key
        private List<char> getPrimitiveEventTypes(EventType complexEventType)
        {
            List<char> result = new List<char>();
            if(complexEventType.ToString().Length == 1)
            {
                result.Add(complexEventType.ToString()[0]);
                return result;
            }
                
            var eventTypeString = complexEventType.ToString().TrimAllWhitespace();
            
            for(int currIdx = 4; currIdx < eventTypeString.Length-1; ++currIdx)
            {
                if(Char.IsUpper(eventTypeString[currIdx]))
                {
                    if(eventTypeString[currIdx-1] == '(' || eventTypeString[currIdx-1] == ',')
                    {
                        if(eventTypeString[currIdx+1] == ')' || eventTypeString[currIdx+1] == ',')
                        {
                            //Console.WriteLine(eventTypeString.ToString()[currIdx]);
                            result.Add(eventTypeString.ToString()[currIdx]);
                        }
                    }
                }
            }
            return result;
        }

        //single selectivity keys do look like "A|AB"
        private string determineSingleSelectivityKey(char eventTypeToPull, List<AbstractEvent> eventsToPullWith)
        {
            SortedSet<string> allReceivedUniqueEventTypes = new SortedSet<string>();
            
            foreach(var evnt in eventsToPullWith)
                foreach(var primitiveEvent in evnt.getAllPrimitiveEventComponents())
                    allReceivedUniqueEventTypes.Add(primitiveEvent.type.ToString());
                    
            
            allReceivedUniqueEventTypes.Add(eventTypeToPull.ToString());
            
            string key = eventTypeToPull + "" + '|';
            
            foreach(var eventType in allReceivedUniqueEventTypes)
                key += eventType;
            
            return key;
        }
        
        
        //returns the according selectivity
        private double determineSelectivity(EventType eventTypeToPull, List<AbstractEvent> eventsToPullWith)
        {
            double resultingSelectivity = 1.0;
            var singleEventTypes = getPrimitiveEventTypes(eventTypeToPull);
            
            foreach(var singleEventType in singleEventTypes)
            {
                var singleKey = determineSingleSelectivityKey(singleEventType, eventsToPullWith);
                
                resultingSelectivity *= executionPlan.singleSelectivities[singleKey];
            }
            //Console.WriteLine("RESULTING SELECTIVITY:" + resultingSelectivity);
            return resultingSelectivity;
        }

        //decides for a candidate event e and an activation if the event is dropped or used for transition.
        //the parameter "searchingThroughLocalEventBuffer" is added for the recursive step in "processActivation" 
        //and indicates that the only the pairwise selectivities of the incoming event and all events that have not yet been compared with it are used
        private bool shouldDropEvent(AbstractEvent e, Activation activation)
        {           
            SortedSet<char> allUniqueEventTypes = new SortedSet<char>();
            
            foreach(var eventFromEventBuffer in activation.eventBuffer)
            {
                foreach(var primitiveEvent in getPrimitiveEventTypes(eventFromEventBuffer.type))
                {
                    allUniqueEventTypes.Add(primitiveEvent);
                }
                
            }
            /*
            if (e.nodeName.Equals(nodeName))
            {
                
                
                var singleSelectivity = determineSelectivity(e.type, determineEventsToPullWith(query.pushPullPlan[activation.currentPushPullState].dependencies, activation));
                
                var prob = randomNumberGenerator.NextDouble();
                if(prob > singleSelectivity)
                {
                    return true;
                }

            }*/
            
            
            foreach(var primitiveEvent in getPrimitiveEventTypes(e.type))
            {
                foreach(var uniqueBufferedEventType in allUniqueEventTypes)
                {
                    if(primitiveEvent == uniqueBufferedEventType)
                        continue;
                    
                    //hacky char concatenation with ""
                    string allEventTypes = primitiveEvent < uniqueBufferedEventType ? primitiveEvent + "" + uniqueBufferedEventType : uniqueBufferedEventType + "" + primitiveEvent;
                    
                    
                    //it holds that the pairwise selectivitiy sigma_AB = sigma_A|AB * sigma_B|AB can be determined by single selectivities
                    //therefore, we create the keys for the single selectivities (e.g., A|AB, B|AB) and multiply the corresponding values
                    string key1 = primitiveEvent + "|" + allEventTypes;
                    string key2 = uniqueBufferedEventType + "|" + allEventTypes;

                    double pairwiseSelectivity = (executionPlan.singleSelectivities[key1] * executionPlan.singleSelectivities[key2]);

                    //the resulting pairwise selectivities are checked one by one instead of multiplying the results and checking them only once due to better float precision
                    if(randomNumberGenerator.NextDouble() > pairwiseSelectivity)
                    {
                        return true;
                    }
                }
                
            }
            

            
            return false;
        }

        private void cleanLocalPullEventBuffers(DateTime currentTime)
        {
            foreach(var pullEventBuffer in pullEventBuffers)
            {
                pullEventBuffer.Value.RemoveAll(pullEvent => (currentTime - pullEvent.timeCreated) >= (maxActiveEventBufferTime));
            }
        }
        
        private bool shouldSendPullRequest(List<AbstractEvent> eventsToPullWith, int currentPushPullState)
        {
            
            int pullEventCounter = eventsToPullWith.Count;
            int counter = 0;
            foreach(var x in eventsToPullWith)
            {
                string pullRequestID = "";
                pullRequestID += x.ID;
                
                foreach(var eventToAcquire in query.pushPullPlan[currentPushPullState].toAcquire)
                    pullRequestID += eventToAcquire;

                if (!pullRequestAlreadySent.Contains(pullRequestID))
                    ++counter;

            }
            
            if (pullEventCounter != counter)
                if (!(pullEventCounter > 1 && counter > 1))
                {
                    return false;
                }
            
            foreach(var x in eventsToPullWith)
            {
                string pullRequestID = "";
                pullRequestID += x.ID;
                foreach(var eventToAcquire in query.pushPullPlan[currentPushPullState].toAcquire)
                {
                    pullRequestID += eventToAcquire;
                    
                }
                
                if (!pullRequestAlreadySent.Contains(pullRequestID))
                {   
                    foreach(var eventToPullWith in eventsToPullWith)
                    {
                        pullRequestID = "";
                        pullRequestID += eventToPullWith.ID;
                        foreach(var eventToAcquire in query.pushPullPlan[currentPushPullState].toAcquire)
                        {
                            pullRequestID += eventToAcquire;
                        }
                        
                        pullRequestAlreadySent.Add(pullRequestID);
                    }
                    return true;
                }
            }

            return false;
        }

        private void processActivation(AbstractEvent e, DateTime t, Activation activation, List<Tuple<EventType,Activation>> invalidActivations, List<ComplexEvent> outputEvents, List<PullRequestMessage> pullRequests)
        {
            if (shouldDropEvent(e, activation))
            {
                return;
            }
            
            var (newactivations, outputeventcomponents, invalid) = activation.consumeEvent(e, t, timeWindow); // add return field to indicate that the activation should be removed
            
            if (invalid) invalidActivations.Add(new Tuple<EventType,Activation>(e.type,activation));
            
            if (newactivations != null)
            {
                foreach (var newactivation in newactivations)
                {
                    
                    SortedSet<string> newactivationEventIDs = new SortedSet<string>();
                    foreach(var newactivationEvent in newactivation.eventBuffer)
                    {
                        newactivationEventIDs.Add(newactivationEvent.ID);
                    }
                    string activationKey = newactivation.currentState.requiredEventType.ToString();
                    foreach(var ID in newactivationEventIDs)
                        activationKey += ID;
                    if (activationAlreadyProduced.Contains(activationKey))
                    {
                        continue;
                    }
                    activationAlreadyProduced.Add(activationKey);
                    
                    
                    var expectedEventsInEventBuffer = 1;
                    
                    for(int state = 0; state < newactivation.currentPushPullState; ++state)
                        expectedEventsInEventBuffer += query.pushPullPlan[state].toAcquire.Count();
                    
                    var numberOfEventsAfterStateProcessed = expectedEventsInEventBuffer - 1 + query.pushPullPlan[newactivation.currentPushPullState].toAcquire.Count;
                    
                    if(!query.pushPullPlan[newactivation.currentPushPullState].toAcquire.Contains(e.type))
                        continue;
                        
                    if(newactivation.testInvalid(t,timeWindow))
                        continue;
                        
                    
                    //finished a push-pull step
                    if(newactivation.eventBuffer.Count() == numberOfEventsAfterStateProcessed)
                    {
                        //if the push-pull plan finished it may have to be checked against the LPE (MSP)
                        if(newactivation.currentPushPullState+1 == query.pushPullPlan.Count())
                        {
                            foreach(var bufferedLPE in locallyProducedEventTypeBuffer)
                            {
                                if (shouldDropEvent(bufferedLPE.Item1, newactivation))
                                {
                                    continue;
                                }
                                var (ignore, additionalLPEoutputcomponent, notNeeded) = newactivation.consumeEvent(bufferedLPE.Item1, bufferedLPE.Item2, timeWindow);

                                if(additionalLPEoutputcomponent != null)
                                {
                                    outputEvents.Add(new ComplexEvent(query.name, additionalLPEoutputcomponent, nodeName));
                                }
                            }
                        }
                        //if the push-pull plan did not finish yet, send a pull request
                        else
                        {
                            ++newactivation.currentPushPullState;
                            var eventsToPullWith = determineEventsToPullWith(query.pushPullPlan[newactivation.currentPushPullState].dependencies, newactivation);
                            eventsToPullWith.Sort((lhs,rhs)=>(lhs.ID.CompareTo(rhs.ID)));

                            
                            if(!query.pushPullPlan[newactivation.currentPushPullState].toAcquire.Contains(newactivation.currentState.requiredEventType))
                                continue;
                            
                            if(shouldSendPullRequest(eventsToPullWith,newactivation.currentPushPullState))
                            {
                                //determine pull answer destination, if multi sink placement
                                var destinations = new List<NodeName>(query.placement.selectedNodes);
                                
                                //determine pull answer destination, if single sink placement
                                if(query.placement.singleNode != null)
                                    destinations.Add(query.placement.singleNode);
                                    
                                foreach(var eventToAcquire in query.pushPullPlan[newactivation.currentPushPullState].toAcquire)
                                {
                                    //check, if needed event type is locally produced and check for activations
                                    if(pullEventBuffers.ContainsKey(eventToAcquire) && eventToAcquire.Equals(newactivation.currentState.requiredEventType) && pullEventBuffers[eventToAcquire].Count > 0)
                                    {
                                        foreach(var bufferedEvent in pullEventBuffers[eventToAcquire])
                                        {
                                            bool eventWasChecked = (newactivation.lastCheckedLocallyBufferTimeStamp.ContainsKey(eventToAcquire) && bufferedEvent.timeCreated <= newactivation.lastCheckedLocallyBufferTimeStamp[eventToAcquire]);
                                        
                                            //if (shouldDropEvent(bufferedEvent, newactivation) || eventWasChecked)
                                            if (eventWasChecked)
                                            {
                                                continue;
                                            }
                                            //recursive call for further transitions caused by the next event type to acquire
                                            processActivation(bufferedEvent,t,newactivation,invalidActivations,outputEvents,pullRequests);
                                        }
                                        var lastCheckedTimeStamp = pullEventBuffers[eventToAcquire].Select(even => even.timeCreated).ToList().Max();
                                        foreach(var newAct in newactivations)
                                        {
                                            newAct.lastCheckedLocallyBufferTimeStamp[eventToAcquire] = lastCheckedTimeStamp;
                                        }

                                    }

                                    //query.pullRequestHandlingNode denotes the node handling the pull requests (i) for a MSP (ii) for a SSP
                                    if(eventsToPullWith.Count() > 0 && query.pullRequestHandlingNode.Equals(nodeName.ToString()))
                                    {
                                        if (!pullRequestTypeCounter.ContainsKey(eventToAcquire))
                                            pullRequestTypeCounter[eventToAcquire] = 1;
                                        else
                                            ++pullRequestTypeCounter[eventToAcquire];
                                        Console.WriteLine("[" + nodeName + "] " + "Number of pull requests for " + eventToAcquire + " is " + pullRequestTypeCounter[eventToAcquire] + " - " + eventsToPullWith[0]);

                                        pullRequests.Add(new PullRequestMessage(nodeName, eventToAcquire, eventsToPullWith, destinations, query));
                                    }
                                }
                            }

                        }
                    }
                    //check if locally buffered pull event types (types pulled from other nodes, but also producing them locally too!) fit for a transition
                    else
                    {
                        
                        if(!query.pushPullPlan[newactivation.currentPushPullState].toAcquire.Contains(newactivation.currentState.requiredEventType))
                            continue;
                            
                        foreach(var eventToAcquire in query.pushPullPlan[newactivation.currentPushPullState].toAcquire)
                        {
                            if(pullEventBuffers.ContainsKey(eventToAcquire) && eventToAcquire.Equals(newactivation.currentState.requiredEventType) && pullEventBuffers[eventToAcquire].Count > 0)
                            {
                                foreach(var bufferedEvent in pullEventBuffers[eventToAcquire])
                                {
                                    bool eventWasChecked = (newactivation.lastCheckedLocallyBufferTimeStamp.ContainsKey(eventToAcquire) && bufferedEvent.timeCreated <= newactivation.lastCheckedLocallyBufferTimeStamp[eventToAcquire]);

                                    //if (shouldDropEvent(bufferedEvent, newactivation) || eventWasChecked)
                                    if (eventWasChecked)
                                    {
                                        continue;
                                    }
                                    processActivation(bufferedEvent,t,newactivation,invalidActivations,outputEvents,pullRequests);
                                }
                                
                                    var lastCheckedTimeStamp = pullEventBuffers[eventToAcquire].Select(even => even.timeCreated).ToList().Max();
                                
                                foreach(var newAct in newactivations)
                                {
                                    newAct.lastCheckedLocallyBufferTimeStamp[eventToAcquire] = lastCheckedTimeStamp;
                                }

                            }
                        }
                   }

                   
                   activationsByInputEventType[newactivation.currentState.requiredEventType].Add(newactivation);
                }
            }

            
            if (outputeventcomponents != null)
            {
                outputEvents.Add(new ComplexEvent(query.name, outputeventcomponents, nodeName));
            }
        }


        private (IEnumerable<ComplexEvent>, IEnumerable<PullRequestMessage>) skipTillAnyMatch(AbstractEvent e, DateTime t)
        {  
            //Console.WriteLine("Entered skipTillAnyMatch");
            if (!activationsByInputEventType.ContainsKey(e.type)) return (Enumerable.Empty<ComplexEvent>(), Enumerable.Empty<PullRequestMessage>()); 
            
            cleanLocalPullEventBuffers(e.timeCreated);

            if(!pullEventBuffers.ContainsKey(e.type) && !query.pushPullPlan[0].toAcquire.Contains(e.type))
            {
                pullEventBuffers.Add(e.type, new List<AbstractEvent>());
            }
            
            //if event is not in push event type group
            if(!query.pushPullPlan[0].toAcquire.Contains(e.type))
                pullEventBuffers[e.type].Add(e);


            //Buffer locally produced event type to apply it to MSP projections
            if (query.placement.isMultiSinkPlacement() && e.type.Equals(query.placement.allSourcesOfEvent))
                locallyProducedEventTypeBuffer.Add(new Tuple<AbstractEvent,DateTime>(e,t));
            
            List<ComplexEvent> outputEvents = new List<ComplexEvent>();
            List<PullRequestMessage> pullRequests = new List<PullRequestMessage>();
            
            List<Tuple<EventType,Activation>> invalidActivations = new List<Tuple<EventType,Activation>>(); //Samira [for removing partial matches due to expired timestamp]
            
            foreach (var activation in activationsByInputEventType[e.type])
            {
                processActivation(e,t, activation, invalidActivations, outputEvents, pullRequests);
            }


            return (outputEvents,pullRequests);
        }
         
        public override void removeActivations(DateTime t)
        {
            List<Tuple<EventType,Activation>> invalidActivations = new List<Tuple<EventType,Activation>>();
            foreach (var key in activationsByInputEventType.Keys)
            {
                foreach (var act in activationsByInputEventType[key])
                {
                    if (act.testInvalid(t,timeWindow))
                    { 
                        invalidActivations.Add(new Tuple<EventType,Activation>(key,act));
                    }
                }
            }
            foreach (var tuple in invalidActivations)
            {   
                removedActivations++;
                activationsByInputEventType[tuple.Item1].Remove(tuple.Item2);
            }  
        }
    }
}
