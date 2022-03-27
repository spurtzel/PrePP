using DCEP.Core;
using DCEP.Core.DCEPControlMessage;
using DCEP.Core.QueryProcessing;
using DCEP.Core.Utils;
using DCEP.Core.Utils.DeepCloneExtension;
using System.Diagnostics;
using System;
using System.Globalization;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using DCEP.AmbrosiaNodeAPI;
using DCEP.Node.Benchmarking;
using DCEP.Core.Utils.LeastCommonAncestor;
using DCEP.Core.QueryProcessing.Operators;


namespace DCEP.Node
{
    
    public class PulledEvent
    {
        public PulledEvent(AbstractEvent pulledEvent,NodeName destination)
        {
            this.pulledEvent = pulledEvent;
            this.destination = destination;
        }
        
        public AbstractEvent pulledEvent;
        public NodeName destination;
    }
    
    public class ActivePullRequest
    {
        public ActivePullRequest(PullRequestMessage pullRequest, DateTime timeReceived)
        {
            this.pullRequest = pullRequest;
            this.timeReceived = timeReceived;
        }
        
        public PullRequestMessage pullRequest;
        public DateTime timeReceived;
    }
    
    
    [DataContract]
    public class DCEPNode : IAmbrosiaNode
    {
        [DataMember] NodeExecutionState state;
        [DataMember] private readonly SerializableQueue<AbstractEvent> externalEventQueue;
        [DataMember] private readonly SerializableQueue<AbstractEvent> internalEventQueue;
        [DataMember] private readonly SerializableQueue<PulledEvent> pullEventQueue;
        
        [DataMember] private readonly SerializableQueue<DCEPControlMessage> controlMessageQueue;
        [DataMember] public Dictionary<EventType, ForwardRule> forwardRules { get; set; }
        [DataMember] private readonly DCEPSettings settings;
        [DataMember] private readonly string TAG;
        [DataMember] private bool dataSetMode; //new
        [DataMember] public long receivedEventCount { get; set; }
        [DataMember] public long receivedPullRequestsCount { get; set; }
        [DataMember] public long locallyGeneratedComplexEventCount { get; set; }
        [DataMember] public long locallyGeneratedPrimitiveEventCount { get; set; }
        
        [DataMember] public long locallyDroppedComplexEvents { get; set; }
        [DataMember] public long locallyDroppedPartialMatches { get; set; }
        [DataMember] public NodeName nodeName { get; set; }
        [DataMember] private List<QueryProcessor> queryProcessors;
        [DataMember] private ForwardRuleProcessor forwardRuleProcessor;
        [DataMember] private Random randomNumberGenerator = new Random(42); // for the implementation of selection rates
        [DataMember] Stopwatch stopwatch = new Stopwatch(); // for performance benchmarking
        [DataMember] ExecutionPlan executionPlan;
        [DataMember] private readonly BenchmarkMeter benchmarkMeter;
        [DataMember] PrimitiveEventSourceService primitiveEventSourceService;
        [DataMember] private DirectorNodeService directorNodeService = null;
        //private Dictionary<NodeName, IAmbrosiaNodeProxy> proxyDict;
        //private IAmbrosiaNodeProxy directorNodeProxy { get; set; }
        //private IAmbrosiaNodeProxy thisProxy { get; set; }

        private INodeProxyProvider proxyProvider;
        [DataMember] long lastStatusMessageToCoordinator = -1;
        [DataMember] private bool sentReadyToStartMessage;
        [DataMember] private long _remainingTimeLastPrintTime = 0;
        [DataMember] private long _remainingTimeLastProcessedCount = 0;
        
        [DataMember] private ConcurrentDictionary<NodeName, DateTime> TimestampsDict = new ConcurrentDictionary<NodeName, DateTime>(); 
        [DataMember] private DateTime oldestTimestamp = DateTime.MinValue; // 
        
        [DataMember] private Dictionary<EventType, List<AbstractEvent>> pullEventBuffers;
        
        // NodeName, events to pull with, event to pull
        struct bufferedEventsToBuildPullRequestsKey 
        {
            public NodeName sourceNode;
            public EventType eventTypeToPullWith;
            public EventType eventTypeToPull;
            public override int GetHashCode()
            {
                return (sourceNode,eventTypeToPullWith,eventTypeToPull).GetHashCode();
            }
        }
        
        [DataMember] private Dictionary<bufferedEventsToBuildPullRequestsKey, Dictionary<string, AbstractEvent>> bufferedEventsToBuildPullRequests;
        
        [DataMember] private Dictionary<EventType, List<ActivePullRequest>> activePullRequestsByInputEventType;
    
        private TimeSpan timeWindow = TimeSpan.FromSeconds(30);
        private TimeSpan maxActivePullRequestDuration = TimeSpan.FromSeconds(30);
        
        [DataMember] private Dictionary<Tuple<NodeName,EventType>, DateTime> latestPullAnswerToNode = new Dictionary<Tuple<NodeName,EventType>, DateTime>();
        
        [DataMember] private HashSet<NodeName> potentialPullEventDestinations = new HashSet<NodeName>();
        
        //[DataMember] private HashSet<String> alreadyGenerated = new HashSet<String>(); //
        
        
        //[DataMember] private int pullEventCounter;
        
        public DCEPNode(NodeName name, string[] inputlines, DCEPSettings settings)
        {
            TAG = "[" + name + "] ";
            Console.WriteLine(TAG + "DCEPNode Constructor called.");
            state = NodeExecutionState.WaitForStart;
            receivedEventCount = 0;
            receivedPullRequestsCount = 0;
            sentReadyToStartMessage = false;
            nodeName = name;
            
            externalEventQueue = new SerializableQueue<AbstractEvent>();
            internalEventQueue = new SerializableQueue<AbstractEvent>();
            pullEventQueue = new SerializableQueue<PulledEvent>();
            
            controlMessageQueue = new SerializableQueue<DCEPControlMessage>();
            queryProcessors = new List<QueryProcessor>();
            this.settings = settings;
            executionPlan = new ExecutionPlan(inputlines);
            benchmarkMeter = new BenchmarkMeter(settings, nodeName);
            createQueryProcessors(executionPlan.queriesByNodeName[nodeName]);
            dataSetMode = false; // new
            
            bufferedEventsToBuildPullRequests = new Dictionary<bufferedEventsToBuildPullRequestsKey, Dictionary<string, AbstractEvent>>();
            
            activePullRequestsByInputEventType = new Dictionary<EventType, List<ActivePullRequest>>();
            
            initPotentialPullEventDestinations();
            initPullEventBuffers();
            timeWindow = settings.timeUnit.GetTimeSpanFromDuration(settings.timeWindow);
            maxActivePullRequestDuration = settings.timeUnit.GetTimeSpanFromDuration(settings.timeWindow);
        }


        private void initPotentialPullEventDestinations()
        {
            foreach(var queries in executionPlan.queriesByNodeName)
            {
                foreach(var query in queries.Value)
                {
                    for(int idx = 1; idx < query.pushPullPlan.Count; ++idx)
                    {
                        foreach(var eventTypeToAcquire in query.pushPullPlan[idx].toAcquire)
                        {
                            if(!executionPlan.sourceNodesByEventName.ContainsKey(eventTypeToAcquire))
                                throw new ArgumentException("Event type " + eventTypeToAcquire + " is required which is not produced.");
                            
                            
                            if(executionPlan.sourceNodesByEventName[eventTypeToAcquire].Contains(nodeName))
                                potentialPullEventDestinations.Add(queries.Key);
                        }
                    }
                }
            }
        }

        private void initPullEventBuffers()
        {
            pullEventBuffers = new Dictionary<EventType, List<AbstractEvent>>();
            foreach(var queries in executionPlan.queriesByNodeName)
            {
                foreach(var query in queries.Value)
                {
                    for(int idx = 1; idx < query.pushPullPlan.Count; ++idx)
                    {
                        foreach(var eventTypeToAcquire in query.pushPullPlan[idx].toAcquire)
                        {
                            if(!executionPlan.sourceNodesByEventName.ContainsKey(eventTypeToAcquire))
                                throw new ArgumentException("Event type " + eventTypeToAcquire + " is required which is not produced.");
                            
                            //Console.WriteLine(eventTypeToAcquire);
                            
                            if(!pullEventBuffers.ContainsKey(eventTypeToAcquire) && executionPlan.sourceNodesByEventName[eventTypeToAcquire].Contains(nodeName))
                                pullEventBuffers.Add(eventTypeToAcquire, new List<AbstractEvent>());
                        }
                    }
                }
            }
            

            
        }

        private void initPrimitiveEventSourceService()
        {
            switch (executionPlan.primitiveInputMode)
            {
                case PrimitiveInputMode.RANDOM_WITH_RATES:
                    primitiveEventSourceService =  new RandomPrimitiveEventGenerationService(nodeName,
                        executionPlan.networkPlan[nodeName],
                        proxyProvider,
                        settings);
                    break;
            
                case PrimitiveInputMode.DATASET:
                    dataSetMode = true; //new
                    primitiveEventSourceService = new DatasetPrimitiveEventInputService(proxyProvider,
                        TAG,
                        executionPlan.datasetFileNameTemplate,
                        nodeName,
                        settings,
                        executionPlan.networkPlan[nodeName]);
                    break;
            
                default:
                    throw new ArgumentException("Unknown primitiveInputMode in executionPlan.");
            }
        }

        public void onFirstStart(INodeProxyProvider proxyProvider){
            this.proxyProvider = proxyProvider;


            if (settings.directorNodeName == null){
                throw new ArgumentException(TAG + "DirectorNodeName must not be null.");
            }

            
            var forwardRules = executionPlan.forwardRulesByNodeName[nodeName];
            this.forwardRuleProcessor = new ForwardRuleProcessor(TAG, forwardRules, proxyProvider);

            initPrimitiveEventSourceService();

            if (this.nodeName.Equals(settings.directorNodeName))
            {
                directorNodeService = new DirectorNodeService(TAG,
                                                              executionPlan.networkPlan.Keys.ToList(),
                                                              proxyProvider,
                                                              settings);
            }
        }

        public void threadStartMethod()
        {
            stopwatch.Start();

            while (true)
            {
                processControlMessages();

                switch (state)
                {
                    case NodeExecutionState.WaitForStart:
 /*                        // broadcast isready signal to directorNode every second
                        if (stopwatch.ElapsedMilliseconds - lastStatusMessageToCoordinator > 1000){
                            proxyProvider.getProxy(settings.directorNodeName).ReceiveDCEPControlMessageFork(new NodeIsReadyToStartMessage(nodeName));
                            //Console.WriteLine(TAG + "sending ready to start message to director node "+settings.directorNodeName.ToString());
                            lastStatusMessageToCoordinator = stopwatch.ElapsedMilliseconds;
                        } */

                        if (!sentReadyToStartMessage){
                            proxyProvider.getProxy(settings.directorNodeName).ReceiveDCEPControlMessageFork(new NodeIsReadyToStartMessage(nodeName));
                            sentReadyToStartMessage = true;
                        }

                    break;

                    case NodeExecutionState.DoStartInputGeneration:
                        // TODO: check if this is not already running and throw an error if it is
                        primitiveEventSourceService.start();
                        state = NodeExecutionState.Running;
                        processingStep();
                    break;

                    case NodeExecutionState.Running:
                        processingStep();
                    break;

                    case NodeExecutionState.DoStopInputGeneration:
                        primitiveEventSourceService.stop();
                        state = NodeExecutionState.ProcessingRemainder;

                    break;

                    case NodeExecutionState.ProcessingRemainder:
                        processingStep();
                        
                        // when queues are empty, send isReadyToTerminate message every second 
                        if (getQueuedEventCount() == 0)
                        {
                            if (stopwatch.ElapsedMilliseconds - lastStatusMessageToCoordinator > 1000)
                            {
                                proxyProvider.getProxy(settings.directorNodeName)
                                    .ReceiveDCEPControlMessageFork(new NodeIsReadyToTerminateMessage(nodeName));
                                lastStatusMessageToCoordinator = stopwatch.ElapsedMilliseconds;
                            }
                        }

                        break;

                    case NodeExecutionState.DoSendExperimentDataAndTerminate:

                       
                        var data = new ExperimentRunData(
                            locallyGeneratedComplexEventCount,
                            receivedEventCount,
                            locallyGeneratedPrimitiveEventCount,
                            locallyDroppedComplexEvents,
                            locallyDroppedPartialMatches,
                            receivedPullRequestsCount);
                        
                        proxyProvider.getProxy(settings.directorNodeName).ReceiveDCEPControlMessageFork(new ExperimentRunNodeDataMessage(nodeName, data));
                        Console.WriteLine(TAG + "Sent experiment data. Update loop is terminating.");
                        Thread.Sleep(500);
                        if (getQueuedEventCount() > 0)
                        {
                            Console.WriteLine(TAG + String.Format("WARNING: requested to terminate with {0} events left in queue.", getQueuedEventCount()));
                        }

                        if (directorNodeService != null){
                            while(!directorNodeService.localNodeCanTerminate){
                                processControlMessages();
                            }
                        }
                        return;



                    case NodeExecutionState.DoTerminate:
                        if (getQueuedEventCount() > 0){
                            Console.WriteLine(TAG + String.Format("WARNING: requested to terminate with {0} events left in queue.", getQueuedEventCount()));
                        }
                        return;
                }

            }
        }

        public long getQueuedEventCount()
        {
            return internalEventQueue.Data.LongCount() + externalEventQueue.Data.LongCount();
        }

        private void createQueryProcessors(IEnumerable<Query> queries)
        {
            var timeWindow = settings.timeUnit.GetTimeSpanFromDuration(settings.timeWindow);
            timeWindow = timeWindow.Multiply(1.0 / settings.datasetSpeedup);
            
            foreach (var q in queries)
            {
                var processor = QueryProcessor.getQueryProcessorForQuery(q, timeWindow, executionPlan, nodeName);
                if (processor != null)
                {
                    queryProcessors.Add(processor);
                }
                else
                {
                    Console.WriteLine(TAG + String.Format("!WARNING! - Inactive Query '{0}' due not no matching QueryProcessor implementation.", q.ToString()));
                }
            }
        }


        //because of the arbitrary ordering of a push-pull plan (i.e., ,,lazy evaluation'') the sequence constraints have to be checked for satisfaction 
        private bool sequenceConstraintSatisfied(Query queryToProcess, AbstractEvent firstEvent, AbstractEvent secondEvent)
        {
            var leastCommonAncestorFinder = new LeastCommonAncestorFinder<QueryComponent>(queryToProcess.rootOperator);
            
            foreach(var primEventFirstEvent in firstEvent.getAllPrimitiveEventComponents())
            {
                foreach(var primEventSecondEvent in secondEvent.getAllPrimitiveEventComponents())
                {
                    var op = (AbstractQueryOperator)leastCommonAncestorFinder.FindCommonParent(primEventFirstEvent.type, primEventSecondEvent.type);
                    
                    if (op is SEQOperator)
                    {
                        // derive predecessor or successor constraint
                        var first = op.getFirstOccuringChildOf(primEventFirstEvent.type, primEventSecondEvent.type);

                        if (primEventFirstEvent.Equals(first) && primEventFirstEvent.timeCreated > primEventSecondEvent.timeCreated)
                        {
                            return false;
                        }
                        
                        if (primEventSecondEvent.Equals(first) && primEventFirstEvent.timeCreated < primEventSecondEvent.timeCreated)
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }



        //checks for a single buffered event if it satisfies all related sequence constraints
        private bool eventMatchesPullRequest(AbstractEvent candidateEvent, List<AbstractEvent> eventsToPullWith, Query queryToProcess)
        {            
            var oldestTimestampEvent = eventsToPullWith.Select(even => even.timeCreated).ToList().Min();
            var newestTimestampEvent = eventsToPullWith.Select(even => even.timeCreated).ToList().Max();
            var timeWindow = settings.timeUnit.GetTimeSpanFromDuration(settings.timeWindow);
            timeWindow = timeWindow.Multiply(1.0 / settings.datasetSpeedup);
            if (((newestTimestampEvent - candidateEvent.timeCreated).Duration() >= timeWindow))
                return false;
            
            if (((candidateEvent.timeCreated - oldestTimestampEvent).Duration() >= timeWindow))
                return false;
            
            HashSet<EventType> satisfied = new HashSet<EventType>();
            HashSet<EventType> toSatisfy = new HashSet<EventType>();
            
            bool anySeqConstraintSatisfied = false;
            foreach(var eventToPullWith in eventsToPullWith)
            {
                toSatisfy.Add(eventToPullWith.type);
                if(sequenceConstraintSatisfied(queryToProcess, candidateEvent, eventToPullWith))
                    satisfied.Add(eventToPullWith.type);
            } 


            if(satisfied.Count() != toSatisfy.Count())
            {
                //Console.WriteLine("Not Satisfied!!");
                return false;
            }
            //Console.WriteLine("Is Satisfied!!");
            return true;
        }

        //after receiving a pull request, the corresponding local buffers are searched through for all potential matching candidates. 
        //therefore, two things are checked: 1. does the event satisfy all sequence constraints? 2. does it match despite the selectivity?
        private List<AbstractEvent> determineEventsToBePulled(EventType eventTypeToPull, List<AbstractEvent> eventsToPullWith, Query queryToProcess)
        {

            List<AbstractEvent> resultEventSet = new List<AbstractEvent>();
            
            //Console.WriteLine(eventTypeToPull);
            lock(pullEventBuffers)
            {
                if(!pullEventBuffers.ContainsKey(eventTypeToPull))
                    Console.WriteLine("Event is missing");
                
                var dropProbability = determineSelectivity(eventTypeToPull, eventsToPullWith);
                
                foreach(var candidateEvent in pullEventBuffers[eventTypeToPull])
                {

                    if(eventMatchesPullRequest(candidateEvent, eventsToPullWith, queryToProcess))
                    {
                        if(shouldDropEvent(dropProbability))
                            continue;
                            
                        resultEventSet.Add(candidateEvent);

                    }
                }
                
            }
            return resultEventSet;
        }


        //event buffer space management watermark based on latest time stamp already processed
        private void cleanUpPullEventBuffers(DateTime latestPullAnswer)
        {
            lock(pullEventBuffers)
            {
                //DateTime currentTime = DateTime.Now;
                //remove all locally buffered events based on the highest time stamp every node processed yet
                foreach(var pullEventBuffer in pullEventBuffers)
                {
                    pullEventBuffer.Value.RemoveAll(pullEvent => (latestPullAnswer - pullEvent.timeCreated) >= timeWindow);
                }
            }
            
        }


        //kinda hacky way for checking if an event is primitive or complex..
        private bool isComplexEvent(EventType candidateEvent)
        {
            return candidateEvent.ToString().Length > 1;
        }


        //transforms an EventType to a list of chars containing alls primitive event types to construct the selectivity hash-key
        private List<char> getPrimitiveEventTypes(EventType complexEventType)
        {
            var eventTypeString = complexEventType.ToString().TrimAllWhitespace();
            List<char> result = new List<char>();
            
            for(int currIdx = 4; currIdx < eventTypeString.Length-1; ++currIdx)
            {
                if(Char.IsUpper(eventTypeString[currIdx]))
                {
                    if(eventTypeString[currIdx-1] == '(' || eventTypeString[currIdx-1] == ',')
                    {
                        if(eventTypeString[currIdx+1] == ')' || eventTypeString[currIdx+1] == ',')
                        {
                            result.Add(eventTypeString.ToString()[currIdx]);
                        }
                    }
                }
            }
            return result;
        }

        
        //determine the selectivity, if a complex event is pulled. for instance, SEQ(A,B) has to be pulled with C, then there is no SEQ(A,B)|SEQ(A,B)C inside the hashmap.
        //therefore, sel(A|ABC) * sel(B|ABC) has to be determined
        private string determineComplexSingleSelectivityKey(char eventTypeToPull, List<char> allPrimitiveComplexEvents, List<AbstractEvent> eventsToPullWith)
        {
            SortedSet<string> allReceivedUniqueEventTypes = new SortedSet<string>();
            
            foreach(var evnt in eventsToPullWith)
                foreach(var primitiveEvent in evnt.getAllPrimitiveEventComponents())
                    allReceivedUniqueEventTypes.Add(primitiveEvent.type.ToString());
                    
            foreach(var primitiveEvent in allPrimitiveComplexEvents)
                allReceivedUniqueEventTypes.Add(Char.ToString(primitiveEvent));
            
            allReceivedUniqueEventTypes.Add(Char.ToString(eventTypeToPull));
            
            string key = eventTypeToPull.ToString() + '|';
            
            foreach(var eventType in allReceivedUniqueEventTypes)
                key += eventType;
            
            return key;
        }


        //single selectivity keys do look like "A|AB"
        private string determineSingleSelectivityKey(EventType eventTypeToPull, List<AbstractEvent> eventsToPullWith)
        {
            SortedSet<string> allReceivedUniqueEventTypes = new SortedSet<string>();
            
            foreach(var evnt in eventsToPullWith)
                foreach(var primitiveEvent in evnt.getAllPrimitiveEventComponents())
                    allReceivedUniqueEventTypes.Add(primitiveEvent.type.ToString());
                    
            
            allReceivedUniqueEventTypes.Add(eventTypeToPull.ToString());
            
            string key = eventTypeToPull.ToString() + '|';
            
            foreach(var eventType in allReceivedUniqueEventTypes)
                key += eventType;
            
            return key;
        }


        //returns the according selectivity
        private double determineSelectivity(EventType eventTypeToPull, List<AbstractEvent> eventsToPullWith)
        {
            if(isComplexEvent(eventTypeToPull))
            {
                double resultingSelectivity = 1.0;
                var singleEventTypes = getPrimitiveEventTypes(eventTypeToPull);
                
                foreach(var singleEventType in singleEventTypes)
                {
                    var singleKey = determineComplexSingleSelectivityKey(singleEventType, singleEventTypes, eventsToPullWith);
                    
                    resultingSelectivity *= executionPlan.singleSelectivities[singleKey];
                }
                //Console.WriteLine("RESULTING SELECTIVITY:" + resultingSelectivity);
                return resultingSelectivity;
            }
            
            var key = determineSingleSelectivityKey(eventTypeToPull, eventsToPullWith);
            
            return executionPlan.singleSelectivities[key];
        }

        //if an event does not match selecitvity-wise, then drop it
        private bool shouldDropEvent(double dropProbability)
        {
            //Console.WriteLine("dropProbability:" + dropProbability);
            var doDropIt = randomNumberGenerator.NextDouble() > dropProbability;
            if (doDropIt)
            {
                //Console.WriteLine("Event is dropped due to selectivity.");
                return true;
            }
            return false;
        }

        private void clearPullRequestBuildEventBuffers(DateTime currentTime)
        {
            foreach(var pullEventBuffer in bufferedEventsToBuildPullRequests)
            {
                List<string> toDelete = new List<string>();
                
                foreach(var pullEvent in pullEventBuffer.Value)
                {
                    if ((currentTime - pullEvent.Value.timeCreated).Duration() >= maxActivePullRequestDuration)
                        toDelete.Add(pullEvent.Key);
                }
                
                foreach(var delete in toDelete)
                {
                    pullEventBuffer.Value.Remove(delete);
                }
            }
        }

        private bool doEventsMatch(AbstractEvent firstEvent, AbstractEvent secondEvent)
        {
            
            var timeWindow = settings.timeUnit.GetTimeSpanFromDuration(settings.timeWindow);
            timeWindow = timeWindow.Multiply(1.0 / settings.datasetSpeedup);
            if (((firstEvent.timeCreated - secondEvent.timeCreated).Duration() >= timeWindow))
                return false;
            
            
            //hacky char concatenation with ""
            string allEventTypes = firstEvent.type.ToString()[0] < secondEvent.type.ToString()[0] ? firstEvent.type + "" + secondEvent.type : secondEvent.type + "" + firstEvent.type;
            
            //it holds that the pairwise selectivitiy sigma_AB = sigma_A|AB * sigma_B|AB can be determined by single selectivities
            //therefore, we create the keys for the single selectivities (e.g., A|AB, B|AB) and multiply the corresponding values
            string key1 = firstEvent.type + "|" + allEventTypes;
            string key2 = secondEvent.type + "|" + allEventTypes;

            double pairwiseSelectivity = (executionPlan.singleSelectivities[key1] * executionPlan.singleSelectivities[key2]);


            //the resulting pairwise selectivities are checked one by one instead of multiplying the results and checking them only once due to better float precision
            if(randomNumberGenerator.NextDouble() > pairwiseSelectivity)
            {
                return false;
            }
            
            return true;
        }

        private void buildPullRequests(PullRequestMessage pullRequest, List<AbstractEvent> partialPullRequest, AbstractEvent eventToBuildPullRequets, HashSet<string> alreadyBuilt)
        {
            //the end of the recursion is reached if we have collected as many events as needed for the according pull request 
            //e.g., if we want to pull with ABC, but only send an A as pull request, we search through the buffers for a matching B and a matching C and save them into the partialPullRequest
            //so if the number of collected events equals the number of events we originally wanted to pull with, the recursion stops as a potential pull request match was found.
            if (partialPullRequest.Count == pullRequest.eventsToPullWith.Count)
            {
                bool valid = false;
                for(int index = 0; index < partialPullRequest.Count; ++index)
                {
                    //this is to avoid that the initial pull request is build multiple times
                    //therefore, only build a pull request from a partial pull requests, if it includes the same number of events to pull with
                    //and if at least one ID differs from the initial pull request (as the initial pull request was already handled)
                    if (!partialPullRequest[index].ID.Equals(pullRequest.eventsToPullWith[index].ID))
                    {
                        valid = true;
                        break;
                    }
                }
                
                if (valid)
                {
                    PullRequestMessage newPullRequestMessage = new PullRequestMessage(pullRequest.sendingNode, pullRequest.pullEvent, partialPullRequest, pullRequest.destinations, pullRequest.queryToProcess);
                    handlePullRequest(newPullRequestMessage);
                }
                return;
            }
            
            EventType nextEventTypeToAcquire = pullRequest.eventsToPullWith[partialPullRequest.Count].type;
            if (nextEventTypeToAcquire.Equals(eventToBuildPullRequets.type))
            {
                foreach(var evnt in partialPullRequest)
                {
                    bool isKnown = false;
                    foreach(var knownEvent in pullRequest.eventsToPullWith)
                    {
                        if (knownEvent.ID.Equals(evnt.ID))
                            isKnown = true;
                    }
                    if (!doEventsMatch(evnt, eventToBuildPullRequets) && !isKnown)
                        return;
                }
                partialPullRequest.Add(eventToBuildPullRequets);
                buildPullRequests(pullRequest, partialPullRequest, eventToBuildPullRequets, alreadyBuilt);
                
            }
            else
            {                
                bufferedEventsToBuildPullRequestsKey key;
                key.sourceNode = pullRequest.sendingNode;
                key.eventTypeToPullWith = nextEventTypeToAcquire;
                key.eventTypeToPull = pullRequest.pullEvent;
                
                foreach(var bufferedEvent in bufferedEventsToBuildPullRequests[key])
                {
                    if (alreadyBuilt.Contains(bufferedEvent.Value.ID))
                        continue;
                    
                    bool constraintsSatisfied = true;
                    foreach(var evnt in partialPullRequest)
                    {
                        if (!doEventsMatch(evnt, bufferedEvent.Value))
                        {
                            constraintsSatisfied = false;
                            break;
                        }
                    }
                    
                    if (!constraintsSatisfied)
                        continue;
                    
                    var copiedPartialPullRequests = new List<AbstractEvent>(partialPullRequest);
                    copiedPartialPullRequests.Add(bufferedEvent.Value);
                    
                    buildPullRequests(pullRequest, copiedPartialPullRequests, eventToBuildPullRequets, alreadyBuilt);
                
                }
            }
        }

        private void createAndHandlePullRequest(PullRequestMessage pullRequest)
        {
            DateTime newestTimeStamp = new DateTime();
            foreach(var eventToPullWith in pullRequest.eventsToPullWith)
                newestTimeStamp = newestTimeStamp > eventToPullWith.timeCreated ? newestTimeStamp : eventToPullWith.timeCreated;
            clearPullRequestBuildEventBuffers(newestTimeStamp);
            
            List<AbstractEvent> newEventsToCreatePullRequests = new List<AbstractEvent>();
            foreach(var eventToPullWith in pullRequest.eventsToPullWith)
            {
                bufferedEventsToBuildPullRequestsKey key;
                key.sourceNode = pullRequest.sendingNode;
                key.eventTypeToPullWith = eventToPullWith.type;
                key.eventTypeToPull = pullRequest.pullEvent;
                
                if (!bufferedEventsToBuildPullRequests.ContainsKey(key))
                    bufferedEventsToBuildPullRequests[key] = new Dictionary<string, AbstractEvent>();
                
                //an event is "new" if it does not appear in the pull-request-build buffers.
                if (!bufferedEventsToBuildPullRequests[key].ContainsKey(eventToPullWith.ID))
                    newEventsToCreatePullRequests.Add(eventToPullWith);
                
                //TryAdd does not throw an exception, if an element is added which was already contained
                bufferedEventsToBuildPullRequests[key].TryAdd(eventToPullWith.ID,eventToPullWith);
                    
            }
            HashSet<string> alreadyBuilt = new HashSet<string>();
            
            //for each new event we check the corresponding pull-request-build buffers for potential matchings (i.e., to build pull requests)
            foreach(var newEvent in newEventsToCreatePullRequests)
            {
                buildPullRequests(pullRequest, new List<AbstractEvent>(), newEvent, alreadyBuilt);
                
                //to avoid that pull requests are build using an event type multiple times
                alreadyBuilt.Add(newEvent.ID);
            }
                
            
            handlePullRequest(pullRequest);
        }

        //after receiving a pull request, it has to be handled:
        //1. determine the result set of all locally buffered events which match the received pull request.
        //2. iterate through all pull-request destinations (i.e., all nodes interested in the matching events) and put them into the pull event queue (which is processed in ProcessingStep afterwards)
        //3. based on the specified time-window it could be that further events are generated which match the pull request. therefore, a list of ,,active pull-requests'' has to be managed,
        //   whereby newly generated events are potentially checked against all active pull-requests and sent. after the time-window expiration, a pull-request is finally removed.
        private void handlePullRequest(PullRequestMessage pullRequest)
        {

            var eventsToBePulled = determineEventsToBePulled(pullRequest.pullEvent, pullRequest.eventsToPullWith, pullRequest.queryToProcess);
            DateTime latestPullAnswer = new DateTime();
            foreach(var destination in pullRequest.destinations)
            {
                foreach(var eventToBePulled in eventsToBePulled)
                {
                    bool doSend = true;
                    
                    //.. process it according to the watermark and..
                    lock(latestPullAnswerToNode)
                    {
                        var key = new Tuple<NodeName,EventType>(destination,eventToBePulled.type);
                        if(latestPullAnswerToNode.ContainsKey(key) && latestPullAnswerToNode[key] >= eventToBePulled.timeCreated)
                            doSend = false;
                        
                        //.. update the watermark accordingly (the watermark increased, if the pull event creation time is newer than the old watermark)
                        if(latestPullAnswerToNode.ContainsKey(key))
                        {
                            DateTime latestKnownTime = latestPullAnswerToNode[key];
                            latestPullAnswerToNode[key] = (latestKnownTime > eventToBePulled.timeCreated ? latestKnownTime : eventToBePulled.timeCreated);
                        }
                        else
                        {
                            latestPullAnswerToNode.Add(key, eventToBePulled.timeCreated);
                        }
                        latestPullAnswer = latestPullAnswer > eventToBePulled.timeCreated ? latestPullAnswer : eventToBePulled.timeCreated;
                    }
                    
                    //if the event fulfills everything, it can be sent to the destination node
                    if(doSend)
                    {
                        Console.WriteLine(TAG + "Pull answer sent!" + eventToBePulled + " - to " + destination);
                        pullEventQueue.Data.Enqueue(new PulledEvent(eventToBePulled, destination));
                    }
                }
            }
            
            lock(activePullRequestsByInputEventType)
            {                
                var oldestTimeStamp = pullRequest.eventsToPullWith.Select(even => even.timeCreated).ToList().Min();
                
                if(!activePullRequestsByInputEventType.ContainsKey(pullRequest.pullEvent))
                {
                    activePullRequestsByInputEventType[pullRequest.pullEvent] = new List<ActivePullRequest>();
                    activePullRequestsByInputEventType[pullRequest.pullEvent].Add(new ActivePullRequest(pullRequest, oldestTimeStamp));
                }
                else
                {
                    activePullRequestsByInputEventType[pullRequest.pullEvent].Add(new ActivePullRequest(pullRequest, oldestTimeStamp));
                }
            }

            
            //check for expired events within buffers for memory management
            cleanUpPullEventBuffers(latestPullAnswer);
        }


        public void processControlMessages(){
            while(!controlMessageQueue.Data.IsEmpty){
                DCEPControlMessage controlMessage = null;
                if (controlMessageQueue.Data.TryDequeue(out controlMessage)){

                    if (controlMessage is NodeInfoForCoordinatorMessage){
                        directorNodeService.ProcessNodeInfoForCoordinatorMessage(controlMessage as NodeInfoForCoordinatorMessage);

                    } else if (controlMessage is UpdatedExecutionStateMessage){
                        var newState = (controlMessage as UpdatedExecutionStateMessage).newState;
                        Console.WriteLine(TAG + "updated execution state from " + state.ToString() + " to "+newState.ToString());
                        state = newState;
                    }
                    //check if a received control message is a pull request and handle it appropriately
                    else if (controlMessage is PullRequestMessage){
                        var pullRequest = controlMessage as PullRequestMessage;

                        ++receivedPullRequestsCount;
                        createAndHandlePullRequest(controlMessage as PullRequestMessage);
                    }
                }
            }
        }

        public async Task<int> ReceiveDCEPControlMessageAsync(DCEPControlMessage controlMessage)
        {
            controlMessageQueue.Data.Enqueue(controlMessage);
            return 0;
        }

        public async Task<int> ReceiveExternalEventAsync(AbstractEvent e)
        {
            receivedEventCount++;
            externalEventQueue.Data.Enqueue(e);

            return 0;
        }

        public void processingStep(){
            
            if (getQueuedEventCount()>10000 & dataSetMode) //  [ "artificial" input buffer of size 10000 - only works for reading input from file]
            {
               //Console.WriteLine(TAG + $"Buffer capacity full!!!!");
               primitiveEventSourceService.setFull(true);
               // terminateImmediately();
            }
            else if (getQueuedEventCount()<10000) {
                primitiveEventSourceService.setFull(false);
                //Console.WriteLine(TAG + $"Events in Queue: "  + getQueuedEventCount());
            }
          

            AbstractEvent externalEvent = null;
            if (externalEventQueue.Data.TryDequeue(out externalEvent))
            {
                externalEvent.knownToNodes.Add(this.nodeName);
                var processingStart = stopwatch.ElapsedMilliseconds;

                processQueries(externalEvent);

                benchmarkMeter.registerProcessedEvent(externalEvent, processingStart, stopwatch.ElapsedMilliseconds);
            }

            AbstractEvent internalEvent = null;
            if (internalEventQueue.Data.TryDequeue(out internalEvent))
            {
                if(internalEvent.knownToNodes.Count==0) internalEvent.knownToNodes.Add(this.nodeName); //  : now internal primtive events have two identical entries, however complex events did not get a first entry before   
               
                var processingStart = stopwatch.ElapsedMilliseconds;
                if (queryProcessors.Count != 0) 
                {
                    processQueries(internalEvent);

                    benchmarkMeter.registerProcessedEvent(internalEvent, processingStart, stopwatch.ElapsedMilliseconds);
                }
         
                forwardRuleProcessor.processEvent(internalEvent);
            }

            PulledEvent pulledEvent = null;
            //if there are pull answers events within the queue, sent them
            while (pullEventQueue.Data.TryDequeue(out pulledEvent))
            {
                proxyProvider.getProxy(pulledEvent.destination).ReceiveExternalEventFork(pulledEvent.pulledEvent.DeepClone());
            }


            benchmarkMeter.tick(stopwatch.ElapsedMilliseconds);
            updateRemainingTimePrinter(stopwatch.ElapsedMilliseconds);
        }

        private void processQueries(AbstractEvent inputEvent)
        {

            DateTime t = inputEvent.getOldest(); //   [getOldest is defined as generation time for prim, oldest timestamp of contained prim for complex events]

            if (TimestampsDict.TryGetValue(inputEvent.knownToNodes[0], out DateTime value)) //  [if t is older than the oldest timestamp received from the same node (inputEvent.knownToNodes[0])
            {
                if (t > value) 
                    TimestampsDict.AddOrUpdate(inputEvent.knownToNodes[0], t, (key, oldValue) => t); 
            }
            else // : If not initialized, set to t
            {
                TimestampsDict.AddOrUpdate(inputEvent.knownToNodes[0], t, (key, oldValue) => t);
            }

            
            t = TimestampsDict.Values.Min(); //  [get current oldest value and use it for removing activations in between]
            if (t > oldestTimestamp) 
            {
                oldestTimestamp = t; //  [oldest timestamp updated -> trigger activation deletion]

                foreach (var queryProcessor in queryProcessors) 
                {
                    queryProcessor.removeActivations(oldestTimestamp);
                }
            }

            foreach (var queryProcessor in queryProcessors)
            {
                var (outputEvents, pullRequests) = queryProcessor.processInputEvent(inputEvent, inputEvent.getOldest());
                foreach (var outputEvent in outputEvents)
                {
                    outputEvent.timeSent = DateTime.Now;
                    proxyProvider.getProxy(nodeName).RegisterComplexEventMatchFork(outputEvent, false); //  [isDropped is always false, as dropping events now happens during processing]
                }
                
                foreach (var pullRequest in pullRequests)
                {
                    foreach (var producingNode in executionPlan.sourceNodesByEventName[pullRequest.pullEvent])
                    {
                        if(!producingNode.Equals(pullRequest.sendingNode))
                        {
                            //send pull request
                            proxyProvider.getProxy(producingNode).ReceiveDCEPControlMessageFork(pullRequest);
                        }
                    }
                }
            }
        }


        //remove every buffered active pull request for which the time-window constraint is not satisfied anymore
        private void cleanUpExpiredPullRequests(DateTime currentTime)
        {
            lock(activePullRequestsByInputEventType)
            {
                foreach(var activePullRequestBuffer in activePullRequestsByInputEventType)
                {
                    activePullRequestBuffer.Value.RemoveAll(activePullRequest => (currentTime - activePullRequest.timeReceived) >= maxActivePullRequestDuration);
                }
            }
        }


        //iterate through all active pull requests with the received pulling event type and check, if the event satisfies any active pull requests
        private void handleActivePullRequest(AbstractEvent e)
        {
            //Console.WriteLine("Entered handleActivePullRequest");
            cleanUpExpiredPullRequests(e.getNewestAlt()); //e.timeCreated); 
            
            lock(activePullRequestsByInputEventType)
            {
                if (activePullRequestsByInputEventType.ContainsKey(e.type))
                {
                    foreach(var activePullRequest in activePullRequestsByInputEventType[e.type])
                    {
                        
                        if(e.type.Equals(activePullRequest.pullRequest.pullEvent) && eventMatchesPullRequest(e, activePullRequest.pullRequest.eventsToPullWith, activePullRequest.pullRequest.queryToProcess))
                        {
                            
                            var dropProbability = determineSelectivity(activePullRequest.pullRequest.pullEvent, activePullRequest.pullRequest.eventsToPullWith);
                            
                            if(shouldDropEvent(dropProbability))
                                continue;
                                
                            
                            //if the event satisfies everything, it is put into the pull event queue and is processed afterwards in ProcessingStep
                            foreach(var destination in activePullRequest.pullRequest.destinations)
                            {
                                bool doSend = true;
                                
                                //.. process it according to the watermark and..
                                lock(latestPullAnswerToNode)
                                {
                                    var key = new Tuple<NodeName,EventType>(destination,e.type);
                                    if(latestPullAnswerToNode.ContainsKey(key) && latestPullAnswerToNode[key] >= e.timeCreated)
                                        doSend = false;
                                    
                                    //.. update the watermark accordingly (the watermark increased, if the pull event creation time is newer than the old watermark)
                                    if(latestPullAnswerToNode.ContainsKey(key))
                                    {
                                        DateTime latestKnownTime = latestPullAnswerToNode[key];
                                        latestPullAnswerToNode[key] = (latestKnownTime > e.timeCreated ? latestKnownTime : e.timeCreated);
                                    }
                                    else
                                    {
                                        latestPullAnswerToNode.Add(key, e.timeCreated);
                                    }
                                }
                                
                                //if the event fulfills everything, it can be sent to the destination node
                                if(doSend)
                                {
                                    //++pullEventCounter;
                                    //Console.WriteLine("Put something in pullEventQueue");
                                    pullEventQueue.Data.Enqueue(new PulledEvent(e, destination));
                                }
                            }
                        }
                    }  
                }
            }
        }

        public async Task RegisterPrimitiveEventInputAsync(PrimitiveEvent e) 
        {            
            locallyGeneratedPrimitiveEventCount++;
            
            //put the generated primitive event type into the buffer, if it has one of the event types which have to be buffered
            lock(pullEventBuffers)
            {
                if(pullEventBuffers.ContainsKey(e.type))
                {
                    pullEventBuffers[e.type].Add(e);
                    handleActivePullRequest(e);
                }
            }
            internalEventQueue.Data.Enqueue(e);
        }


        public async Task RegisterComplexEventMatchAsync(ComplexEvent e, bool isDropped)
        {
            
            TimeSpan delay = DateTime.Now - e.getNewestAlt(); //  [getNewestAlt reflects actual time event was created as opposed to artificial time stamp caused by dataset based input generation]

            Console.WriteLine("Complex;" + e.type + ";" +  delay);

            benchmarkMeter.registerComplexMatchBeforeDropout(e);
            if (!isDropped)
            {
                locallyGeneratedComplexEventCount++;
                
                //put the generated primitive event type into the buffer, if it has one of the event types which have to be buffered
                lock(pullEventBuffers)
                {
                    if(pullEventBuffers.ContainsKey(e.type))
                    {
                        pullEventBuffers[e.type].Add(e);
                        handleActivePullRequest(e);
                        
                    }
                    internalEventQueue.Data.Enqueue(e);
                    benchmarkMeter.registerComplexMatchAfterDropout(e);
                }
            }
            else
            {
                locallyDroppedComplexEvents++;
            }
        }

        public void  terminateImmediately(){
            
            // ProcessingRemainder
            state = NodeExecutionState.DoSendExperimentDataAndTerminate;
            if (directorNodeService != null){
                directorNodeService.terminateAllNodesImmediately();
            }
        }
        public void updateRemainingTimePrinter(long passedMilliseconds)
        {
            if (_remainingTimeLastPrintTime == 0)
            {
                _remainingTimeLastPrintTime = passedMilliseconds;
                return;
            }
            
            // after 60 seconds, every 60 seconds: 
            if (passedMilliseconds - _remainingTimeLastPrintTime > 60000)
            {
                var totalEvents = locallyGeneratedComplexEventCount +
                                  receivedEventCount +
                                  receivedPullRequestsCount +
                                  locallyGeneratedPrimitiveEventCount;

                var throughput = totalEvents - _remainingTimeLastProcessedCount;
                if (throughput == 0) return;
                
                double interval = (passedMilliseconds - _remainingTimeLastPrintTime);
                double queueCount = getQueuedEventCount();
                double estimatedMs  = interval * (queueCount / throughput);
                long estimatedMinutes = (long) (estimatedMs / 60000);
       
                 
                
                _remainingTimeLastPrintTime = passedMilliseconds;
                _remainingTimeLastProcessedCount = totalEvents;
                 Console.WriteLine("Internal Queue Size:" + internalEventQueue.Data.LongCount());
                 Console.WriteLine("External Queue Size:" + externalEventQueue.Data.LongCount());
                 //Console.WriteLine("Pull Event Queue Size:" + pullEventQueue.Data.LongCount());
                 //Console.WriteLine("pullEventCounter:" + pullEventCounter);
                 //Console.WriteLine("locallyGeneratedPrimitiveEventCount:" + locallyGeneratedPrimitiveEventCount);
                 
                 Console.WriteLine(TAG + $"Estimated time for processing queued events: {estimatedMinutes} minutes ({queueCount} events in queue) ");
                
            }
        }
    }
}
