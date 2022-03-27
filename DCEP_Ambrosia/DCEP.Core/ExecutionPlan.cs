using System.Diagnostics;
using System.Data;
using System;
using System.Collections.Generic;
using DCEP.Core.Utils;
using System.Linq;
using System.Runtime.Serialization;
using System.IO;

namespace DCEP.Core
{

    [DataContract]
    public class ExecutionPlan
    {
        [DataMember] public PrimitiveInputMode primitiveInputMode;
        
        [DataMember]
        public Dictionary<NodeName, NodeParams> networkPlan { get; set; }

        [DataMember]
        public Dictionary<NodeName, HashSet<Query>> queriesByNodeName { get; set; }

        [DataMember]
        public Dictionary<NodeName, Dictionary<EventType, ForwardRule>> forwardRulesByNodeName { get; set; }

        [DataMember]
        public Dictionary<EventType, HashSet<NodeName>> sourceNodesByEventName { get; set; }

        [DataMember]
        private Dictionary<EventType, List<Query>> parsedQueries { get; set; }

        [DataMember]
        private HashSet<EventType> primitiveEventNames = null;

        [DataMember]
        private int numberOfNodes = -1;

        [DataMember] public string datasetFileNameTemplate { get; set; }

        public DictionaryWithDefault<Query, bool> wasQueryProcessed;

        [DataMember]
        public Dictionary<string, double> singleSelectivities { get; set; }

        [DataMember]
        private string EventNameSequence = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        public ExecutionPlan(string[] inputlines)
        {
            if (inputlines == null)
            {
                throw new ArgumentException("inputlines must not be null.");
            }

            this.wasQueryProcessed = new DictionaryWithDefault<Query, bool>(false);
            this.networkPlan = new Dictionary<NodeName, NodeParams>();

            this.sourceNodesByEventName = new Dictionary<EventType, HashSet<NodeName>>();
            this.forwardRulesByNodeName = new Dictionary<NodeName, Dictionary<EventType, ForwardRule>>();
            this.queriesByNodeName = new Dictionary<NodeName, HashSet<Query>>();
            this.singleSelectivities = new Dictionary<string, double>();

            // parsing the which nodes there are and what primitive events they generate at what rates
            parseNetworkPlan(inputlines);
            
            // initialize members now that the nodeCount is known
            foreach (var nodeName in networkPlan.Keys)
            {
                forwardRulesByNodeName[nodeName] = new Dictionary<EventType, ForwardRule>();
                queriesByNodeName[nodeName] = new HashSet<Query>();
            }

            var remaining = inputlines.Skip(numberOfNodes + 1).ToArray();

            var primitiveInputModeLine = remaining[0];

            if (primitiveInputModeLine.Trim().ToLower().Contains("randomized rate-based"))
            {
                primitiveInputMode = PrimitiveInputMode.RANDOM_WITH_RATES;

                if (!remaining[1].Trim().StartsWith("-"))
                {
                    throw new ArgumentException("dash separation line expected after randomized rate-based primitive event generation statement ");
                }
                
                remaining = remaining.Skip(2).ToArray();
            }
            else
            {
                primitiveInputMode = PrimitiveInputMode.DATASET;
                datasetFileNameTemplate = remaining[1].Trim();

                if (!remaining[2].Trim().StartsWith("-"))
                {
                    throw new ArgumentException("dash separation line expected after Dataset-Based Primitive Event Generation statement and a single next line with the dataset configuration");
                }
                
                remaining = remaining.Skip(3).ToArray();
            }
            
            if(remaining[0].StartsWith("Single Selectivities"))
            {
                parseSingleSelectivities(remaining[0]);
                if (!remaining[1].Trim().StartsWith("-"))
                {
                    throw new ArgumentException("dash separation line expected after Single Selectivities statement");
                }
                remaining = remaining.Skip(2).ToArray();
            }
            
            // parsing what queries and compex events there are
            parseComplexQueries(remaining);

            // foreach (var eventName in parsedQueries.Keys)
            // {
            //     sourceNodesByEventName[eventName] = new HashSet<NodeName>();
            // }


            // deriving forward rules and query placements on node ids
            deriveQueryPlacementAndForwardRules();
            determinePushForwardRules();
        }

        /// Parse network plan (which nodes generate which primitive events at what rate)
        private void parseNetworkPlan(string[] inputlines)
        {
            int nodeID = -1;
            numberOfNodes = -1;

            foreach (string line in inputlines)
            {
                if (line.StartsWith("-"))
                {
                    numberOfNodes = nodeID + 1;
                    // finished with parsing the network plan
                    return;
                }

                nodeID++;
                var currentNodeName = new NodeName(nodeID.ToString());

                int[] rates = Array.ConvertAll(line.Trim().Split(' '), Int32.Parse);
                EventType[] eventNames = EventNameSequence.Substring(0, rates.Length).Select(c => new EventType(c.ToString())).ToArray();


                if (primitiveEventNames == null)
                {
                    primitiveEventNames = new HashSet<EventType>(eventNames);
                }

                NodeParams nodeParams = new NodeParams(currentNodeName, eventNames, rates);
                networkPlan[currentNodeName] = nodeParams;

                foreach (var e in eventNames.Zip(rates, (e, r) => new { Name = e, Rate = r }))
                {
                    if (e.Rate > 0)
                    {
                        if (!sourceNodesByEventName.ContainsKey(e.Name))
                        {
                            sourceNodesByEventName[e.Name] = new HashSet<NodeName>() { currentNodeName };
                        }
                        else
                        {
                            sourceNodesByEventName[e.Name].Add(currentNodeName);
                        }
                    }
                }
            }

            throw new ArgumentException("Expected ---- separation line after the network plan information was not found.");
        }


        private void parseSingleSelectivities(string line)
        {
            line = line.Trim('}');
            string[] selectivities = line.Split('{')[1].Split(',');
            
            foreach(string selectivity in selectivities)
            {
                string formattedString = selectivity.TrimAllWhitespace();
                string eventTypes = formattedString.Split(':')[0];
                string outputRate = formattedString.Split(':')[1];
                string eventProjectionKey = eventTypes.Substring(1, eventTypes.Length-2);

                double convertedOutputRate = 0.0;

                double.TryParse(outputRate, out convertedOutputRate);
                
                singleSelectivities[eventProjectionKey] = convertedOutputRate;
                
            }
            /*
            foreach (KeyValuePair<string, double> kvp in singleSelectivities)
            {
                Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
            }*/
        }


        // populate parsedQueries Dictionary by parsing the input string
        private void parseComplexQueries(string[] inputlines)
        {
            parsedQueries = new Dictionary<EventType, List<Query>>();
            foreach (string line in inputlines)
            {
                if (line.TrimAllWhitespace().Length == 0)
                {
                    continue;
                }

                Query q = Query.createFromString(line);

                List<Query> allEventTypeQueries;
                if (!parsedQueries.TryGetValue(q.name, out allEventTypeQueries))
                {
                    parsedQueries[q.name] = new List<Query>() { q };
                }
                else
                {
                    allEventTypeQueries.Add(q);
                }
            }
        }

        private void RegisterQueryPlacementOnNode(Query q, NodeName n)
        {
            if (!sourceNodesByEventName.ContainsKey(q.name))
            {
                sourceNodesByEventName[q.name] = new HashSet<NodeName>() { n };
            }
            else
            {
                sourceNodesByEventName[q.name].Add(n);
            }

            if (!queriesByNodeName.ContainsKey(n))
            {
                queriesByNodeName[n] = new HashSet<Query>() { q }; ;
            }
            else
            {
                queriesByNodeName[n].Add(q);
            }
        }

        private void RegisterForwardRequest(EventType e, NodeName target, NodeName source = null)
        {
            if (source == null)
            {
                // add forward rule for all source nodes
                foreach (var sourceNodeName in sourceNodesByEventName[e])
                {
                    if (!sourceNodeName.Equals(target))
                    {
                        var ruleDict = forwardRulesByNodeName[sourceNodeName];
                        if (!ruleDict.ContainsKey(e))
                        {
                            ruleDict[e] = new ForwardRule();
                        }
                        ruleDict[e].addTarget(target);
                    }
                }
            }
            else
            {
                // add forward rule from specific source node only
                if (source.Equals(target))
                {
                    throw new ArgumentException("RegisterForwardRequest: Source equals target." + source.ToString() + " " + target, ToString());
                }

                var ruleDict = forwardRulesByNodeName[source];
                if (!ruleDict.ContainsKey(e))
                {
                    ruleDict[e] = new ForwardRule();
                }
                ruleDict[e].addTarget(target);
            }
        }

        private void determinePushForwardRules()
        {
            forwardRulesByNodeName = new Dictionary<NodeName, Dictionary<EventType, ForwardRule>>();
            
            foreach (var nodeName in networkPlan.Keys)
                forwardRulesByNodeName[nodeName] = new Dictionary<EventType, ForwardRule>();

            
            foreach(var queries in queriesByNodeName)
            {
                var destination = queries.Key;
                foreach(var query in queries.Value)
                {
                    foreach(var pushEventType in query.pushPullPlan[0].toAcquire)
                    {
                        foreach(var source in sourceNodesByEventName[pushEventType])
                        {
                            if(source.Equals(destination))
                                continue;
                            
                            RegisterForwardRequest(pushEventType, destination, source);
                        }
                    }
                }
            }
        }

        private void processSingleQuery(Query q)
        {

            // resolve the list of source nodes for the current query 
            if (q.placement.singleNode != null)
            {
                // this query is placed on a single node
                NodeName location = q.placement.singleNode;
                RegisterQueryPlacementOnNode(q, location);

                // all input events need to be forwarded to this node
                foreach (EventType e in q.inputEvents)
                {
                    RegisterForwardRequest(e, location);
                }
            }
            else
            {
                // There exists a locally produced event (LPE) for all nodes of the query. 
                Debug.Assert(q.placement.allSourcesOfEvent != null);
                EventType locallyProcessedEvent = q.placement.allSourcesOfEvent;

                IEnumerable<NodeName> locations;
                if (q.placement.selectedNodes.Count == 0)
                {
                    // query is processed at all source nodes of LPE 
                    locations = sourceNodesByEventName[q.placement.allSourcesOfEvent];

                }
                else
                {
                    // query is processed at a subset of source nodes of LPE
                    locations = q.placement.selectedNodes;
                }

                foreach (var location in locations)
                {
                    RegisterQueryPlacementOnNode(q, location);

                    // forward all input events except for the LPE
                    foreach (EventType e in q.inputEvents)
                    {
                        if (e.Equals(locallyProcessedEvent))
                        {
                            continue;
                        }

                        RegisterForwardRequest(e, location);
                    }
                }

                if (q.placement.selectedNodes.Count > 0)
                {
                    // forward the LPE from nodes that are not in the subset to
                    // only one (the first) node of the subset, s.t. the LPE instances will be processed exactly once

                    var externalSources = sourceNodesByEventName[locallyProcessedEvent].Except(q.placement.selectedNodes);

                    NodeName selectedDestination = q.placement.selectedNodesForwardDestination;

                    foreach (NodeName sourceNode in externalSources)
                    {
                        RegisterForwardRequest(locallyProcessedEvent, selectedDestination, sourceNode);
                    }


                }
            }
        }


        /// processing query while always processing subqueries belonging to input events first
        private void processQueryBottomUp(Query q)
        {

            EventType queryEventName = q.name;

            // only continue if query was not already processed
            if (wasQueryProcessed[q])
            {
                return;
            }


            // check if input event sources are already known and if not, process their subqueries first
            foreach (var inputEventName in q.inputEvents)
            {

                if (!primitiveEventNames.Contains(inputEventName) && !sourceNodesByEventName.ContainsKey(inputEventName))
                {
                    foreach (var item in parsedQueries[inputEventName])
                    {
                        processQueryBottomUp(item);
                    }
                }
            }

            processSingleQuery(q);
            wasQueryProcessed[q] = true;

            return;
        }

        private void deriveQueryPlacementAndForwardRules()
        {
            foreach (var querylist in parsedQueries.Values)
            {
                foreach (var query in querylist)
                {
                    processQueryBottomUp(query);
                }
            }
        }

        public string generateHumanReadableString()
        {
            StringWriter writer = new StringWriter();


            foreach (var nodeName in networkPlan.Keys)
            {
                writer.WriteLine("");
                writer.WriteLine("***** Node " + nodeName + ": *****");

                writer.Write("Source of events:");

                foreach (var item in sourceNodesByEventName)
                {
                    if (item.Value.Contains(nodeName))
                    {
                        writer.Write(" " + item.Key);
                    }
                }

                writer.WriteLine("");

                // foreach (var e in networkPlan[nodeName].primitiveEventNames.Zip(networkPlan[nodeName].primitiveEventRates, (e, r) => new { Name = e, Rate = r }))
                // {
                //     if (e.Rate > 0)
                //     {
                //         writer.Write(" " + e.Name);
                //     }
                // }

                writer.WriteLine("Processing Queries:");


                foreach (var query in queriesByNodeName[nodeName])
                {
                    writer.Write("- [");

                    foreach (var item in query.inputEvents)
                    {
                        writer.Write(" " + item);
                    }

                    writer.Write(" ] => ");
                    writer.WriteLine(query.name);
                }

                writer.WriteLine("Forwarding Local Events to:");

                foreach (var ruleDict in forwardRulesByNodeName[nodeName])
                {
                    writer.Write("- " + ruleDict.Key + " =>");

                    foreach (var destinationNode in ruleDict.Value.destinations)
                    {
                        writer.Write(" " + destinationNode);
                    }

                    writer.WriteLine("");
                }
            }

            return writer.ToString();
        }
    }

}
