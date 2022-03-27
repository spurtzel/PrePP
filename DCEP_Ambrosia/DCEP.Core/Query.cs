using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;
using DCEP.Core.QueryProcessing;
using DCEP.Core.QueryProcessing.Operators;
using DCEP.Core.Utils;

namespace DCEP.Core
{
    
    public struct pushPullPlanStep
    {
        public List<EventType> toAcquire;
        public List<EventType> dependencies;
    }
    
    [DataContract]
    public class Query
    {
        [DataMember]
        public EventType name { get; set; }

        [DataMember]
        public List<EventType> inputEvents { get; set; }

        [DataMember]
        public PlacementInfo placement { get; set; }

        [DataMember]
        public double selectionRate { get; set; }
        public double selectionRateTransition { get; set; }


        [DataMember]
        public AbstractQueryOperator rootOperator { get; set; }

        [DataMember]
        public string eventSelectionStrategy { get; set; }
        
        [DataMember]
        public List<pushPullPlanStep> pushPullPlan { get; set; }
        
        [DataMember]
        public string pullRequestHandlingNode { get; set; }

        public Query(EventType name, PlacementInfo placementInfo, IEnumerable<EventType> inputEventNames, double selectionRate, string eventSelectionStrategy)
        {
            this.name = name;
            this.inputEvents = new List<EventType>();
            this.placement = placementInfo;
            this.selectionRate = selectionRate;
            this.rootOperator = (AbstractQueryOperator)new QueryComponentParser().parse(name.ToString());

			this.eventSelectionStrategy = eventSelectionStrategy;

            foreach (EventType item in inputEventNames)
            {
                inputEvents.Add(item);
            }
            this.selectionRateTransition = Math.Pow(selectionRate, 1.0/inputEvents.Count);
        }
        
        public Query(EventType name, PlacementInfo placementInfo, IEnumerable<EventType> inputEventNames, double selectionRate, string eventSelectionStrategy, List<pushPullPlanStep> pushPullPlan, string pullRequestHandlingNode)
        {
            this.name = name;
            this.inputEvents = new List<EventType>();
            this.placement = placementInfo;
            this.selectionRate = selectionRate;
            this.rootOperator = (AbstractQueryOperator)new QueryComponentParser().parse(name.ToString());
            this.pushPullPlan = pushPullPlan;
			this.eventSelectionStrategy = eventSelectionStrategy;

            foreach (EventType item in inputEventNames)
            {
                inputEvents.Add(item);
            }
            this.selectionRateTransition = Math.Pow(selectionRate, 1.0/inputEvents.Count);
            this.pullRequestHandlingNode = pullRequestHandlingNode;
        }

        /// parsing legacy format and set default value for selection rate
        public static Query parseLegacyFormat(string input)
        {
            // remove whitespace
            input = string.Join("", input.Split(' '));

            string selectionStrategy = input.Substring(0,4);
            if(!selectionStrategy.Equals("STAM") && !selectionStrategy.Equals("STNM"))
                selectionStrategy = "STAM";

            string eventName = input.Split('[')[1].TrimEnd(',');
            string inputEventsString = input.Split('[')[2].Split(']')[0];
            string placementString = input.Split('[')[2].Split(']')[1].TrimStart(',');

            IEnumerable<EventType> inputEventNames = EventType.splitSemicolonSeparatedEventNames(inputEventsString);
            PlacementInfo placementInfo = new PlacementInfo(placementString);

            return new Query(new EventType(eventName), placementInfo, inputEventNames, 1.0d, selectionStrategy);
        }

        public static List<List<EventType>> parseListOfEventTypeLists(string list)
        {
            var result = new List<List<EventType>>();
            var current_group = new List<EventType>();
            string current_event = "";
            foreach(var character in list)
            {
                if((character == ' ' || character == ';' || character == '[' || character == ']') && current_event != "")
                {
                    current_group.Add(new EventType(current_event));
                    current_event = "";
                }
                
                if(!(character == ' ' || character == ';' || character == '[' || character == ']'))
                {
                    current_event += character;
                }
                
                if(character == ']')
                {
                    result.Add(current_group);
                    current_group = new List<EventType>();
                }
                
            }
            result.RemoveAt(result.Count-1);
            
            return result;
        }
        
        
        
        
        public static List<pushPullPlanStep> parsePushPullPlan(string pushPullPlanStr)
        {
            List<pushPullPlanStep> pushPullPlan = new List<pushPullPlanStep>();
            
            string pushPullPlanOrderStr = pushPullPlanStr.Split('§')[0];
            string chosenPullSubsetsStr = pushPullPlanStr.Split('§')[1];
            
            var pushPullPlanOrder = parseListOfEventTypeLists(pushPullPlanOrderStr);
            var chosenPullSubsets = parseListOfEventTypeLists(chosenPullSubsetsStr);
            
            if (pushPullPlanOrder.Count != chosenPullSubsets.Count)
            {
                throw new ArgumentException("Plans differ in length.");
            }
            
            for(int i = 0; i < chosenPullSubsets.Count; ++i)
            {
                pushPullPlanStep nextStep = new pushPullPlanStep();
                
                nextStep.toAcquire = pushPullPlanOrder[i];
                nextStep.dependencies = chosenPullSubsets[i];
                
                pushPullPlan.Add(nextStep);
            }
            
            return pushPullPlan;
        }



        public static Query createFromString(string input)
        {
            // support legacy format
            if (input.TrimAllWhitespace().StartsWith("["))
            {
                return parseLegacyFormat(input);
            }

            if (!input.Contains("SELECT")){
                throw new ArgumentException("Query string is missing the SELECT statement.");
            }

            if (!input.Contains("ON"))
            {
                throw new ArgumentException("Query string is missing the ON statement.");
            }


            // parsing required components
            // determine event selection strategy (ANY/NEXT)
            string selectionStrategy = input.Substring(0,4);
            if(!selectionStrategy.Equals("STAM") && !selectionStrategy.Equals("STNM"))
                selectionStrategy = "STAM";

            string pushPullPlanOrder = input.Contains("~") ? input.Split("~")[1] : "";
            var pushPullPlan = parsePushPullPlan(pushPullPlanOrder);
            string pullRequestHandlingNode = input.Contains("#") ? input.Split("#")[1] : "";
            
            
            
            input = input.Split("~")[0];

            string eventName = input.Split("SELECT")[1].Split("FROM")[0].TrimAllWhitespace();
            string inputEventsString = input.Split("FROM")[1].Split("ON")[0].TrimAllWhitespace();
            string placementString = input.Split("ON")[1].Split("WITH")[0].TrimAllWhitespace();
            Console.WriteLine("placementString: " + placementString);
            IEnumerable<EventType> inputEventNames = EventType.splitSemicolonSeparatedEventNames(inputEventsString);
            PlacementInfo placementInfo = new PlacementInfo(placementString);
            
            pullRequestHandlingNode = pullRequestHandlingNode == "" ? placementInfo.singleNode.ToString() : pullRequestHandlingNode;

            Console.WriteLine("pullRequestHandlingNode:" + pullRequestHandlingNode);
            // parsing optional components
            string withString = (input.Contains("WITH")) ? input.Split("WITH")[1].TrimAllWhitespace() : "";

            double selectionRate = 1.0;
            if (withString.Contains("selectionRate="))
            {
                selectionRate = Double.Parse(withString.Split("selectionRate=")[1].Split(",")[0], CultureInfo.InvariantCulture);
            }

            // constructing query instance
            return new Query(new EventType(eventName), placementInfo, inputEventNames, selectionRate, selectionStrategy, pushPullPlan, pullRequestHandlingNode);
        }

        public override string ToString()
        {
            return String.Format("SELECT {0} FROM {1}", name, string.Join(",", inputEvents));
        }
    }
}
