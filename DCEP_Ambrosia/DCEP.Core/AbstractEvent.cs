using DCEP;
using DCEP.Core;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.Serialization;

namespace DCEP.Core
{
    [DataContract]
    [KnownType(typeof(PrimitiveEvent))]
    [KnownType(typeof(ComplexEvent))]
    public abstract class AbstractEvent
    {
        [DataMember]
        public DateTime timeCreated;
        
        [DataMember]
        public DateTime actualTime; // 

        [DataMember]
        public string ID { get; set; }

        [DataMember]
        public EventType type { get; set; }

        [DataMember]
        public Dictionary<string, string> attributes { get; set; }

        [DataMember]
        public List<NodeName> knownToNodes { get; set; }

        [DataMember]
        public NodeName lastSenderNodeName { get; set; }

        [DataMember]
        public NodeName nodeName;

        protected AbstractEvent(EventType name, NodeName nodeName)
        {
            timeCreated = DateTime.Now; //
            actualTime = timeCreated; // 
            ID = Guid.NewGuid().ToString();
            attributes = new Dictionary<string, string>();
            knownToNodes = new List<NodeName>();
            this.nodeName = nodeName;
            this.type = name;
        }
        
        /*new*/
        protected AbstractEvent(EventType name, DateTime t)
        {
            actualTime = DateTime.Now; // 
            timeCreated = t; // [generate event with creation time from input file]
            ID = Guid.NewGuid().ToString();
            attributes = new Dictionary<string, string>();
            knownToNodes = new List<NodeName>();
            this.type = name;
            this.nodeName = nodeName;
        }
        /*new*/

        public string getCreatedTimestamp()
        {
            return timeCreated.ToString("yyyy-MM-dd HH:mm:ss.fff",
                                            CultureInfo.InvariantCulture);
        }

        public override string ToString()
        {
            return String.Format("{{{0}, {1}, {2}}}", type, getCreatedTimestamp(), ID.Substring(0, 8));
        }

        public abstract DateTime getOldest(); //
        public abstract DateTime getNewestAlt(); // 
       
        
        public override bool Equals(object obj)
        {
            return base.Equals(obj);
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public IEnumerable<AbstractEvent> getAllPrimitiveEventComponents()
        {
            List<AbstractEvent> output = new List<AbstractEvent>();
            var s = new Stack<AbstractEvent>();
            s.Push(this);

            while (s.Count != 0)
            {
                var current = s.Pop();
                if (current is ComplexEvent)
                {
                    foreach (var child in (current as ComplexEvent).children)
                    {
                        s.Push(child);
                    }
                }
                else if (current is PrimitiveEvent)
                {
                    output.Add(current);
                }

            }

            return output;
        }
        
        public IEnumerable<AbstractEvent> getAllEventComponents() //
        {
            List<AbstractEvent> output = new List<AbstractEvent>();
            var s = new Stack<AbstractEvent>();
            s.Push(this);
            
            while (s.Count != 0)
            {
                var current = s.Pop();
                
                output.Add(current);
                if (current is ComplexEvent)
                {
                    foreach (var child in (current as ComplexEvent).children)
                    {
                        s.Push(child);
                    }
                }
            }

            return output;
        }
        
    }

}
