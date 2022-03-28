using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace DCEP.Core
{
    [DataContract]
    public class ComplexEvent : AbstractEvent
    {
        [DataMember]
        public IEnumerable<AbstractEvent> children { get; private set; }
        
        [DataMember]
        public DateTime timeSent;

        public ComplexEvent(EventType name, IEnumerable<AbstractEvent> outputeventcomponents, NodeName nodeName)  : base(name, nodeName)
        {
            children = outputeventcomponents;
        }

        public override DateTime getOldest() // 
        {
           return children.Select(even => even.getOldest()).ToList().Min();
        }
        
        public override DateTime getNewestAlt() // 
        {
            return children.Select(even => even.getNewestAlt()).ToList().Max();
        }
    }
}
