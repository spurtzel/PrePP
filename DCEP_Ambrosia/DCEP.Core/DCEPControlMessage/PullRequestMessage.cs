using System.Collections.Generic;
using System.Runtime.Serialization;


namespace DCEP.Core.DCEPControlMessage
{
    [DataContract]
    public class PullRequestMessage : DCEPControlMessage
    {
        [DataMember]
        public EventType pullEvent;
        
        [DataMember]
        public List<AbstractEvent> eventsToPullWith;
        
        [DataMember]
        public List<NodeName> destinations;
        
        [DataMember]
        public Query queryToProcess;


        public PullRequestMessage(NodeName sendingNode, EventType pullEvent, List<AbstractEvent> eventsToPullWith, List<NodeName> destinations, Query queryToProcess) : base(sendingNode)
        {
            this.pullEvent = pullEvent;
            this.eventsToPullWith = eventsToPullWith;
            this.destinations = destinations;
            this.queryToProcess = queryToProcess;
        }
    }
}
