using System.Runtime.Serialization;

namespace DCEP.Core
{
    [DataContract]
    public class ExperimentRunData
    {
        [DataMember] public long locallyGeneratedComplexEventCount {get; set; }
        [DataMember] public long receivedEventCount { get; set; }
        [DataMember] public long receivedPullRequests { get; set; }
        [DataMember] public long locallyGeneratedPrimitiveEventCount { get; set; }
        
        [DataMember] public long locallyDroppedComplexEvents { get; set; }

        [DataMember] public long locallyDroppedPartialMatches { get; set; }

        public ExperimentRunData(long locallyGeneratedComplexEventCount, long receivedEventCount, long locallyGeneratedPrimitiveEventCount, long locallyDroppedComplexEvents, long locallyDroppedPartialMatches, long receivedPullRequests)
        {
            this.locallyGeneratedComplexEventCount = locallyGeneratedComplexEventCount;
            this.receivedEventCount = receivedEventCount;
            this.receivedPullRequests = receivedPullRequests;
            this.locallyGeneratedPrimitiveEventCount = locallyGeneratedPrimitiveEventCount;
            this.locallyDroppedComplexEvents = locallyDroppedComplexEvents;
            this.locallyDroppedPartialMatches = locallyDroppedPartialMatches;
        }


    }


}
