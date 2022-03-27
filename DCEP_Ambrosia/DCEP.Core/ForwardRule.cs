using System.Collections.Generic;
using System.Runtime.Serialization;
using DCEP;

namespace DCEP.Core
{
    [DataContract]
    public class ForwardRule
    {
        [DataMember]
        public HashSet<NodeName> destinations { get; set; }

        public ForwardRule()
        {
            this.destinations = new HashSet<NodeName>();
        }

        public void addTarget(NodeName n)
        {
            destinations.Add(n);
        }
    }
}