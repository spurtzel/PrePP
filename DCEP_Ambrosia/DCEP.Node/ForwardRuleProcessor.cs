using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using DCEP.AmbrosiaNodeAPI;
using DCEP.Core;
using DCEP.Core.Utils.DeepCloneExtension;

namespace DCEP.Node
{
    [DataContract]
    public class ForwardRuleProcessor
    {
        [DataMember] private readonly string TAG;
        [DataMember] private Dictionary<EventType, ForwardRule> forwardRules;

        private INodeProxyProvider proxyProvider;

        public ForwardRuleProcessor(string TAG, Dictionary<EventType, ForwardRule> forwardRules, INodeProxyProvider proxyProvider)
        {
            this.TAG = TAG + "[ForwardRuleProcessor] ";
            this.forwardRules = forwardRules;
            this.proxyProvider = proxyProvider;
        }

        public void processEvent(AbstractEvent e)
        {
            ForwardRule rule;

            if (forwardRules.TryGetValue(e.type, out rule))
            {
                foreach (var nodeName in rule.destinations)
                {
                    // avoiding forwarding circles
                    if (!e.knownToNodes.Contains(nodeName))
                    {
                        Console.WriteLine(String.Format(TAG + "Sending {0} to Node {1}", e, nodeName));
                        proxyProvider.getProxy(nodeName).ReceiveExternalEventFork(e.DeepClone());
                    }
                }
            }
        }
    }
}
