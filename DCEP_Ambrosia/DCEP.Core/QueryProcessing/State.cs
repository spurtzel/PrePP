using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using DCEP.Core.QueryProcessing.Constraints;

namespace DCEP.Core
{
    [DataContract]
    internal class State
    {
        [DataMember]
        public List<State> nextStates { get; set; }

        [DataMember]
        public IEnumerable<BufferConstraint> bufferConstraints { get; private set; }

        [DataMember]
        public IEnumerable<PrimitiveBufferComponentAnyMatchConstraint> pBCAnyMatchConstraints { get; private set; }

        [DataMember]
        public IEnumerable<PrimitiveBufferComponentAllMatchConstraint> pBCAllMatchConstraints { get; private set; }

        [DataMember]
        public EventType requiredEventType { get; set; }

        public State(EventType requiredEventType,
                     IEnumerable<BufferConstraint> bufferConstraints,
                     IEnumerable<PrimitiveBufferComponentAnyMatchConstraint> primitiveBufferComponentConstraints,
                     IEnumerable<PrimitiveBufferComponentAllMatchConstraint> pBCAllMatchConstraints)
        {
            this.requiredEventType = requiredEventType;
            this.nextStates = new List<State>();
            this.bufferConstraints = bufferConstraints;
            this.pBCAnyMatchConstraints = primitiveBufferComponentConstraints;
            this.pBCAllMatchConstraints = pBCAllMatchConstraints;
        }

        public bool testGuardConditions(AbstractEvent candidate, IEnumerable<AbstractEvent> eventBuffer)
        {
            if (!candidate.type.Equals(requiredEventType))
            {
                return false;
            }

            if (eventBuffer == null)
            {
                eventBuffer = Enumerable.Empty<AbstractEvent>();
            }


            if (!PrimitiveBufferComponentAnyMatchConstraint.checkAll(pBCAnyMatchConstraints, candidate, eventBuffer))
            {
                return false;
            }

            if (!PrimitiveBufferComponentAllMatchConstraint.checkAll(pBCAllMatchConstraints, candidate, eventBuffer))
            {
                return false;
            }

            foreach (var item in bufferConstraints)
            {
                if (!item.check(candidate, eventBuffer))
                {
                    return false;
                }
            }
            return true;
        }
    }
}
