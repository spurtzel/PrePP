using System.Collections;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using DCEP.Core;
using System.Text;
using System.Linq;
using DCEP.Core.QueryProcessing.Operators;

namespace DCEP.Core
{
    [DataContract]
    public class EventType : QueryComponent
    {

        [DataMember]
        public string name { get; set; }

        public override bool Equals(object obj)
        {
            return obj is EventType name &&
                   this.name.Equals(name.name);
        }

        public override string ToString()
        {
            return this.name.ToString();
        }

        public override int GetHashCode()
        {
            return this.name.GetHashCode();
        }

        public EventType(string name)
        {
            this.name = name.Replace(" ", "");
        }

        public static IEnumerable<EventType> splitSemicolonSeparatedEventNames(string input)
        {
            List<EventType> result = new List<EventType>();

            int index = 0;
            int currentNameStartIndex = 0;
            int openBraces = 0;

            while (index <= input.Length)
            {
                if (index == input.Length && index > currentNameStartIndex || openBraces == 0 && input[index] == ';')
                {
                    // found end of current event name
                    string name = new string(input.Skip(currentNameStartIndex).Take(index - currentNameStartIndex).ToArray());
                    result.Add(new EventType(name));
                    currentNameStartIndex = index + 1;

                    if (index == input.Length)
                    {
                        break;
                    }
                }

                if (input[index] == '(')
                {
                    openBraces++;
                }

                if (input[index] == ')')
                {
                    openBraces--;
                }

                index++;
            }

            return result;

        }

        public override IEnumerable<QueryComponent> getChildren()
        {
            return Enumerable.Empty<QueryComponent>();
        }

        public override EventType asEventType()
        {
            return this;
        }

        public override IEnumerable<EventType> getComponentsAsEventTypes()
        {
            return Enumerable.Empty<EventType>();
        }

    }
}
