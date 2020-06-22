using ConsensusProject.Messages;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusProject.App
{
    public class Config
    {
        public string HubIpAddress { get; set; }
        public int HubPort { get; set; }
        public string NodeIpAddress { get; set; }
        public int NodePort { get; set; }
        public string Alias { get; set; }
        public int ProccessIndex { get; set; }
        public int Delay { get; set; }
        public int EpochIncrement { get; set; }
        public bool IsEqual(ProcessId processId)
        {
            return processId.Owner == Alias &&
                processId.Port == NodePort &&
                processId.Host == NodeIpAddress &&
                processId.Index == ProccessIndex;
        }
    }
}
