using ConsensusProject.Messages;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusProject.Utils
{
    public class AbstractionHelpers
    {
        public static ProcessId GetMaxRankedProcess(List<ProcessId> processes)
        {
            if (processes.Count == 0) return null;
            ProcessId max = processes[0];
            foreach (var proc in processes)
            {
                if (proc.Rank > max.Rank)
                {
                    max = proc;
                }
            }
            return max;
        }
    }
    
    public class State
    {
        public int Valts { get; set; }
        public Value Value { get; set; }
    }
}
