using ConsensusProject.Messages;
using Google.Protobuf;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusProject.Abstractions
{
    public interface Abstraction
    {
        bool Handle(Message message);
    }
}
