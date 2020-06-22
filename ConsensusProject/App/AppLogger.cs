using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;
using System.Collections.Generic;
using System.Text;

namespace ConsensusProject.App
{
    public class AppLogger
    {
        private Config _config;
        private string _location = "";
        private string _system = "";

        public AppLogger(Config c)
        {
            _config = c;
        }

        public AppLogger(Config c, string location)
        {
            _config = c;
            _location = location;
        }

        public AppLogger(Config c, string location, string system)
        {
            _config = c;
            _location = location;
            _system = "/"+system;
        }

        public void LogInfo(string message)
        {
            Console.WriteLine($"{DateTime.Now.TimeOfDay} INF {_config.Alias}-{_config.ProccessIndex}{_system} at {_location}: {message}");
        }
        public void LogInfo(string message, Message messageObj)
        {
            Console.WriteLine($"{DateTime.Now.TimeOfDay} INF {_config.Alias}-{_config.ProccessIndex}{_system} at {_location}: {message} \n {JsonHelper.FormatJson(messageObj.ToString())}");
        }
        public void LogError(string message)
        {
            Console.WriteLine($"{DateTime.Now.TimeOfDay} ERR {_config.Alias}-{_config.ProccessIndex}{_system} at {_location}: {message}");
        }

    }
}
