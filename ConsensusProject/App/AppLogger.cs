using ConsensusProject.Messages;
using ConsensusProject.Utils;
using System;

namespace ConsensusProject.App
{
    public class AppLogger
    {
        private Config _config;
        private string _location = string.Empty;
        private string _system = string.Empty;
        private ConsoleColor _consoleColor;

        public AppLogger(Config c)
        {
            _config = c;
            _consoleColor = (ConsoleColor)Enum.Parse(typeof(ConsoleColor), Enum.GetNames(typeof(ConsoleColor))[_config.ProccessIndex % 16]);
        }

        public AppLogger(Config c, string location)
        {
            _config = c;
            _location = location;
            _consoleColor = (ConsoleColor)Enum.Parse(typeof(ConsoleColor), Enum.GetNames(typeof(ConsoleColor))[_config.ProccessIndex % 16]);
        }

        public AppLogger(Config c, string location, string system)
        {
            _config = c;
            _location = location;
            _system = system;
            _consoleColor = (ConsoleColor)Enum.Parse(typeof(ConsoleColor), Enum.GetNames(typeof(ConsoleColor))[_config.ProccessIndex % 16]);
        }

        public void LogInfo(string message)
        {
            Console.ForegroundColor = _consoleColor;
            Console.WriteLine($"{DateTime.Now.TimeOfDay} INF {_config.Alias}-{_config.ProccessIndex}{FormatSystem} at {_location}: {message}");
        }
        public void LogInfo(string message, Message messageObj)
        {
            Console.ForegroundColor = _consoleColor;
            Console.WriteLine($"{DateTime.Now.TimeOfDay} INF {_config.Alias}-{_config.ProccessIndex}{FormatSystem} at {_location}: {message} \n {JsonHelper.FormatJson(messageObj.ToString())}");
        }
        public void LogError(string message)
        {
            Console.ForegroundColor = _consoleColor;
            Console.WriteLine($"{DateTime.Now.TimeOfDay} ERR {_config.Alias}-{_config.ProccessIndex}{FormatSystem} at {_location}: {message}");
        }
        public void LogError(string message, Exception ex)
        {
            Console.ForegroundColor = _consoleColor;
            Console.WriteLine($"{DateTime.Now.TimeOfDay} ERR {_config.Alias}-{_config.ProccessIndex}{FormatSystem} at {_location}: {message} {ex}");
        }

        private string FormatSystem => string.IsNullOrWhiteSpace(_system) ? string.Empty : $"/{_system}";
    }
}
