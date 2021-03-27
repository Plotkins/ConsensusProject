using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;

namespace ConsensusProject.Utils
{
    public class TcpWrapper
    {

        private TcpListener _server = null;

        public TcpWrapper(string ipAddress, int port)
        {
            IPAddress localAddr = IPAddress.Parse(ipAddress);
            _server = new TcpListener(localAddr, port);
            _server.Start();
        }

        public void Start() => _server.Start();

        public void Send(string ipAddress, int port, byte[] content)
        {
            try
            {
                using (TcpClient client = new TcpClient(ipAddress, port))
                using (var ms = new MemoryStream())
                using (NetworkStream stream = client.GetStream())
                {
                    var bufferLength = BitConverter.GetBytes(content.Length);
                    Array.Reverse(bufferLength);
                    var finalArray = bufferLength.Concat(content).ToArray();
                    stream.Write(finalArray, 0, finalArray.Length);
                }
            }
            catch
            {
                throw;
            }
        }

        public byte[] Receive()
        {
            try
            {
                using (TcpClient client = _server.AcceptTcpClient())
                using (NetworkStream stream = client.GetStream())
                {
                    byte[] bufferLength = new byte[4];
                    stream.Read(bufferLength, 0, 4);
                    Array.Reverse(bufferLength);

                    var length = BitConverter.ToInt32(bufferLength);

                    byte[] objectArray = new byte[length];

                    stream.Read(objectArray, 0, length);

                    return objectArray;
                }
            }
            catch
            {
                _server.Stop();
                throw;
            }
        }
    }
}
