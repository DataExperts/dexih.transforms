using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace dexih.functions
{
    public class Message: ReturnValue<object>
    {
        public string MessageId { get; set; }
        public string RemoteToken { get; set; }
        public string Command { get; set; }
        public long HubKey { get; set; }

        public Message() { }

        public Message(string remoteToken, string messageId, string command, ReturnValue<object> returnValue)
        {
            RemoteToken = remoteToken;
            MessageId = messageId;
            Command = command;
            Success = returnValue.Success;
            Message = returnValue.Message;
            Exception = returnValue.Exception;
            Value = returnValue.Value;
        }

        public Message(string remoteToken, string messageId, string command, object value)
        {
            RemoteToken = remoteToken;
            MessageId = messageId;
            Command = command;
            Success = true;
            Message = "";
            Exception = null;
            Value = value;
        }

        public Message(string remoteToken, string messageId, string command, string returnValue)
        {
            RemoteToken = remoteToken;
            MessageId = messageId;
            Command = command;
            Success = true;
            Message = "";
            Exception = null;
            Value = returnValue;
        }

        public Message(bool success, string message, Exception exception)
        {
            Success = success;
            Message = message;
            Exception = exception;
        }
    }
}
